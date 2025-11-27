using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Linq;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class ObjectBuilderTests
    {
        // --- helpers ---
        private static int CharBytes(int charCount) => charCount * sizeof(char);

        [Test]
        public void ScalarFields_BasicLayout_AndNames_AreSortedOrdinal()
        {
            // Arrange
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("a");    // default zero
            ob.SetScalar<float>("b");  // default zero

            // Act
            Container c = ob.BuildContainer();

            // Assert
            Assert.NotNull(c);
            var view = c.View;

            Assert.AreEqual(2, view.FieldCount, "Should have 2 fields.");
            Assert.AreEqual(0, view.Header.Version);

            // Names are stored as UTF-16 and field headers are sorted by name (ordinal)
            Assert.That("a".AsSpan().SequenceEqual(view.GetFieldName(0)), Is.True);
            Assert.That("b".AsSpan().SequenceEqual(view.GetFieldName(1)), Is.True);

            // Length/ElemSize check
            var f0 = view.Fields[0];
            var f1 = view.Fields[1];
            Assert.AreEqual(sizeof(int), f0.Length);
            Assert.AreEqual(sizeof(float), f1.Length);
            Assert.AreEqual(sizeof(int), f0.ElemSize);
            Assert.AreEqual(sizeof(float), f1.ElemSize);

            // Names blob sanity: name segment length equals sum of char-bytes
            var expectedNameBytes = CharBytes("a".Length + "b".Length);
            Assert.AreEqual(expectedNameBytes, view.NameSegment.Length);
        }

        [Test]
        public void DataOffsets_ShouldBeAbsoluteAndCumulative()
        {
            // Arrange
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("a");
            ob.SetScalar<long>("b");   // 8 bytes
            ob.SetScalar<short>("c");  // 2 bytes

            // Act
            Container c = ob.BuildContainer();

            // Assert
            var view = c.View;
            int dataStart = view.Header.DataOffset; // absolute start of data segment

            var f0 = view.Fields[0];
            var f1 = view.Fields[1];
            var f2 = view.Fields[2];

            // Expected absolute layout:
            // f0 at dataStart,
            // f1 at dataStart + sizeof(int),
            // f2 at dataStart + sizeof(int) + sizeof(long)
            Assert.AreEqual(dataStart, f0.DataOffset, "First field data offset should be absolute dataStart.");
            Assert.AreEqual(dataStart + sizeof(int), f1.DataOffset, "Second field should start right after first field.");
            Assert.AreEqual(dataStart + sizeof(int) + sizeof(long), f2.DataOffset, "Third field should be cumulative (absolute).");

            // Also verify lengths
            Assert.AreEqual(sizeof(int), f0.Length);
            Assert.AreEqual(sizeof(long), f1.Length);
            Assert.AreEqual(sizeof(short), f2.Length);


            // Write payloads using absolute offsets into full buffer
            var buf = view.Span;
            BitConverter.GetBytes(123).CopyTo(buf.Slice(f0.DataOffset, f0.Length));
            BitConverter.GetBytes((long)456789).CopyTo(buf.Slice(f1.DataOffset, f1.Length));
            BitConverter.GetBytes((short)-7).CopyTo(buf.Slice(f2.DataOffset, f2.Length));

            // Read back via GetFieldBytes (should also use absolute offsets internally)
            Assert.AreEqual(123, BitConverter.ToInt32(view.GetFieldBytes(0)));
            Assert.AreEqual(456789L, BitConverter.ToInt64(view.GetFieldBytes(1)));
            Assert.AreEqual((short)-7, BitConverter.ToInt16(view.GetFieldBytes(2)));
        }


        [Test]
        public void ArrayField_ShouldMarkElemSize_AndLength()
        {
            // Arrange
            var ob = new ObjectBuilder();
            var arr = new[] { 10, 20, 30, 40 };
            ob.SetArray<int>("ints", arr);

            // Act
            Container c = ob.BuildContainer();

            // Assert
            var view = c.View;
            Assert.AreEqual(1, view.FieldCount);
            var f = view.Fields[0];
            Assert.True(f.FieldType.IsInlineArray, "Should be marked as array.");
            Assert.AreEqual(sizeof(int), f.ElemSize);
            Assert.AreEqual(arr.Length * sizeof(int), f.Length);

            // Optional payload verification (will fail if data was copied into wrong segment):
            var data = view.GetFieldBytes(0);
            for (int i = 0; i < arr.Length; i++)
            {
                var v = BitConverter.ToInt32(data.Slice(i * 4, 4));
                Assert.AreEqual(arr[i], v, $"array[{i}] must match");
            }
        }

        [Test]
        public void RefField_ShouldBe8Bytes_AndHoldGivenId()
        {
            // Arrange
            var ob = new ObjectBuilder();
            var ft = new FieldType(ValueType.Ref, false);
            ob.SetRef("child", id: 0x0102030405060708UL);

            // Act
            Container c = ob.BuildContainer();

            // Assert
            var view = c.View;
            var f = view.Fields[0];
            Assert.True(f.IsRef);
            Assert.AreEqual(8, f.Length);
            var data = view.GetFieldBytes(0);
            var id = BinaryPrimitives.ReadUInt64LittleEndian(data);
            Assert.AreEqual(0x0102030405060708UL, id);
        }

        [Test]
        public void NamesBlob_IsUtf16_AndMatchesOffsets()
        {
            // Arrange
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("apple");
            ob.SetScalar<int>("banana");
            ob.SetScalar<int>("carrot");

            // Act
            Container c = ob.BuildContainer();

            // Assert
            var view = c.View;
            var ns = view.NameSegment;

            for (int i = 0; i < view.FieldCount; i++)
            {
                var fh = view.Fields[i];
                var slice = ns.Slice(fh.NameOffset - view.Header.NameOffset, fh.NameLength * sizeof(char));
                var s = MemoryMarshal.Cast<byte, char>(slice).ToString();
                Assert.AreEqual(view.GetFieldName(i).ToString(), s);
            }
        }

        [Test]
        public void NamesBlob_IsUtf16_AndTestOrder()
        {
            string[] names = new string[] { "apple", ("banana"), ("carrot") };

            // Arrange
            var ob = new ObjectBuilder();
            for (int i = 0; i < names.Length; i++)
            {
                string item = names[i];
                ob.SetScalar<int>(item);
            }

            // Act
            Container c = ob.BuildContainer();

            // Assert
            var view = c.View;
            var ns = view.NameSegment;

            for (int i = 0; i < names.Length; i++)
            {
                string item = names[i];
                Assert.AreEqual(view.GetFieldName(i).ToString(), item);
            }
        }

        [Test]
        public void Layout_CreatesZeroInitializedInstances_WithCorrectOffsets()
        {
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("hp");
            ob.SetScalar<long>("ticks");
            ob.SetArray<int>("scores", arraySize: 3);

            var layout = ob.BuildLayout();
            Assert.Greater(layout.TotalLength, 0);

            // Create a zero-initialized container from layout
            var c = CreateWildContainer(layout);
            var v = c.View;

            // Field count
            Assert.AreEqual(3, v.FieldCount);

            // Resolve indices by name (do not assume insertion order)
            int iHp = v.IndexOf("hp");
            int iScores = v.IndexOf("scores");
            int iTicks = v.IndexOf("ticks");

            Assert.GreaterOrEqual(iHp, 0);
            Assert.GreaterOrEqual(iScores, 0);
            Assert.GreaterOrEqual(iTicks, 0);

            // Names (optional sanity, but using positions resolved above)
            Assert.AreEqual("hp", v.GetFieldName(iHp).ToString());
            Assert.AreEqual("scores", v.GetFieldName(iScores).ToString());
            Assert.AreEqual("ticks", v.GetFieldName(iTicks).ToString());

            // Offsets must be absolute and cumulative in the sorted-by-name order:
            // sorted: "hp"(4) -> "scores"(3*4=12) -> "ticks"(8)
            int ds = v.Header.DataOffset;
            var fHp = v.Fields[iHp];
            var fScores = v.Fields[iScores];
            var fTicks = v.Fields[iTicks];

            // compute expected absolute offsets using sizes
            int hpSize = sizeof(int);
            int scoresSize = 3 * sizeof(int);

            // Determine actual order by comparing Name to enforce the cumulative check
            // (If you want, you can just assert exact values directly since we know the sort: hp < scores < ticks)
            Assert.AreEqual(ds, fHp.DataOffset);
            Assert.AreEqual(ds + hpSize, fScores.DataOffset);
            Assert.AreEqual(ds + hpSize + scoresSize, fTicks.DataOffset);

            // Data are zeros
            Assert.IsTrue(v.GetFieldBytes(iHp).ToArray().All(b => b == 0));
            Assert.IsTrue(v.GetFieldBytes(iScores).ToArray().All(b => b == 0));
            Assert.IsTrue(v.GetFieldBytes(iTicks).ToArray().All(b => b == 0));

            // Write some values and read back
            BinaryPrimitives.WriteInt32LittleEndian(v.GetFieldBytes(iHp), 7);
            BinaryPrimitives.WriteInt64LittleEndian(v.GetFieldBytes(iTicks), 999);

            var scores = v.GetFieldBytes(iScores);
            BinaryPrimitives.WriteInt32LittleEndian(scores.Slice(0, 4), 10);
            BinaryPrimitives.WriteInt32LittleEndian(scores.Slice(4, 4), 20);
            BinaryPrimitives.WriteInt32LittleEndian(scores.Slice(8, 4), 30);

            Assert.AreEqual(7, BitConverter.ToInt32(v.GetFieldBytes(iHp)));
            Assert.AreEqual(999, BitConverter.ToInt64(v.GetFieldBytes(iTicks)));
            CollectionAssert.AreEqual(new[] { 10, 20, 30 }, new[]
            {
                BitConverter.ToInt32(scores[..4]),
                BitConverter.ToInt32(scores.Slice(4,4)),
                BitConverter.ToInt32(scores.Slice(8,4))
            });
        }

        [Test]
        public void Builder_StringAPIs_GenericNoFieldType_Sanity()
        {
            var ob = new ObjectBuilder();

            // Scalars
            ob.SetScalar<int>("hp");
            ob.SetScalar<float>("ratio", 0.5f);

            // Arrays
            ob.SetArray<int>("scores", 3);
            ob.SetArray<float>("weights", new float[] { 1, 2, 3 });

            // Refs
            ob.SetRef("child", 0UL);
            ob.SetRefArray("children", 2);

            // Raw bytes
            ob.SetArray<byte>("blob", new byte[] { 1, 2, 3, 4 });

            var layout = ob.BuildLayout();
            var c = CreateWildContainer(layout);
            var v = c.View;

            // names exist and sorted
            Assert.GreaterOrEqual(v.IndexOf("hp".AsSpan()), 0);
            Assert.GreaterOrEqual(v.IndexOf("ratio".AsSpan()), 0);
            Assert.GreaterOrEqual(v.IndexOf("scores".AsSpan()), 0);
            Assert.GreaterOrEqual(v.IndexOf("weights".AsSpan()), 0);
            Assert.GreaterOrEqual(v.IndexOf("child".AsSpan()), 0);
            Assert.GreaterOrEqual(v.IndexOf("children".AsSpan()), 0);
            Assert.GreaterOrEqual(v.IndexOf("blob".AsSpan()), 0);

            // types/sizes (spot-check)
            var iScores = v.IndexOf("scores".AsSpan());
            Assert.AreEqual(3 * sizeof(int), v.Fields[iScores].Length);
            Assert.AreEqual(sizeof(int), v.Fields[iScores].ElemSize);
            Assert.IsTrue(v.Fields[iScores].FieldType.IsInlineArray);

            var iChild = v.IndexOf("child".AsSpan());
            Assert.IsTrue(v.Fields[iChild].IsRef);
            Assert.AreEqual(8, v.Fields[iChild].Length);

            var iChildren = v.IndexOf("children".AsSpan());
            Assert.IsTrue(v.Fields[iChildren].IsRef);
            Assert.IsTrue(v.Fields[iChildren].FieldType.IsInlineArray);
            Assert.AreEqual(2 * 8, v.Fields[iChildren].Length);
        }

        private static Container CreateWildContainer(ContainerLayout layout)
        {
            return Container.Registry.Shared.CreateWild(layout, "");
        }
    }
}
