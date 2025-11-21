using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Linq;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class ContainerTests
    {
        // --- helpers ---
        private static int CharBytes(int charCount) => charCount * sizeof(char);

        [Test]
        public void Build_ZeroInit_DataAllZero()
        {
            // Create several scalar fields; do not assign any values -> defaults are zero.
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("hp");
            ob.SetScalar<int>("spd");
            ob.SetScalar<long>("id");

            var c = ob.BuildContainer();
            var v = c.View;

            // Check total length is header + field headers + names + data (not strictly needed, but sanity)
            int n = v.FieldCount;
            int namesBytes = CharBytes("hp".Length + "spd".Length + "id".Length);
            int expectedMinTotal = ContainerHeader.Size + n * FieldHeader.Size + namesBytes;
            Assert.GreaterOrEqual(c.Length, expectedMinTotal);

            // All data bytes should be zero
            var data = v.DataSegment;
            Assert.IsTrue(data.ToArray().All(b => b == 0), "All uninitialized payload bytes should be zero.");
        }

        [Test]
        public void Build_FromExplicitBytes_Roundtrip()
        {
            // Put a raw 8-byte id payload via SetBytes / SetRaw-like path
            var ob = new ObjectBuilder();
            var ft = new FieldType(ValueType.UInt8, isArray: true); // ElemSize=1
            var payload = new byte[8];
            new System.Random(0xC0FFEE).NextBytes(payload);

            ob.SetBytes("id", ft, payload);

            var c = ob.BuildContainer();
            var v = c.View;

            // Verify field meta and payload
            int idx = v.IndexOf("id".AsSpan());
            Assert.GreaterOrEqual(idx, 0);
            var f = v.Fields[idx];
            Assert.AreEqual(8, f.Length);
            Assert.AreEqual(1, f.ElemSize);

            var got = v.GetFieldBytes(idx).ToArray();
            CollectionAssert.AreEqual(payload, got);
        }

        [Test]
        public void WriteRead_Unmanaged_ByName_Works()
        {
            // Build with empty slots (zero); write later via Container API
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("hp");
            ob.SetScalar<float>("spd");
            var c = ob.BuildContainer();

            c.Write<int>("hp", 123456789, allowRescheme: false);
            c.Write<float>("spd", 3.5f, allowRescheme: false);

            Assert.AreEqual(123456789, c.Read<int>("hp"));
            Assert.That(c.Read<float>("spd"), Is.EqualTo(3.5f).Within(1e-6));
        }

        [Test]
        public void Write_Unmanaged_SizeTooLarge_Throws_WhenNoRescheme()
        {
            // Make a 2-byte field; try to write Int32 with allowRescheme=false -> should throw
            var ob = new ObjectBuilder();
            // Construct a 2-byte slot using a dummy array of bytes (ElemSize=1, Length=2)
            var ft = new FieldType(ValueType.UInt8, true);
            ob.SetBytes("tiny".AsMemory(), (byte)ft, new byte[2]);

            var c = ob.BuildContainer();
            Assert.That(() => c.Write<int>("tiny", 42, allowRescheme: false), Throws.TypeOf<IndexOutOfRangeException>());
        }

        [Test]
        public void TryReadWrite_Unmanaged_Behavior_SmallerFieldFails_LargerFieldSucceeds()
        {
            var ob = new ObjectBuilder();
            // a: 2 bytes, b: 4 bytes
            var ftBytes = new FieldType(ValueType.UInt8, true);
            ob.SetBytes("a", (byte)ftBytes, new byte[2]);
            ob.SetScalar<int>("b"); // 4 bytes

            var c = ob.BuildContainer();

            Assert.IsFalse(c.TryWrite<int>("a", 7, allowRescheme: false)); // too small -> false

            Assert.IsTrue(c.TryWrite<int>("b", 300, allowRescheme: false)); // fits -> true
            Assert.IsTrue(c.TryRead<int>("b", out var v));
            Assert.AreEqual(300, v);

            // reading narrower type from same bytes also succeeds
            Assert.IsTrue(c.TryRead<byte>("b", out _));
        }

        [Test]
        public void WriteBytes_ReadBytes_SizeChecksAndRoundtrip()
        {
            var ob = new ObjectBuilder();
            var ft = new FieldType(ValueType.UInt8, true);
            ob.SetBytes("id", (byte)ft, new byte[8]);

            var c = ob.BuildContainer();

            var payload = new byte[8];
            new System.Random(0xC0FFEE).NextBytes(payload);

            c.WriteBytes("id", payload);
            var dst = new byte[8];
            c.ReadBytes("id", dst);

            CollectionAssert.AreEqual(payload, dst);

            Assert.That(() => c.WriteBytes("id", new byte[7]),
                Throws.TypeOf<ArgumentException>().With.Message.Contains("must equal field length"));
            Assert.IsFalse(c.TryWriteBytes("id", new byte[7]));
        }

        [Test]
        public void TrailingBytes_AreCleared_WhenWritingSmallerT()
        {
            // field length 4; write Int16 then read Int32 -> upper two bytes must be 0
            var ob = new ObjectBuilder();
            // make a 4-byte slot (UInt8 array len=4), then we will write short via Container API
            ob.SetBytes("word", (byte)new FieldType(ValueType.UInt8, true), new byte[4]);
            var c = ob.BuildContainer();

            short s = unchecked((short)0xABCD);  // little-endian bytes [CD AB]
            c.Write<short>("word", s, allowRescheme: false);

            int val = c.Read<int>("word");
            Assert.AreEqual(0x000000CD, val); // shrink to CD
        }

        [Test]
        public void Clone_ProducesIndependentCopy()
        {
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("hp");
            ob.SetScalar<int>("mp");
            var c1 = ob.BuildContainer();

            c1.Write<int>("hp", 10, allowRescheme: false);
            c1.Write<int>("mp", 5, allowRescheme: false);

            var c2 = c1.Clone();
            Assert.AreEqual(10, c2.Read<int>("hp"));
            Assert.AreEqual(5, c2.Read<int>("mp"));

            c1.Write<int>("hp", 77, allowRescheme: false);
            Assert.AreEqual(77, c1.Read<int>("hp"));
            Assert.AreEqual(10, c2.Read<int>("hp")); // independent
        }

        [Test]
        public void CopyFrom_DifferentTotalLengths_Throws()
        {
            // Build two containers of different total byte sizes
            var ob1 = new ObjectBuilder();
            ob1.SetScalar<int>("a");
            var c1 = ob1.BuildContainer();

            var ob2 = new ObjectBuilder();
            ob2.SetScalar<long>("a"); // bigger payload -> larger total
            var c2 = ob2.BuildContainer();

            Assert.That(() => c1.CopyFrom(c2),
                Throws.TypeOf<ArgumentException>().With.Message.Contains("Destination length"));
        }

        [Test]
        public void Dispose_ThenUse_ThrowsOnMethods()
        {
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("x");
            var c = ob.BuildContainer();

            c.Dispose();

            // Span property may not throw by design; call methods that guard against disposed
            Assert.That(() => c.Clear(), Throws.TypeOf<ObjectDisposedException>());
            Assert.That(() => c.IndexOf("x".AsSpan()), Throws.TypeOf<ObjectDisposedException>());
        }

        [Test]
        public void DataOffsets_ShouldBeAbsoluteAndCumulative()
        {
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("a");
            ob.SetScalar<long>("b");
            ob.SetScalar<short>("c");

            var c = ob.BuildContainer();
            var v = c.View;

            int dataStart = v.Header.DataOffset;

            var f0 = v.Fields[0];
            var f1 = v.Fields[1];
            var f2 = v.Fields[2];

            Assert.AreEqual(dataStart, f0.DataOffset);
            Assert.AreEqual(dataStart + sizeof(int), f1.DataOffset);
            Assert.AreEqual(dataStart + sizeof(int) + sizeof(long), f2.DataOffset);

            // And verify reading/writing at those absolute offsets works
            var buf = v.Span; // whole buffer
            BitConverter.GetBytes(123).CopyTo(buf.Slice(f0.DataOffset, f0.Length));
            BitConverter.GetBytes(456789L).CopyTo(buf.Slice(f1.DataOffset, f1.Length));
            BitConverter.GetBytes((short)-7).CopyTo(buf.Slice(f2.DataOffset, f2.Length));

            Assert.AreEqual(123, BitConverter.ToInt32(v.GetFieldBytes(0)));
            Assert.AreEqual(456789L, BitConverter.ToInt64(v.GetFieldBytes(1)));
            Assert.AreEqual((short)-7, BitConverter.ToInt16(v.GetFieldBytes(2)));
        }
    }

    [TestFixture]
    public class ContainerTests_New
    {
        [Test]
        public void Registry_CreateWild_WithExplicitSize()
        {
            int sz = ContainerHeader.Size + 128;
            var wild = Container.Registry.Shared.CreateWild(sz);

            Assert.AreEqual(Container.Registry.ID.Wild, wild.ID);
            Assert.GreaterOrEqual(wild.Span.Length, sz);
            // Depending on implementation, header.Id may be set later; this just asserts it's not the "Empty" default.
            Assert.AreNotEqual(Container.Registry.ID.Empty, wild.ID);
        }

        [Test]
        public void BuildThenView_IndexOf_And_GetFieldBytes()
        {
            // Build via ObjectBuilder
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("x");
            ob.SetScalar<int>("y");

            var c = ob.BuildContainer();
            var view = c.View;

            int ix = view.IndexOf("x".AsSpan());
            int iy = view.IndexOf("y".AsSpan());

            Assert.AreEqual(0, ix);
            Assert.AreEqual(1, iy);

            // write data directly (as if we had a writer)
            var bx = view.GetFieldBytes(ix);
            var by = view.GetFieldBytes(iy);

            BinaryPrimitives.WriteInt32LittleEndian(bx, 123);
            BinaryPrimitives.WriteInt32LittleEndian(by, -456);

            Assert.AreEqual(123, BitConverter.ToInt32(bx));
            Assert.AreEqual(-456, BitConverter.ToInt32(by));
        }

        [Test]
        public void ToString_ShouldListAllFieldsAndValuesView()
        {
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("score");
            ob.SetScalar<long>("ticks");

            var c = ob.BuildContainer();

            // fill some bytes so ToString prints non-empty-ish ValueView
            var score = c.View.GetFieldBytes(0);
            var ticks = c.View.GetFieldBytes(1);
            BinaryPrimitives.WriteInt32LittleEndian(score, 42);
            BinaryPrimitives.WriteInt64LittleEndian(ticks, 9876543210L);

            var s = c.ToString();
            StringAssert.Contains("\"score\"", s);
            StringAssert.Contains("\"ticks\"", s);
        }

        [Test]
        public void GetRefSpan_And_GetRef_ShouldMapToUlongs()
        {
            var ob = new ObjectBuilder();
            // single ref
            ob.SetRef("childA", 11UL);
            // array-of-refs (IsArray=true), and allocate 3 slots (3 * 8 bytes)
            ob.SetRefArray("children", arraySize: 3);

            var c = ob.BuildContainer();

            // single ref
            ref var idRef = ref c.GetRef("childA");
            Assert.AreEqual(11UL, idRef.id);
            idRef = 99UL;
            Assert.AreEqual(99UL, c.GetRef("childA").id);

            // array of refs
            var span = c.GetFieldData<ContainerReference>(c.GetFieldHeader("children"));
            Assert.AreEqual(3, span.Length);
            span[0] = 101UL;
            span[1] = 202UL;
            span[2] = 303UL;

            var view = c.View;
            var data = view.GetFieldBytes(view.IndexOf("children".AsSpan()));
            Assert.AreEqual(101UL, BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(0, 8)));
            Assert.AreEqual(202UL, BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(8, 8)));
            Assert.AreEqual(303UL, BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(16, 8)));
        }

        [Test]
        public void Clone_CopyTo_CopyFrom_BasicSanity()
        {
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("a");
            ob.SetScalar<int>("b");
            var c = ob.BuildContainer();

            var view = c.View;
            BinaryPrimitives.WriteInt32LittleEndian(view.GetFieldBytes(0), 7);
            BinaryPrimitives.WriteInt32LittleEndian(view.GetFieldBytes(1), 9);

            var clone = c.Clone();
            var cv = clone.View;
            Assert.AreEqual(7, BitConverter.ToInt32(cv.GetFieldBytes(0)));
            Assert.AreEqual(9, BitConverter.ToInt32(cv.GetFieldBytes(1)));

            // CopyTo
            Assert.AreEqual(c.Length, c.Memory.Buffer.Length);
            var dst = new byte[c.Length];
            c.CopyTo(dst);
            var c2 = Container.Registry.Shared.CreateWild(dst.Length);
            dst.AsSpan().CopyTo(c2.Span);

            // CopyFrom
            var c3 = Container.Registry.Shared.CreateWild(c.Length);
            c3.CopyFrom(c);
            Assert.AreEqual(c.Length, c3.Length);
            Assert.AreEqual(BitConverter.ToInt32(c.View.GetFieldBytes(0)), BitConverter.ToInt32(c3.View.GetFieldBytes(0)));
        }

        [Test]
        public void Rename_DoesNotBreakFieldAccess_AndPreservesData()
        {
            // Arrange: create a container with multiple fields and fill data
            var ob = new ObjectBuilder();
            ob.SetScalar<int>("hp");
            ob.SetScalar<float>("spd");
            ob.SetArray<int>("arr", 3);

            var c = ob.BuildContainer();

            c.Write("hp", 123, allowRescheme: false);
            c.Write("spd", 4.5f, allowRescheme: false);

            var arrSpan = c.GetFieldData<int>(c.GetFieldHeader("arr"));
            arrSpan[0] = 10;
            arrSpan[1] = 20;
            arrSpan[2] = 30;

            // Sanity before rename
            Assert.AreEqual(123, c.Read<int>("hp"));
            Assert.That(c.Read<float>("spd"), Is.EqualTo(4.5f).Within(1e-6));
            CollectionAssert.AreEqual(new[] { 10, 20, 30 }, c.GetFieldData<int>(c.GetFieldHeader("arr")).ToArray());

            // Act: rename to a different-length name to force offset changes
            c.Rename("renamed-container-with-longer-name");

            // Assert: field lookup by name still works and values are preserved
            Assert.AreEqual(123, c.Read<int>("hp"));
            Assert.That(c.Read<float>("spd"), Is.EqualTo(4.5f).Within(1e-6));

            var arrAfter = c.GetFieldData<int>(c.GetFieldHeader("arr"));
            CollectionAssert.AreEqual(new[] { 10, 20, 30 }, arrAfter.ToArray());

            // Also verify writes still succeed after rename
            c.Write("hp", 777, allowRescheme: false);
            arrAfter[1] = 222;
            Assert.AreEqual(777, c.Read<int>("hp"));
            CollectionAssert.AreEqual(new[] { 10, 222, 30 }, c.GetFieldData<int>(c.GetFieldHeader("arr")).ToArray());

            // IndexOf must still locate the fields
            int ihp = c.IndexOf("hp".AsSpan());
            int ispd = c.IndexOf("spd".AsSpan());
            int iarr = c.IndexOf("arr".AsSpan());
            Assert.GreaterOrEqual(ihp, 0);
            Assert.GreaterOrEqual(ispd, 0);
            Assert.GreaterOrEqual(iarr, 0);
        }
    }

}
