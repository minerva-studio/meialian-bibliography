using NUnit.Framework;
using System;
using System.Buffers.Binary;

namespace Minerva.DataStorage.Tests
{
    /// <summary>
    /// Tests for the new non-generic StorageInlineArray API only.
    /// - All reads/writes via ValueView.TryWrite and ValueView.Bytes
    /// - Avoid lambdas/delegates capturing ref struct; use try/catch assertions instead.
    /// </summary>
    [TestFixture]
    public class Array_New_NoGeneric_Tests
    {
        private const string Field_Ints = "ints";
        private const string Field_Floats = "floats";
        private const string Field_Children = "children";

        /// <summary>
        /// Build a layout using ObjectBuilder (arrays are non-ref inline; children is a ref array).
        /// </summary>
        private static ContainerLayout BuildLayout(int intsCount = 5, int floatsCount = 4, int childrenCount = 0)
        {
            return new ObjectBuilder()
                .SetArray<int>(Field_Ints, intsCount)     // non-ref inline int array
                .SetArray<float>(Field_Floats, floatsCount)   // non-ref inline float array
                .SetRefArray(Field_Children, childrenCount) // ref array
                .BuildLayout();
        }

        private static Storage NewStorage(int intsCount = 5, int floatsCount = 4, int childrenCount = 0) => new Storage(BuildLayout(intsCount, floatsCount, childrenCount));

        // ============== 1) Basic: length, write, read, clear =================
        [Test]
        public void Ints_Length_Write_Read_Clear()
        {
            using var storage = NewStorage();
            var root = storage.Root;

            // Non-generic array view
            var ia = root.GetArray(Field_Ints);
            Assert.That(ia.Length, Is.EqualTo(5), "Length mismatch for int inline array.");
            Assert.That(ia.Type, Is.EqualTo(ValueType.Int32), "Type should be Int32 for 'ints' field.");

            // Initially zero
            CollectionAssert.AreEqual(new[] { 0, 0, 0, 0, 0 }, ReadAllInt32(ia), "Initial values should be zero.");

            // Write values via ValueView.TryWrite
            ia.Write(0, 1);
            ia.Write(1, 2);
            ia.Write(2, 3);
            ia.Write(3, 4);
            ia.Write(4, 5);
            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4, 5 }, ReadAllInt32(ia));

            // Clear all
            ia.Clear();
            CollectionAssert.AreEqual(new[] { 0, 0, 0, 0, 0 }, ReadAllInt32(ia));
        }

        // ============== 2) ClearAt single slot =================
        [Test]
        public void Ints_ClearAt_Works()
        {
            using var storage = NewStorage();
            var root = storage.Root;

            var ia = root.GetArray(Field_Ints);
            for (int i = 0; i < ia.Length; i++)
                ia.Write(i, i + 10);

            ia.ClearAt(2); // clear slot 2
            CollectionAssert.AreEqual(new[] { 10, 11, 0, 13, 14 }, ReadAllInt32(ia));
        }

        // ============== 3) Float array write/read (no generics) =================
        [Test]
        public void Floats_Write_Read_With_ValueView()
        {
            using var storage = NewStorage();
            var root = storage.Root;

            var fa = root.GetArray(Field_Floats);
            Assert.That(fa.Length, Is.EqualTo(4), "Length mismatch for float inline array.");
            Assert.That(fa.Type, Is.EqualTo(ValueType.Float32), "Type should be Float32 for 'floats' field.");

            var src = new[] { 1.5f, -2.25f, 0f, 99.875f };
            for (int i = 0; i < src.Length; i++)
                fa.Write(i, src[i]);

            CollectionAssert.AreEqual(src, ReadAllFloat32(fa));
        }

        // ============== 4) Bulk pattern via per-slot writes, then clear =================
        [Test]
        public void Ints_Bulk_Pattern_And_Clear()
        {
            using var storage = NewStorage(intsCount: 6);
            var root = storage.Root;

            var ia = root.GetArray(Field_Ints);
            for (int i = 0; i < ia.Length; i++)
                ia.Write(i, i * i); // 0,1,4,9,16,25

            CollectionAssert.AreEqual(new[] { 0, 1, 4, 9, 16, 25 }, ReadAllInt32(ia));

            ia.Clear();
            CollectionAssert.AreEqual(new[] { 0, 0, 0, 0, 0, 0 }, ReadAllInt32(ia));
        }

        // ============== 5) Out-of-range behavior =================
        // Avoid Assert.Throws with lambdas capturing a ref struct.
        // Use explicit try/catch to assert exceptions.
        [Test]
        public void Index_OutOfRange_Throws_ArgumentOutOfRange()
        {
            using var storage = NewStorage(intsCount: 3);
            var root = storage.Root;

            var ia = root.GetArray(Field_Ints);
            Assert.That(ia.Length, Is.EqualTo(3));

            // Read at -1
            try
            {
                var _ = ia.Scalar[-1];
                Assert.Fail("Expected ArgumentOutOfRangeException for index -1.");
            }
            catch (ArgumentOutOfRangeException) { }

            // Read at 3
            try
            {
                var _ = ia.Scalar[3];
                Assert.Fail("Expected ArgumentOutOfRangeException for index 3.");
            }
            catch (ArgumentOutOfRangeException) { }

            // Write at 3
            try
            {
                ia.Write(3, 99);
                Assert.Fail("Expected ArgumentOutOfRangeException when writing index 3.");
            }
            catch (ArgumentOutOfRangeException) { }
        }

        // ============== 6) Zero-length: safe ops =================
        [Test]
        public void ZeroLength_Array_Safe_Operations()
        {
            using var storage = NewStorage(intsCount: 0);
            var root = storage.Root;

            var ia = root.GetArray(Field_Ints);
            Assert.That(ia.Length, Is.EqualTo(0), "Zero-length inline array expected.");

            ia.Clear(); // should not throw
            CollectionAssert.IsEmpty(ReadAllInt32(ia));
        }

        // ===================== Helpers: ValueView read/write =====================

        /// <summary>Write an Int32 into a ValueView (little-endian).</summary>
        private static void WriteInt32(ValueView vv, int value)
        {
            Span<byte> tmp = stackalloc byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(tmp, value);
            vv.Write(tmp, ValueType.Int32);
        }

        /// <summary>Write a Float32 into a ValueView by bit-casting to Int32 (little-endian).</summary>
        private static void WriteFloat32(ValueView vv, float value)
        {
            Span<byte> tmp = stackalloc byte[4];
            // No BinaryPrimitives.ReadSingleLittleEndian/WriteSingleLittleEndian -> use bit-cast
            BinaryPrimitives.WriteInt32LittleEndian(tmp, BitConverter.SingleToInt32Bits(value));
            vv.Write(tmp, ValueType.Float32);
        }

        /// <summary>Read all Int32 elements from a non-generic inline array.</summary>
        private static int[] ReadAllInt32(StorageArray a)
        {
            var res = new int[a.Length];
            for (int i = 0; i < a.Length; i++)
                res[i] = ReadInt32(a.Scalar[i]);
            return res;
        }

        /// <summary>Read all Float32 elements from a non-generic inline array.</summary>
        private static float[] ReadAllFloat32(StorageArray a)
        {
            var res = new float[a.Length];
            for (int i = 0; i < a.Length; i++)
                res[i] = ReadFloat32(a.Scalar[i]);
            return res;
        }

        /// <summary>Read Int32 from ValueView.Bytes (little-endian).</summary>
        private static int ReadInt32(ReadOnlyValueView vv)
        {
            return BinaryPrimitives.ReadInt32LittleEndian(vv.Bytes);
        }

        /// <summary>Read Float32 from ValueView.Bytes by bit-casting Int32 (little-endian).</summary>
        private static float ReadFloat32(ReadOnlyValueView vv)
        {
            return BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(vv.Bytes));
        }
    }
}
