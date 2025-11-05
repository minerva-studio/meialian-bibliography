using System;
using System.Buffers.Binary;
using System.Linq;
using NUnit.Framework;
using static Amlos.Container.TypeUtil; // Pack, PrimOf<T>()

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class MigrationConverterElementwiseTests
    {
        private static byte[] Int32ArrayToBytes(params int[] vals)
        {
            var buf = new byte[vals.Length * 4];
            for (int i = 0; i < vals.Length; i++)
                BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(i * 4, 4), vals[i]);
            return buf;
        }

        private static byte[] FloatArrayToBytes(params float[] vals)
        {
            var buf = new byte[vals.Length * 4];
            for (int i = 0; i < vals.Length; i++)
            {
                int bits = BitConverter.SingleToInt32Bits(vals[i]);
                BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(i * 4, 4), bits);
            }
            return buf;
        }

        private static byte[] DoubleArrayToBytes(params double[] vals)
        {
            var buf = new byte[vals.Length * 8];
            for (int i = 0; i < vals.Length; i++)
            {
                long bits = BitConverter.DoubleToInt64Bits(vals[i]);
                BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(i * 8, 8), bits);
            }
            return buf;
        }

        private static float[] BytesToFloatArray(byte[] buf)
        {
            int n = buf.Length / 4;
            var outv = new float[n];
            for (int i = 0; i < n; i++)
            {
                int bits = BinaryPrimitives.ReadInt32LittleEndian(buf.AsSpan(i * 4, 4));
                outv[i] = BitConverter.Int32BitsToSingle(bits);
            }
            return outv;
        }

        private static int[] BytesToInt32Array(byte[] buf)
        {
            int n = buf.Length / 4;
            var outv = new int[n];
            for (int i = 0; i < n; i++)
                outv[i] = BinaryPrimitives.ReadInt32LittleEndian(buf.AsSpan(i * 4, 4));
            return outv;
        }

        [Test]
        public void Int32Array_To_Float32Array_Elementwise()
        {
            int[] src = new[] { 1, -2, 123456 };
            var srcBytes = Int32ArrayToBytes(src);
            var dst = new byte[srcBytes.Length];

            MigrationConverter.MigrateValueFieldBytes(srcBytes, dst, ValueType.Int32, ValueType.Float32);

            var got = BytesToFloatArray(dst);
            Assert.AreEqual(src.Length, got.Length);

            for (int i = 0; i < src.Length; i++)
            {
                float expected = (float)src[i]; // elementwise cast
                int expBits = BitConverter.SingleToInt32Bits(expected);
                int gotBits = BitConverter.SingleToInt32Bits(got[i]);
                Assert.AreEqual(expBits, gotBits, $"element {i} expected bits {expBits:X8} got {gotBits:X8}");
            }
        }

        [Test]
        public void DoubleArray_To_Float32Array_ElementwiseCasting()
        {
            double[] src = new[] { 1.23456789012345, -2.5, 1e40 }; // last overflows to +Inf
            var srcBytes = DoubleArrayToBytes(src);
            var dst = new byte[src.Length * 4];

            // expected: elementwise cast using C# semantics
            float[] exp = src.Select(s => (float)s).ToArray();

            // ---- CALL MIGRATION FIRST ----
            MigrationConverter.MigrateValueFieldBytes(srcBytes, dst, ValueType.Float64, ValueType.Float32, true);

            // ---- THEN read back floats ----
            var got = BytesToFloatArray(dst);

            // compare elementwise with a small tolerance for finite values
            for (int i = 0; i < exp.Length; i++)
            {
                if (float.IsNaN(exp[i]))
                {
                    Assert.IsTrue(float.IsNaN(got[i]), $"idx {i}: expected NaN");
                }
                else if (float.IsInfinity(exp[i]))
                {
                    Assert.IsTrue(float.IsInfinity(got[i]) && Math.Sign(exp[i]) == Math.Sign(got[i]),
                        $"idx {i}: expected Infinity {exp[i]} got {got[i]}");
                }
                else
                {
                    Assert.AreEqual(exp[i], got[i], 1e-6, $"idx {i} mismatch: expected {exp[i]} got {got[i]}");
                }
            }
        }

        [Test]
        public void Float32Array_To_Int32Array_TruncateElementwise()
        {
            float[] src = new[] { 1.9f, -2.9f, 3.0f };
            var srcBytes = FloatArrayToBytes(src);
            var dst = new byte[srcBytes.Length];

            MigrationConverter.MigrateValueFieldBytes(srcBytes, dst, ValueType.Float32, ValueType.Int32, true);

            var got = BytesToInt32Array(dst);
            Assert.AreEqual(src.Length, got.Length);

            for (int i = 0; i < src.Length; i++)
            {
                int expected = (int)Math.Truncate(src[i]); // elementwise truncate
                Assert.AreEqual(expected, got[i], $"idx {i} expected {expected} got {got[i]}");
            }
        }

        [Test]
        public void PartialCopy_And_ZeroFill_Behaviour()
        {
            int[] src = new[] { 10, 20, 30 };
            var srcBytes = Int32ArrayToBytes(src);
            var dst = new byte[5 * 4]; // larger target: 5 ints
            byte hint = Pack(PrimOf<int>(), isArray: true);

            MigrationConverter.MigrateValueFieldBytes(srcBytes, dst, hint, hint);

            var got = BytesToInt32Array(dst);
            Assert.AreEqual(5, got.Length);
            Assert.AreEqual(10, got[0]);
            Assert.AreEqual(20, got[1]);
            Assert.AreEqual(30, got[2]);
            Assert.AreEqual(0, got[3], "tail should be zero filled");
            Assert.AreEqual(0, got[4], "tail should be zero filled");
        }

        [Test]
        public void UnknownHint_RawCopyAndZeroFill()
        {
            byte[] src = new byte[] { 1, 2, 3, 4 };
            var dst = new byte[8];
            byte unknown = 0; // ValueType.Unknown
            byte newHint = Pack(PrimOf<int>(), isArray: false);

            MigrationConverter.MigrateValueFieldBytes(src, dst, unknown, newHint);

            CollectionAssert.AreEqual(new byte[] { 1, 2, 3, 4, 0, 0, 0, 0 }, dst);
        }

        [Test]
        public void ConvertScalarInPlace_Int32ToFloat_WritesFloatBits()
        {
            var buf = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(0, 4), 42);

            MigrationConverter.ConvertScalarInPlace(buf.AsSpan(), PrimOf<int>(), PrimOf<float>());

            // verify bit-exact float representation of 42.0f
            int gotBits = BinaryPrimitives.ReadInt32LittleEndian(buf);
            int expectedBits = BitConverter.SingleToInt32Bits(42.0f);
            Assert.AreEqual(expectedBits, gotBits);
        }

        [Test]
        public void ConvertArrayInPlaceSameSize_Int32ToFloat_Elementwise()
        {
            int[] src = new[] { 5, -6, 7 };
            var buf = Int32ArrayToBytes(src);

            MigrationConverter.ConvertArrayInPlaceSameSize(buf.AsSpan(), src.Length, PrimOf<int>(), PrimOf<float>());

            var got = BytesToFloatArray(buf);
            Assert.AreEqual(src.Length, got.Length);
            for (int i = 0; i < src.Length; i++)
            {
                float expected = (float)src[i];
                int expBits = BitConverter.SingleToInt32Bits(expected);
                int gotBits = BitConverter.SingleToInt32Bits(got[i]);
                Assert.AreEqual(expBits, gotBits, $"idx {i} expected bits {expBits:X8} got {gotBits:X8}");
            }
        }








        [Test]
        public void DoubleArray_To_FloatArray_Elementwise_Casting_ValueTolerance()
        {
            double[] src = new[] { 1.23456789012345, -2.5, 1e40 }; // last overflows to Infinity
            var srcBytes = DoubleArrayToBytes(src);
            var dst = new byte[src.Length * 4];

            // expected elementwise cast
            float[] exp = src.Select(d => (float)d).ToArray();

            // CALL migration THEN read
            MigrationConverter.MigrateValueFieldBytes(srcBytes, dst, ValueType.Float64, ValueType.Float32, true);
            var got = BytesToFloatArray(dst);

            Assert.AreEqual(exp.Length, got.Length);
            for (int i = 0; i < exp.Length; i++)
            {
                if (float.IsNaN(exp[i])) Assert.IsTrue(float.IsNaN(got[i]));
                else if (float.IsInfinity(exp[i])) Assert.IsTrue(float.IsInfinity(got[i]) && Math.Sign(exp[i]) == Math.Sign(got[i]));
                else Assert.AreEqual(exp[i], got[i], 1e-6, $"idx {i} mismatch");
            }
        }

        [Test]
        public void Int32Array_To_Float32Array_Elementwise_BitExact()
        {
            int[] src = new[] { 1, -2, 123456 };
            var srcBytes = Int32ArrayToBytes(src);
            var dst = new byte[srcBytes.Length];
            byte oldHint = Pack(PrimOf<int>(), isArray: true);
            byte newHint = Pack(PrimOf<float>(), isArray: true);

            MigrationConverter.MigrateValueFieldBytes(srcBytes, dst, oldHint, newHint);

            var got = BytesToFloatArray(dst);
            Assert.AreEqual(src.Length, got.Length);
            for (int i = 0; i < src.Length; i++)
            {
                float expected = (float)src[i];
                int expBits = BitConverter.SingleToInt32Bits(expected);
                int gotBits = BitConverter.SingleToInt32Bits(got[i]);
                Assert.AreEqual(expBits, gotBits, $"idx {i} bits differ");
            }
        }

        [Test]
        public void Float32Array_To_Int32Array_Truncate()
        {
            float[] src = new[] { 1.9f, -2.9f, 3.0f };
            var srcBytes = FloatArrayToBytes(src);
            var dst = new byte[srcBytes.Length];

            MigrationConverter.MigrateValueFieldBytes(srcBytes, dst, ValueType.Float32, ValueType.Int32, true);

            var got = BytesToInt32Array(dst);
            for (int i = 0; i < src.Length; i++)
            {
                int expected = (int)Math.Truncate(src[i]);
                Assert.AreEqual(expected, got[i], $"idx {i}");
            }
        }

        [Test]
        public void PartialCopy_TargetBigger_ZeroFilledTail()
        {
            int[] src = new[] { 10, 20, 30 };
            var srcBytes = Int32ArrayToBytes(src);
            var dst = new byte[5 * 4]; // larger target: 5 ints
            byte hint = Pack(PrimOf<int>(), isArray: true);

            MigrationConverter.MigrateValueFieldBytes(srcBytes, dst, hint, hint);

            var got = BytesToInt32Array(dst);
            Assert.AreEqual(10, got[0]);
            Assert.AreEqual(20, got[1]);
            Assert.AreEqual(30, got[2]);
            Assert.AreEqual(0, got[3]);
            Assert.AreEqual(0, got[4]);
        }

        [Test]
        public void NonElementAligned_SourceTail_FallbacksToRawCopy()
        {
            // src length not multiple of element size: e.g., 10 bytes with oldElem=8 (double)
            byte[] src = new byte[10];
            for (int i = 0; i < src.Length; i++) src[i] = (byte)(i + 1);
            var dst = new byte[16];
            byte oldHint = 0; // mark as Unknown to emulate fallback check, or use a double hint but src misaligned
            byte newHint = Pack(PrimOf<long>(), isArray: true);

            // Use mismatched sizing: mark oldVt as Float64 (8) but src length 10 not divisible.
            byte oldDoubleHint = Pack(PrimOf<double>(), isArray: true);

            MigrationConverter.MigrateValueFieldBytes(src, dst, oldDoubleHint, newHint);

            // Expect first min(len(src), len(dst)) bytes copied, remaining zeroed
            var expected = new byte[16];
            Array.Copy(src, expected, src.Length);
            CollectionAssert.AreEqual(expected, dst);
        }
    }
}
