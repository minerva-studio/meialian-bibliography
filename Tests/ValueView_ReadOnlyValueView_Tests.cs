using NUnit.Framework;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class ValueView_ReadOnlyValueView_Tests
    {
        // Helper: make a fixed-size buffer for a primitive type
        private static Span<byte> BufOf<T>() where T : unmanaged
            => new byte[Unsafe.SizeOf<T>()];

        [Test]
        public void TryWrite_Succeeds_For_Int16_To_Int32_Implicit()
        {
            // Arrange
            short srcVal = 1234;
            var srcBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref srcVal, 1)).ToArray();
            var ro = new ReadOnlyValueView(srcBytes, ValueType.Int16);

            var dst = BufOf<int>();
            var vw = new ValueView(dst, ValueType.Int32);

            // Act
            bool ok = vw.TryWrite(ro, isExplicit: false);

            // Assert
            Assert.IsTrue(ok, "Int16 -> Int32 should be implicitly convertible");
            int got = BinaryPrimitives.ReadInt32LittleEndian(dst);
            Assert.AreEqual(1234, got);
        }

        [Test]
        public void Write_Throws_When_Implicit_Not_Allowed()
        {
            // Float32 -> Char16 is NOT implicit (narrowing & semantic change)
            float f = 65.4f;
            var src = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref f, 1)).ToArray();
            var ro = new ReadOnlyValueView(src, ValueType.Float32);

            var dst = BufOf<ushort>(); // char16 storage
            var vw = new ValueView(dst, ValueType.Char16);

            bool threw = false;
            try
            {
                vw.Write(ro, isExplicit: false);
            }
            catch (InvalidCastException)
            {
                threw = true;
            }
            Assert.True(threw);
        }

        [Test]
        public void Write_Allows_Explicit_Narrowing()
        {
            // With explicit=true, allow Float32 -> Char16 (truncation)
            float f = 66.9f; // 'B' ~= 66
            var src = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref f, 1)).ToArray();
            var ro = new ReadOnlyValueView(src, ValueType.Float32);

            var dst = BufOf<ushort>(); // char16 storage
            var vw = new ValueView(dst, ValueType.Char16);

            bool threw = false;
            try
            {
                vw.Write(ro, isExplicit: true);
            }
            catch (Exception)
            {
                threw = true;
            }
            Assert.False(threw);

            ushort u = BinaryPrimitives.ReadUInt16LittleEndian(dst);
            Assert.AreEqual((ushort)66, u);
        }

        [Test]
        public void SameType_Copy_Does_Not_Overflow_And_Zeros_Remainder()
        {
            // Arrange: Int64 source into a larger destination (simulate larger slot)
            long v = 0x1122334455667788L;
            var src = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref v, 1)).ToArray();
            var ro = new ReadOnlyValueView(src, ValueType.Int64);

            byte[] bigDst = new byte[16];
            var vw = new ValueView(bigDst, ValueType.Int64);

            // Act
            bool ok = vw.TryWrite(ro);
            // Assert
            Assert.IsTrue(ok);
            Assert.AreEqual(src, bigDst[..8]);
            Assert.That(bigDst[8..], Is.EqualTo(new byte[8]));
        }

        [Test]
        public void NaN_And_Infinity_Predicates_Work_And_Are_Length_Safe()
        {
            // float NaN
            float fNaN = float.NaN;
            var fNaNBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref fNaN, 1)).ToArray();
            var roF = new ReadOnlyValueView(fNaNBytes, ValueType.Float32);
            Assert.IsTrue(roF.IsNaN);
            Assert.IsFalse(roF.IsFinite);

            // double +Inf
            double dInf = double.PositiveInfinity;
            var dInfBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref dInf, 1)).ToArray();
            var roD = new ReadOnlyValueView(dInfBytes, ValueType.Float64);
            Assert.IsTrue(roD.IsPositiveInfinity);
            Assert.IsTrue(roD.IsInfinity);
        }

        [Test]
        public void TryRead_And_Read_Work_For_Types()
        {
            // Arrange: UInt32 value
            uint u = 0xDEADBEEF;
            var src = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref u, 1)).ToArray();
            var ro = new ReadOnlyValueView(src, ValueType.UInt32);

            // TryRead<uint>
            Assert.IsTrue(ro.TryRead<uint>(out var gotU));
            Assert.AreEqual(0xDEADBEEF, gotU);

            // Read<ulong> (implicit widening)
            ulong gotUL = ro.Read<ulong>();
            Assert.AreEqual(0xDEADBEEFUL, gotUL);
        }

        [Test]
        public void ToString_Char16_Uses_Real_String_Not_Span_ToString()
        {
            // "AB" as UTF-16LE
            var s = "AB";
            var bytes = new byte[s.Length * 2];
            MemoryMarshal.Cast<char, byte>(s.AsSpan()).CopyTo(bytes);

            var ro = new ReadOnlyValueView(bytes, ValueType.Char16);
            Assert.AreEqual("AB", ro.ToString());
        }

        [Test]
        public void Ref_ToString_Guards_Length()
        {
            var tooShort = new byte[4];
            var ro = new ReadOnlyValueView(tooShort, ValueType.Ref);
            Assert.AreEqual("null", ro.ToString());
        }

        [Test]
        public void ToArrayOrSingleString_Length_Mismatch_Does_Not_Throw()
        {
            // Int32 but only 2 bytes available -> should not throw
            var bad = new byte[] { 0x34, 0x12 };
            var ro = new ReadOnlyValueView(bad, ValueType.Int32);
            var s = ro.ToString();
            StringAssert.StartsWith("Raw:", s);
        }
    }

    /// <summary>
    /// Reference equality comparer for arrays/objects (test helper).
    /// </summary>
    internal sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T>
        where T : class
    {
        public static readonly ReferenceEqualityComparer<T> Instance = new();
        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);
        public int GetHashCode(T obj) => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj);
    }
}
