using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Represent a single value with type information.
    /// </summary>
    public readonly ref struct ReadOnlyValueView
    {
        public ValueType Type { get; }
        public ReadOnlySpan<byte> Bytes { get; }



        public bool IsNaN
        {
            get
            {
                if (Type == ValueType.Float32)
                {
                    // read 4 bytes little-endian as int bits -> float -> IsNaN
                    int bits = BinaryPrimitives.ReadInt32LittleEndian(Bytes);
                    float f = BitConverter.Int32BitsToSingle(bits);
                    return float.IsNaN(f);
                }
                if (Type == ValueType.Float64)
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(Bytes);
                    double d = BitConverter.Int64BitsToDouble(bits);
                    return double.IsNaN(d);
                }
                return false; // not a float type -> cannot be NaN
            }
        }

        public bool IsPositiveInfinity
        {
            get
            {
                if (Type == ValueType.Float32)
                {
                    int bits = BinaryPrimitives.ReadInt32LittleEndian(Bytes);
                    float f = BitConverter.Int32BitsToSingle(bits);
                    return float.IsPositiveInfinity(f);
                }
                if (Type == ValueType.Float64)
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(Bytes);
                    double d = BitConverter.Int64BitsToDouble(bits);
                    return double.IsPositiveInfinity(d);
                }
                return false;
            }
        }

        public bool IsNegativeInfinity
        {
            get
            {
                if (Type == ValueType.Float32)
                {
                    int bits = BinaryPrimitives.ReadInt32LittleEndian(Bytes);
                    float f = BitConverter.Int32BitsToSingle(bits);
                    return float.IsNegativeInfinity(f);
                }
                if (Type == ValueType.Float64)
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(Bytes);
                    double d = BitConverter.Int64BitsToDouble(bits);
                    return double.IsNegativeInfinity(d);
                }
                return false;
            }
        }

        public bool IsInfinity => IsPositiveInfinity || IsNegativeInfinity;

        public bool IsFinite
        {
            get
            {
                if (Type == ValueType.Float32)
                {
                    int bits = BinaryPrimitives.ReadInt32LittleEndian(Bytes);
                    float f = BitConverter.Int32BitsToSingle(bits);
                    return !float.IsNaN(f) && !float.IsInfinity(f);
                }
                if (Type == ValueType.Float64)
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(Bytes);
                    double d = BitConverter.Int64BitsToDouble(bits);
                    return !double.IsNaN(d) && !double.IsInfinity(d);
                }
                // non-float types are always finite from FP perspective
                return true;
            }
        }




        public ReadOnlyValueView(ReadOnlySpan<byte> bytes, ValueType type) : this()
        {
            Bytes = bytes;
            Type = type;
        }




        /// <summary>
        /// Try to convert current value (represented by Bytes/Type) into targetType and write into dst.
        /// - Returns true on success (dst is written).
        /// - Returns false when the conversion is not allowed/supported (no exception).
        /// - If isExplicit == false: only allow implicit conversions (TypeUtil.IsImplicitlyConvertible).
        /// - If isExplicit == true: allow explicit/narrowing conversions when possible.
        /// </summary>
        public bool TryWriteTo(Span<byte> dst, ValueType dstType, bool isExplicit = false)
        {
            // Quick path: identical type -> copy as much as target expects (avoid overrun)
            ValueType srcType = Type;
            ReadOnlySpan<byte> src = Bytes;
            return Migration.TryWriteTo(src, srcType, dst, dstType, isExplicit);
        }

        public bool TryRead<T>(out T value, bool isExplicit = false) where T : unmanaged
        {
            Span<byte> buffer = stackalloc byte[TypeUtil<T>.Size];
            if (TryWriteTo(buffer, TypeUtil<T>.ValueType, isExplicit))
            {
                value = MemoryMarshal.Read<T>(buffer);
                return true;
            }
            value = default;
            return false;
        }

        public T Read<T>(bool isExplicit = true) where T : unmanaged
        {
            if (!TryRead(out T t, isExplicit))
                throw new InvalidCastException();

            return t;
        }




        public static ReadOnlyValueView Create<T>(ref T value) where T : unmanaged
        {
            var buffer = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref value, 1));
            return Create<T>(buffer);
        }

        public static ReadOnlyValueView Create<T>(ReadOnlySpan<byte> buffer) where T : unmanaged
        {
            return new ReadOnlyValueView(buffer, TypeUtil<T>.ValueType);
        }



        public override string ToString() => Migration.ToString(Bytes, Type);

        public static unsafe implicit operator ReadOnlyValueView(int* valueView) => new ReadOnlyValueView(new Span<byte>(valueView, sizeof(int)), ValueType.Int32);
        public static unsafe implicit operator ReadOnlyValueView(long* valueView) => new ReadOnlyValueView(new Span<byte>(valueView, sizeof(long)), ValueType.Int64);
        public static unsafe implicit operator ReadOnlyValueView(float* valueView) => new ReadOnlyValueView(new Span<byte>(valueView, sizeof(float)), ValueType.Float32);
        public static unsafe implicit operator ReadOnlyValueView(double* valueView) => new ReadOnlyValueView(new Span<byte>(valueView, sizeof(double)), ValueType.Float64);
    }
}
