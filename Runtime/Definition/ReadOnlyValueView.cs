using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
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

        public T Read<T>(bool isExplicit = false) where T : unmanaged
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



        public override string ToString()
        {
            switch (Type)
            {
                case ValueType.Unknown:
                    return "RawData:" + Bytes.ToHex();

                case ValueType.Bool:
                    return ToArrayOrSingleString<bool>();

                case ValueType.Int8:
                    return ToArrayOrSingleString<sbyte>();
                case ValueType.UInt8:
                    return ToArrayOrSingleString<byte>();

                case ValueType.Char16:
                    {
                        // Ensure even number of bytes, truncate if odd.
                        int even = Bytes.Length & ~1;
                        if (even <= 0) return string.Empty;
                        var chars = MemoryMarshal.Cast<byte, char>(Bytes.Slice(0, even));
                        // Build string from span (not Span<T>.ToString()).
                        return new string(chars);
                    }

                case ValueType.Int16:
                    return ToArrayOrSingleString<short>();
                case ValueType.UInt16:
                    return ToArrayOrSingleString<ushort>();
                case ValueType.Int32:
                    return ToArrayOrSingleString<int>();
                case ValueType.UInt32:
                    return ToArrayOrSingleString<uint>();
                case ValueType.Int64:
                    return ToArrayOrSingleString<long>();
                case ValueType.UInt64:
                    return ToArrayOrSingleString<ulong>();
                case ValueType.Float32:
                    return ToArrayOrSingleString<float>();
                case ValueType.Float64:
                    return ToArrayOrSingleString<double>();

                case ValueType.Ref:
                    {
                        // Guard length
                        if (Bytes.Length < 8) return "null";
                        ulong id = MemoryMarshal.Read<ulong>(Bytes);
                        if (id != 0UL && Container.Registry.Shared.GetContainer(id) is Container container)
                            return container.ToString();
                        return "null";
                    }
            }
            return "Unknown";
        }

        private string ToArrayOrSingleString<T>() where T : unmanaged
        {
            int sz = TypeUtil<T>.Size;
            if (Bytes.Length > sz)
            {
                var arr = MemoryMarshal.Cast<byte, T>(Bytes[..^(Bytes.Length % sz)]);
                return "[" + string.Join(", ", arr.ToArray()) + "]";
            }
            if (Bytes.Length < sz)
            {
                // Not enough bytes to represent a single T; return hex to avoid throwing.
                return "Raw:" + Bytes.ToHex();
            }
            return MemoryMarshal.Read<T>(Bytes).ToString();
        }
    }
}
