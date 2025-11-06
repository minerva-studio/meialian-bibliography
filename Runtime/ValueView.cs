using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Amlos.Container.MigrationConverter;

namespace Amlos.Container
{
    /// <summary>
    /// Represent a single value with type information.
    /// </summary>
    public readonly ref struct ValueView
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




        public ValueView(ReadOnlySpan<byte> bytes, ValueType type) : this()
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
        public bool TryWrite(Span<byte> dst, ValueType targetType, bool isExplicit = false)
        {
            // Quick path: identical type -> copy as much as target expects (avoid overrun)
            if (Type == targetType)
            {
                Bytes.CopyTo(dst);
                return true;
            }

            // If implicit-only and conversion not allowed by implicit table -> fail
            if (!isExplicit && !TypeUtil.IsImplicitlyConvertible(Type, targetType))
                return false;

            // classify types
            bool srcIsInt = Type.IsIntegral();
            bool dstIsInt = targetType.IsIntegral();
            bool srcIsFloat = Type.IsFloatingPoint();
            bool dstIsFloat = targetType.IsFloatingPoint();
            bool srcIsBool = Type == ValueType.Bool;
            bool dstIsBool = targetType == ValueType.Bool;
            bool srcIsChar = Type == ValueType.Char16;
            bool dstIsChar = targetType == ValueType.Char16;

            try
            {
                // ---- Boolean conversions ----
                if (srcIsBool && dstIsBool)
                {
                    dst[0] = (byte)(Bytes.Length > 0 && Bytes[0] != 0 ? 1 : 0);
                    if (dst.Length > 1) dst.Slice(1).Clear();
                    return true;
                }

                if (srcIsBool && !dstIsBool)
                {
                    bool b = Bytes.Length > 0 && Bytes[0] != 0;
                    double d = b ? 1.0 : 0.0;
                    WriteFromDouble(dst, targetType, d);
                    return true;
                }

                if (!srcIsBool && dstIsBool)
                {
                    bool nonzero = !IsZero(Bytes, Type);
                    dst[0] = (byte)(nonzero ? 1 : 0);
                    if (dst.Length > 1) dst.Slice(1).Clear();
                    return true;
                }

                // ---- Char16 handling (treat as unsigned 16-bit) ----
                if (srcIsChar && dstIsChar)
                {
                    int copy = Math.Min(2, Math.Min(Bytes.Length, dst.Length));
                    if (copy > 0) Bytes.Slice(0, copy).CopyTo(dst.Slice(0, copy));
                    if (dst.Length > copy) dst.Slice(copy).Clear();
                    return true;
                }

                if (srcIsChar && !dstIsChar)
                {
                    ushort u = BinaryPrimitives.ReadUInt16LittleEndian(Bytes);
                    WriteFromULong(dst, targetType, u);
                    return true;
                }

                if (!srcIsChar && dstIsChar)
                {
                    if (srcIsFloat)
                    {
                        double d = ReadDouble(Bytes, Type);
                        ushort u = (ushort)(int)Math.Truncate(d);
                        BinaryPrimitives.WriteUInt16LittleEndian(dst, u);
                    }
                    else
                    {
                        ulong u = ReadULong(Bytes, Type);
                        BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)u);
                    }
                    return true;
                }

                // ---- Integer <-> Integer conversions ----
                if (srcIsInt && dstIsInt)
                {
                    if (Type.IsUnsignedInteger())
                    {
                        ulong u = ReadULong(Bytes, Type);
                        WriteFromULong(dst, targetType, u);
                    }
                    else
                    {
                        long s = ReadLong(Bytes, Type);
                        WriteFromLong(dst, targetType, s);
                    }
                    return true;
                }

                // ---- Integer -> Float conversions ----
                if (srcIsInt && dstIsFloat)
                {
                    double d = Type.IsUnsignedInteger() ? (double)ReadULong(Bytes, Type) : (double)ReadLong(Bytes, Type);
                    WriteFromDouble(dst, targetType, d);
                    return true;
                }

                // ---- Float -> Integer conversions ----
                if (srcIsFloat && dstIsInt)
                {
                    double d = ReadDouble(Bytes, Type);
                    long s = (long)Math.Truncate(d); // truncate toward zero
                    WriteFromLong(dst, targetType, s);
                    return true;
                }

                // ---- Float <-> Float conversions ----
                if (srcIsFloat && dstIsFloat)
                {
                    if (targetType == ValueType.Float32)
                    {
                        float f = (float)ReadDouble(Bytes, Type);
                        int bits = BitConverter.SingleToInt32Bits(f);
                        BinaryPrimitives.WriteInt32LittleEndian(dst, bits);
                    }
                    else
                    {
                        double d = ReadDouble(Bytes, Type);
                        long bits = BitConverter.DoubleToInt64Bits(d);
                        BinaryPrimitives.WriteInt64LittleEndian(dst, bits);
                    }
                    return true;
                }

                // If we fell through, conversion is unsupported even with explicit permission
                return false;
            }
            catch
            {
                // treat any unexpected error as conversion failure (caller should drop/clear)
                return false;
            }
        }

        public bool TryRead<T>(out T value, bool isExplicit = false) where T : unmanaged
        {
            Span<byte> buffer = stackalloc byte[Unsafe.SizeOf<T>()];
            if (TryWrite(buffer, TypeUtil.PrimOf<T>(), isExplicit))
            {
                value = MemoryMarshal.Read<T>(buffer);
                return true;
            }
            value = default;
            return false;
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
                    // special case: decode as UTF-16 string
                    return MemoryMarshal.Cast<byte, char>(Bytes).ToString();
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
                    ulong id = MemoryMarshal.Read<ulong>(Bytes);
                    if (id != 0UL && Container.Registry.Shared.GetContainer(id) is Container container)
                    {
                        return container.ToString();
                    }
                    return "null";
                default:
                    break;
            }
            return "Unknown";

        }

        string ToArrayOrSingleString<T>() where T : unmanaged
        {
            if (Bytes.Length > Unsafe.SizeOf<T>())
            {
                var arr = MemoryMarshal.Cast<byte, T>(Bytes);
                return "[" + string.Join(", ", arr.ToArray()) + "]";
            }
            return MemoryMarshal.Read<T>(Bytes).ToString();
        }
    }

    public static class HexExtensions
    {
        private static readonly char[] HexUpper = "0123456789ABCDEF".ToCharArray();
        private static readonly char[] HexLower = "0123456789abcdef".ToCharArray();

        public static string ToHex(this ReadOnlySpan<byte> bytes, bool uppercase = true)
        {
            if (bytes.Length == 0) return string.Empty;
            var table = uppercase ? HexUpper : HexLower;
            char[] chars = new char[bytes.Length * 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                var b = bytes[i];
                chars[i * 2] = table[b >> 4];
                chars[i * 2 + 1] = table[b & 0x0F];
            }
            return new string(chars);
        }
    }
}
