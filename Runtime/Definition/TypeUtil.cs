using System;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Bit layout (1 byte):
    /// - bit7: Array flag
    /// - bit6..5: Reserved for future flags (00 for now)
    /// - bit4..0: ValueType code (no shift; direct cast)
    /// </summary>
    public static class TypeUtil
    {
        // --- Bit layout constants ---
        private const int PRIM_BITS = 5;            // bits 0..4
        private const byte PRIM_MASK = (1 << PRIM_BITS) - 1; // 0b0001_1111
        private const int RESERVED_BITS = 2;            // bits 5..6
        private const byte RESERVED_MASK = (byte)(0b11 << PRIM_BITS); // 0b0110_0000
        private const int IS_ARRAY_BIT = 7;            // bit7
        private const byte IS_ARRAY_MASK = (byte)(1 << IS_ARRAY_BIT); // 0b1000_0000

        // Now ValueType can be used directly in the low 5 bits.
        // No shift needed anymore.
        public const byte Ref = (byte)ValueType.Ref;
        public const byte RefArray = (byte)ValueType.Ref | IS_ARRAY_MASK;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte Pack(ValueType valueType, bool isArray) => (byte)(((byte)valueType) | (isArray ? IS_ARRAY_MASK : (byte)0));

        // Keep array flag (bit7) and reserved bits (5..6); replace low-5 prim bits.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte SetType(ref byte b, ValueType v) => b = (byte)((b & IS_ARRAY_MASK) | ((byte)v & PRIM_MASK));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte SetArray(ref byte b, bool isArray) => b = isArray ? (byte)(b | IS_ARRAY_MASK) : (byte)(b & ~IS_ARRAY_MASK);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsArray(byte hint) => (hint & IS_ARRAY_MASK) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ValueType PrimOf(byte hint) => (ValueType)(hint & PRIM_MASK); // No shift

        /// <summary>
        /// Map ValueType to element byte size; Unknown returns 1 for raw byte copy fallback.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int SizeOf(ValueType vt) => vt switch
        {
            ValueType.Bool => 1,
            ValueType.Int8 => 1,
            ValueType.UInt8 => 1,
            ValueType.Char16 => 2,
            ValueType.Int16 => 2,
            ValueType.UInt16 => 2,
            ValueType.Int32 => 4,
            ValueType.UInt32 => 4,
            ValueType.Float32 => 4,
            ValueType.Int64 => 8,
            ValueType.UInt64 => 8,
            ValueType.Float64 => 8,
            ValueType.Ref => 8,
            _ => 1
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsIntegral(this ValueType vt)
            => vt == ValueType.UInt8 || vt == ValueType.UInt16 || vt == ValueType.UInt32 || vt == ValueType.UInt64
            || vt == ValueType.Int8 || vt == ValueType.Int16 || vt == ValueType.Int32 || vt == ValueType.Int64;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsUnsignedInteger(this ValueType vt) => vt == ValueType.UInt8 || vt == ValueType.UInt16 || vt == ValueType.UInt32 || vt == ValueType.UInt64;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsSignedInteger(this ValueType vt) => vt == ValueType.Int8 || vt == ValueType.Int16 || vt == ValueType.Int32 || vt == ValueType.Int64;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsFloatingPoint(this ValueType vt) => vt == ValueType.Float32 || vt == ValueType.Float64;

        /// <summary>
        /// Conservative C#-like implicit numeric conversion table.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsImplicitlyConvertible(ValueType from, ValueType to)
        {
            if (from == to) return true;
            switch (from)
            {
                case ValueType.Int8:
                    return to == ValueType.Int16 || to == ValueType.Int32 || to == ValueType.Int64
                        || to == ValueType.Float32 || to == ValueType.Float64;
                case ValueType.UInt8:
                    return to == ValueType.Int16 || to == ValueType.UInt16 || to == ValueType.Int32
                        || to == ValueType.UInt32 || to == ValueType.Int64 || to == ValueType.UInt64
                        || to == ValueType.Float32 || to == ValueType.Float64;
                case ValueType.Int16:
                    return to == ValueType.Int32 || to == ValueType.Int64 || to == ValueType.Float32 || to == ValueType.Float64;
                case ValueType.UInt16:
                    return to == ValueType.Int32 || to == ValueType.UInt32 || to == ValueType.Int64 || to == ValueType.UInt64
                        || to == ValueType.Float32 || to == ValueType.Float64;
                case ValueType.Int32:
                    return to == ValueType.Int64 || to == ValueType.Float32 || to == ValueType.Float64;
                case ValueType.UInt32:
                    return to == ValueType.Int64 || to == ValueType.UInt64 || to == ValueType.Float32 || to == ValueType.Float64;
                case ValueType.Int64:
                case ValueType.UInt64:
                    return to == ValueType.Float32 || to == ValueType.Float64;
                case ValueType.Float32:
                    return to == ValueType.Float64;
                case ValueType.Char16:
                    return to == ValueType.Int32 || to == ValueType.UInt32
                        || to == ValueType.Int64 || to == ValueType.UInt64
                        || to == ValueType.Float32 || to == ValueType.Float64;
                case ValueType.Bool:
                    return to == ValueType.Bool;
                default:
                    return false;
            }
        }

        public static string ToString(byte s)
        {
            return s switch
            {
                IS_ARRAY_MASK => "(Missing)[]",
                (byte)ValueType.Ref => "Object",
                (byte)ValueType.Ref | IS_ARRAY_MASK => "Object[]",
                (byte)ValueType.Blob => "Blob",
                (byte)ValueType.Blob | IS_ARRAY_MASK => "Blob[]",
                (byte)ValueType.Bool => TypeUtil<bool>.ValueTypeName,
                (byte)ValueType.Bool | IS_ARRAY_MASK => TypeUtil<bool>.ValueTypeArrayName,
                (byte)ValueType.Int8 => TypeUtil<sbyte>.ValueTypeName,
                (byte)ValueType.Int8 | IS_ARRAY_MASK => TypeUtil<sbyte>.ValueTypeArrayName,
                (byte)ValueType.UInt8 => TypeUtil<byte>.ValueTypeName,
                (byte)ValueType.UInt8 | IS_ARRAY_MASK => TypeUtil<byte>.ValueTypeArrayName,
                (byte)ValueType.Char16 => TypeUtil<char>.ValueTypeName,
                (byte)ValueType.Char16 | IS_ARRAY_MASK => TypeUtil<char>.ValueTypeArrayName,
                (byte)ValueType.Int16 => TypeUtil<short>.ValueTypeName,
                (byte)ValueType.Int16 | IS_ARRAY_MASK => TypeUtil<short>.ValueTypeArrayName,
                (byte)ValueType.UInt16 => TypeUtil<ushort>.ValueTypeName,
                (byte)ValueType.UInt16 | IS_ARRAY_MASK => TypeUtil<ushort>.ValueTypeArrayName,
                (byte)ValueType.Int32 => TypeUtil<int>.ValueTypeName,
                (byte)ValueType.Int32 | IS_ARRAY_MASK => TypeUtil<int>.ValueTypeArrayName,
                (byte)ValueType.UInt32 => TypeUtil<uint>.ValueTypeName,
                (byte)ValueType.UInt32 | IS_ARRAY_MASK => TypeUtil<uint>.ValueTypeArrayName,
                (byte)ValueType.Int64 => TypeUtil<long>.ValueTypeName,
                (byte)ValueType.Int64 | IS_ARRAY_MASK => TypeUtil<long>.ValueTypeArrayName,
                (byte)ValueType.UInt64 => TypeUtil<ulong>.ValueTypeName,
                (byte)ValueType.UInt64 | IS_ARRAY_MASK => TypeUtil<ulong>.ValueTypeArrayName,
                (byte)ValueType.Float32 => TypeUtil<float>.ValueTypeName,
                (byte)ValueType.Float32 | IS_ARRAY_MASK => TypeUtil<float>.ValueTypeArrayName,
                (byte)ValueType.Float64 => TypeUtil<double>.ValueTypeName,
                (byte)ValueType.Float64 | IS_ARRAY_MASK => TypeUtil<double>.ValueTypeArrayName,
                _ => "(Missing)",
            };
        }

        public static string ToString(ValueType valueType)
        {
            return valueType switch
            {
                ValueType.Ref => "Object",
                ValueType.Blob => "Blob",
                ValueType.Bool => TypeUtil<bool>.ValueTypeName,
                ValueType.Int8 => TypeUtil<sbyte>.ValueTypeName,
                ValueType.UInt8 => TypeUtil<byte>.ValueTypeName,
                ValueType.Char16 => TypeUtil<char>.ValueTypeName,
                ValueType.Int16 => TypeUtil<short>.ValueTypeName,
                ValueType.UInt16 => TypeUtil<ushort>.ValueTypeName,
                ValueType.Int32 => TypeUtil<int>.ValueTypeName,
                ValueType.UInt32 => TypeUtil<uint>.ValueTypeName,
                ValueType.Int64 => TypeUtil<long>.ValueTypeName,
                ValueType.UInt64 => TypeUtil<ulong>.ValueTypeName,
                ValueType.Float32 => TypeUtil<float>.ValueTypeName,
                ValueType.Float64 => TypeUtil<double>.ValueTypeName,
                _ => "(Missing)",
            };
        }
    }

    /// <summary>
    /// Bit layout (1 byte):
    /// - bit7: Array flag
    /// - bit6..5: Reserved for future flags (00 for now)
    /// - bit4..0: ValueType code (no shift; direct cast)
    /// </summary>
    public static class TypeUtil<T> where T : unmanaged
    {
        public static readonly ValueType ValueType = Compute();
        public static readonly int Size = Unsafe.SizeOf<T>();
        public static readonly bool IsIntegral = ValueType.IsIntegral();
        public static readonly bool IsUnsignedInteger = ValueType.IsUnsignedInteger();
        public static readonly bool IsSignedInteger = ValueType.IsSignedInteger();
        public static readonly bool IsFloatingPoint = ValueType.IsFloatingPoint();
        public static readonly FieldType ScalarFieldType = Create(false);
        public static readonly FieldType ArrayFieldType = Create(true);
        public static readonly TypeData Type = new(ValueType, (short)Size);
        public static readonly string TypeName = ComputeTypeName();
        public static readonly string ValueTypeName = ComputeValueTypeName();
        public static readonly string ValueTypeArrayName = $"{ValueTypeName}[]";



        public static FieldType Create(bool isArray = false) => new FieldType(ValueType, isArray);

        private static ValueType Compute()
        {
            if (typeof(T).IsEnum)
            {
                var ut = Enum.GetUnderlyingType(typeof(T));
                switch (System.Type.GetTypeCode(ut))
                {
                    case TypeCode.SByte: return ValueType.Int8;
                    case TypeCode.Byte: return ValueType.UInt8;
                    case TypeCode.Int16: return ValueType.Int16;
                    case TypeCode.UInt16: return ValueType.UInt16;
                    case TypeCode.Int32: return ValueType.Int32;
                    case TypeCode.UInt32: return ValueType.UInt32;
                    case TypeCode.Int64: return ValueType.Int64;
                    case TypeCode.UInt64: return ValueType.UInt64;
                    default: return ValueType.Blob;
                }
            }

            if (typeof(T) == typeof(bool)) return ValueType.Bool;
            if (typeof(T) == typeof(sbyte)) return ValueType.Int8;
            if (typeof(T) == typeof(byte)) return ValueType.UInt8;
            if (typeof(T) == typeof(char)) return ValueType.Char16; // UTF-16 code unit
            if (typeof(T) == typeof(short)) return ValueType.Int16;
            if (typeof(T) == typeof(ushort)) return ValueType.UInt16;
            if (typeof(T) == typeof(int)) return ValueType.Int32;
            if (typeof(T) == typeof(uint)) return ValueType.UInt32;
            if (typeof(T) == typeof(long)) return ValueType.Int64;
            if (typeof(T) == typeof(ulong)) return ValueType.UInt64;
            if (typeof(T) == typeof(float)) return ValueType.Float32;
            if (typeof(T) == typeof(double)) return ValueType.Float64;
            if (typeof(T) == typeof(nint)) return Unsafe.SizeOf<nint>() == sizeof(int) ? ValueType.Int32 : ValueType.Int64;
            if (typeof(T) == typeof(nuint)) return Unsafe.SizeOf<nuint>() == sizeof(int) ? ValueType.Int32 : ValueType.Int64;
            if (typeof(T) == typeof(ContainerReference)) return ValueType.Ref;
            return ValueType.Blob;
        }

        private static string ComputeTypeName()
        {
            if (typeof(T).IsEnum)
            {
                return $"enum({Enum.GetUnderlyingType(typeof(T)).Name})";
            }
            return typeof(T).Name;
        }

        private static string ComputeValueTypeName()
        {
            switch (ValueType)
            {
                case ValueType.Unknown:
                    return "(Missing)";
                case ValueType.Ref:
                    return "Object";
                case ValueType.Blob:
                    return "Blob";
                default:
                    return ValueType.ToString();
            }
        }
    }
}
