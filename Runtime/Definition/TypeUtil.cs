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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte Pack(ValueType valueType, bool isArray) => (byte)(((byte)valueType) | (isArray ? IS_ARRAY_MASK : (byte)0));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte SetType(ref byte b, ValueType v)
        {
            // Keep array flag (bit7) and reserved bits (5..6); replace low-5 prim bits.
            b = (byte)((b & IS_ARRAY_MASK) | ((byte)v & PRIM_MASK));
            return b;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte SetArray(ref byte b, bool isArray)
        {
            b = isArray ? (byte)(b | IS_ARRAY_MASK) : (byte)(b & ~IS_ARRAY_MASK);
            return b;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsArray(byte hint) => (hint & IS_ARRAY_MASK) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ValueType PrimOf(byte hint)
            => (ValueType)(hint & PRIM_MASK); // No shift

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ValueType PrimOf<T>() where T : unmanaged => PrimCache<T>.Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static FieldType FieldType<T>(bool isArray) where T : unmanaged
            => Pack(PrimOf<T>(), isArray);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static FieldType FieldType<T>() where T : unmanaged => PrimOf<T>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte WithArray(byte hint, bool isArray)
            => isArray ? (byte)(hint | IS_ARRAY_MASK) : (byte)(hint & ~IS_ARRAY_MASK);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte WithPrim(byte hint, ValueType valueType)
            => (byte)((hint & ~PRIM_MASK) | ((byte)valueType & PRIM_MASK));

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
        public static bool IsUnsignedInteger(this ValueType vt)
            => vt == ValueType.UInt8 || vt == ValueType.UInt16 || vt == ValueType.UInt32 || vt == ValueType.UInt64;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsSignedInteger(this ValueType vt)
            => vt == ValueType.Int8 || vt == ValueType.Int16 || vt == ValueType.Int32 || vt == ValueType.Int64;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsFloatingPoint(this ValueType vt)
            => vt == ValueType.Float32 || vt == ValueType.Float64;

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
            var baseType = PrimOf(s).ToString();
            return IsArray(s) ? baseType + "[]" : baseType;
        }

        private static class PrimCache<T> where T : unmanaged
        {
            internal static readonly ValueType Value = Compute();
            private static ValueType Compute()
            {
                if (typeof(T).IsEnum)
                {
                    var ut = Enum.GetUnderlyingType(typeof(T));
                    switch (Type.GetTypeCode(ut))
                    {
                        case TypeCode.SByte: return ValueType.Int8;
                        case TypeCode.Byte: return ValueType.UInt8;
                        case TypeCode.Int16: return ValueType.Int16;
                        case TypeCode.UInt16: return ValueType.UInt16;
                        case TypeCode.Int32: return ValueType.Int32;
                        case TypeCode.UInt32: return ValueType.UInt32;
                        case TypeCode.Int64: return ValueType.Int64;
                        case TypeCode.UInt64: return ValueType.UInt64;
                        default: return ValueType.Unknown;
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
                return ValueType.Blob;
            }
        }
    }
}
