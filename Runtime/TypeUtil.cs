using System.Runtime.CompilerServices;

namespace Amlos.Container
{
    public enum ValueType : byte
    {
        Unknown = 0,
        Bool = 1,   // 1B
        Int8 = 2,   // sbyte
        UInt8 = 3,   // byte
        Char16 = 4,   // .NET char (UTF-16 code unit, 2B)
        Int16 = 5,
        UInt16 = 6,
        Int32 = 7,
        UInt32 = 8,
        Int64 = 9,
        UInt64 = 10,
        Float32 = 11,
        Float64 = 12,
        Blob = 13, // byte[] or something large
        Ref = 14,  // 8B
                   // 14..31 reserved
    }

    public static class TypeUtil
    {
        private const int IS_ARRAY_BIT = 7;         // bit7
        private const int PRIM_SHIFT = 2;         // bits6..2
        private const byte IS_ARRAY_MASK = 1 << IS_ARRAY_BIT;   // 0b1000_0000
        private const byte PRIM_MASK = 0b1_1111;            // 5 bits
        private const byte PRIM_FIELD = (byte)(PRIM_MASK << PRIM_SHIFT);


        public const byte Ref = (byte)ValueType.Ref << PRIM_SHIFT;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte Pack(ValueType valueType, bool isArray) => (byte)((isArray ? IS_ARRAY_MASK : 0) | (((byte)valueType & PRIM_MASK) << PRIM_SHIFT));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte SetType(ref byte b, ValueType v) => (byte)((b & IS_ARRAY_MASK) | ((byte)v & PRIM_MASK) << PRIM_SHIFT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte SetArray(ref byte b, bool isArray) => (byte)((isArray ? IS_ARRAY_MASK : 0) | ((byte)b & PRIM_MASK) << PRIM_SHIFT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsArray(byte hint) => (hint & IS_ARRAY_MASK) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ValueType PrimOf(byte hint)
            => (ValueType)((hint >> PRIM_SHIFT) & PRIM_MASK);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ValueType PrimOf<T>() where T : unmanaged
        {
            if (typeof(T) == typeof(bool)) return Amlos.Container.ValueType.Bool;
            if (typeof(T) == typeof(sbyte)) return Amlos.Container.ValueType.Int8;
            if (typeof(T) == typeof(byte)) return Amlos.Container.ValueType.UInt8;
            if (typeof(T) == typeof(char)) return Amlos.Container.ValueType.Char16; // UTF-16 code unit
            if (typeof(T) == typeof(short)) return Amlos.Container.ValueType.Int16;
            if (typeof(T) == typeof(ushort)) return Amlos.Container.ValueType.UInt16;
            if (typeof(T) == typeof(int)) return Amlos.Container.ValueType.Int32;
            if (typeof(T) == typeof(uint)) return Amlos.Container.ValueType.UInt32;
            if (typeof(T) == typeof(long)) return Amlos.Container.ValueType.Int64;
            if (typeof(T) == typeof(ulong)) return Amlos.Container.ValueType.UInt64;
            if (typeof(T) == typeof(float)) return Amlos.Container.ValueType.Float32;
            if (typeof(T) == typeof(double)) return Amlos.Container.ValueType.Float64;
            return Amlos.Container.ValueType.Unknown;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte WithArray(byte hint, bool isArray)
            => isArray ? (byte)(hint | IS_ARRAY_MASK) : (byte)(hint & ~IS_ARRAY_MASK);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte WithPrim(byte hint, ValueType valueType)
            => (byte)((hint & ~PRIM_FIELD) | (((byte)valueType & PRIM_MASK) << PRIM_SHIFT));

        /// <summary>
        /// Map ValueType to element byte size; Unknown returns 1 for raw byte copy fallback.
        /// </summary>
        /// <param name="vt"></param>
        /// <returns></returns>
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
        public static bool IsIntegral(this ValueType vt) => vt == ValueType.UInt8 || vt == ValueType.UInt16 || vt == ValueType.UInt32 || vt == ValueType.UInt64 || vt == ValueType.Int8 || vt == ValueType.Int16 || vt == ValueType.Int32 || vt == ValueType.Int64;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsUnsignedInteger(this ValueType vt) => vt == ValueType.UInt8 || vt == ValueType.UInt16 || vt == ValueType.UInt32 || vt == ValueType.UInt64;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsSignedInteger(this ValueType vt) => vt == ValueType.Int8 || vt == ValueType.Int16 || vt == ValueType.Int32 || vt == ValueType.Int64;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsFloatingPoint(this ValueType vt)
        {
            switch (vt)
            {
                case ValueType.Float32:
                case ValueType.Float64:
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Return true if a value of 'from' can be implicitly converted to 'to'
        /// (a conservative subset matching C# numeric implicit conversions).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsImplicitlyConvertible(ValueType from, ValueType to)
        {
            if (from == to) return true;

            switch (from)
            {
                case ValueType.Int8: // sbyte
                    return to == ValueType.Int16 || to == ValueType.Int32 || to == ValueType.Int64
                        || to == ValueType.Float32 || to == ValueType.Float64;

                case ValueType.UInt8: // byte
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
                    return to == ValueType.Float32 || to == ValueType.Float64;

                case ValueType.UInt64:
                    return to == ValueType.Float32 || to == ValueType.Float64;

                case ValueType.Float32:
                    return to == ValueType.Float64;

                case ValueType.Char16:
                    // char -> numeric implicit conversions in C#: to int/uint/long/ulong/float/double
                    return to == ValueType.Int32 || to == ValueType.UInt32
                        || to == ValueType.Int64 || to == ValueType.UInt64
                        || to == ValueType.Float32 || to == ValueType.Float64;

                case ValueType.Bool:
                    return to == ValueType.Bool;

                default:
                    return false;
            }
        }




        // Replace with your actual source of recommended hint for a new field.
        // For now: Unknown (0) to be safe, or map by newField.IsRef/AbsLength if you maintain that metadata.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte RecommendedHintOf(FieldDescriptor_Old nf)
        {
            // If you already keep a recommended ValueType per field, convert here.
            // As a safe default, say Unknown scalar/array based on nf:
            bool isArray = !nf.IsRef && nf.AbsLength > 0 && nf.AbsLength % 8 != 0 && nf.AbsLength > SizeOf(ValueType.Int64);
            // Better: if you store per-field expected PrimType, use that.
            return Pack(ValueType.Unknown, isArray);
        }

        public static string ToString(byte s)
        {
            var baseType = PrimOf(s).ToString();
            return TypeUtil.IsArray(s) ? baseType + "[]" : baseType;
        }
    }
}
