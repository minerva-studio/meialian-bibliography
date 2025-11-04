using System.Runtime.CompilerServices;

namespace Amlos.Container
{
    public enum PrimType : byte
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
        Ref = 13,  // 8B
                   // 14..31 reserved
    }

    public static class TypeHintUtil
    {
        private const int IS_ARRAY_BIT = 7;         // bit7
        private const int PRIM_SHIFT = 2;         // bits6..2
        private const byte IS_ARRAY_MASK = 1 << IS_ARRAY_BIT;   // 0b1000_0000
        private const byte PRIM_MASK = 0b1_1111;            // 5 bits
        private const byte PRIM_FIELD = (byte)(PRIM_MASK << PRIM_SHIFT);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte Pack(PrimType prim, bool isArray)
            => (byte)((isArray ? IS_ARRAY_MASK : 0) | (((byte)prim & PRIM_MASK) << PRIM_SHIFT));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsArray(byte hint) => (hint & IS_ARRAY_MASK) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PrimType Prim(byte hint)
            => (PrimType)((hint >> PRIM_SHIFT) & PRIM_MASK);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte WithArray(byte hint, bool isArray)
            => isArray ? (byte)(hint | IS_ARRAY_MASK) : (byte)(hint & ~IS_ARRAY_MASK);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte WithPrim(byte hint, PrimType prim)
            => (byte)((hint & ~PRIM_FIELD) | (((byte)prim & PRIM_MASK) << PRIM_SHIFT));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PrimType PrimOf<T>() where T : unmanaged
        {
            if (typeof(T) == typeof(bool)) return PrimType.Bool;
            if (typeof(T) == typeof(sbyte)) return PrimType.Int8;
            if (typeof(T) == typeof(byte)) return PrimType.UInt8;
            if (typeof(T) == typeof(char)) return PrimType.Char16; // UTF-16 code unit
            if (typeof(T) == typeof(short)) return PrimType.Int16;
            if (typeof(T) == typeof(ushort)) return PrimType.UInt16;
            if (typeof(T) == typeof(int)) return PrimType.Int32;
            if (typeof(T) == typeof(uint)) return PrimType.UInt32;
            if (typeof(T) == typeof(long)) return PrimType.Int64;
            if (typeof(T) == typeof(ulong)) return PrimType.UInt64;
            if (typeof(T) == typeof(float)) return PrimType.Float32;
            if (typeof(T) == typeof(double)) return PrimType.Float64;
            return PrimType.Unknown;
        }
    }

}