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
}
