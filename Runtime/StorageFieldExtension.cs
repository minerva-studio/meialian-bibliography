namespace Amlos.Container
{
    public static class StorageFieldExtension
    {
        /// <summary>
        /// Read bool value from the field
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public static bool GetBool(this StorageField field) => field.Read<bool>();

        public static byte GetByte(this StorageField field) => field.Read<byte>();

        public static sbyte GetSByte(this StorageField field) => field.Read<sbyte>();

        public static short GetInt16(this StorageField field) => field.Read<short>();

        public static ushort GetUInt16(this StorageField field) => field.Read<ushort>();

        public static int GetInt32(this StorageField field) => field.Read<int>();

        public static uint GetUInt32(this StorageField field) => field.Read<uint>();

        public static long GetInt64(this StorageField field) => field.Read<long>();

        public static ulong GetUInt64(this StorageField field) => field.Read<ulong>();

        public static float GetFloat32(this StorageField field) => field.Read<float>();

        public static double GetFloat64(this StorageField field) => field.Read<double>();
    }
}

