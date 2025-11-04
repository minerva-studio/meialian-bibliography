using Unity.Serialization.Json;

namespace Amlos.Container.Serialization
{
    public interface IPrimitiveWriter<T> where T : unmanaged
    {
        void WriteValue(JsonWriter writer, T value);
    }

    public class PrimitiveWriter :
        IPrimitiveWriter<bool>,
        IPrimitiveWriter<byte>,
        IPrimitiveWriter<sbyte>,
        IPrimitiveWriter<short>,
        IPrimitiveWriter<ushort>,
        IPrimitiveWriter<int>,
        IPrimitiveWriter<uint>,
        IPrimitiveWriter<long>,
        IPrimitiveWriter<ulong>,
        IPrimitiveWriter<float>,
        IPrimitiveWriter<double>
    {
        public static readonly PrimitiveWriter Default = new PrimitiveWriter();

        void IPrimitiveWriter<bool>.WriteValue(JsonWriter writer, bool value) => writer.WriteValue(value);
        void IPrimitiveWriter<byte>.WriteValue(JsonWriter writer, byte value) => writer.WriteValue(value);
        void IPrimitiveWriter<sbyte>.WriteValue(JsonWriter writer, sbyte value) => writer.WriteValue(value);
        void IPrimitiveWriter<short>.WriteValue(JsonWriter writer, short value) => writer.WriteValue(value);
        void IPrimitiveWriter<ushort>.WriteValue(JsonWriter writer, ushort value) => writer.WriteValue(value);
        void IPrimitiveWriter<int>.WriteValue(JsonWriter writer, int value) => writer.WriteValue(value);
        void IPrimitiveWriter<uint>.WriteValue(JsonWriter writer, uint value) => writer.WriteValue(value);
        void IPrimitiveWriter<long>.WriteValue(JsonWriter writer, long value) => writer.WriteValue(value);
        void IPrimitiveWriter<ulong>.WriteValue(JsonWriter writer, ulong value) => writer.WriteValue(value);
        void IPrimitiveWriter<float>.WriteValue(JsonWriter writer, float value) => writer.WriteValue(value);
        void IPrimitiveWriter<double>.WriteValue(JsonWriter writer, double value) => writer.WriteValue(value);
    }
}
