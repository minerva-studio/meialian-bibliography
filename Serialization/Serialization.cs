using Unity.Serialization.Json;

namespace Amlos.Container.Serialization
{
    public class StorageAdapter : IJsonAdapter<Storage>, IJsonAdapter
    {
        public Storage Deserialize(in JsonDeserializationContext<Storage> context)
        {
            throw new System.NotImplementedException();
        }

        // 1) Entry stays the same
        public void Serialize(in JsonSerializationContext<Storage> context, Storage value)
        {
            Write(context.Writer, value.Root);
        }

        public void Write(JsonWriter writer, StorageObject value)
        {
            if (value.IsNull)
            {
                writer.WriteNull();
                return;
            }

            using var obj = writer.WriteObjectScope();

            // Iterate fields in layout order; if you need name order do it at build time.
            var schema = value.Schema;
            for (int i = 0; i < schema.Fields.Count; i++)
            {
                var field = schema.Fields[i];
                var fieldName = field.Name;

                byte hint = value.HeaderHints[i];
                var vt = TypeHintUtil.ValueType(hint);
                bool isArray = TypeHintUtil.IsArray(hint);

                // Always write the key for now (no omit-default policy here)
                writer.WriteKey(fieldName);

                // Unknown: always emit raw bytes (byte array), regardless of isArray.
                if (vt == ValueType.Unknown)
                {
                    WriteUnknownBytes(writer, value, field);
                    continue;
                }

                // Arrays
                if (isArray)
                {
                    // Char16[] is serialized as a single string for readability.
                    if (vt == ValueType.Char16)
                    {
                        var str = value.ReadString(fieldName);
                        writer.WriteValue(str);
                        continue;
                    }

                    // Ref array: inline children or null
                    if (vt == ValueType.Ref)
                    {
                        WriteRefArray(writer, value, fieldName);
                        continue;
                    }

                    // Primitive arrays (value arrays)
                    WritePrimitiveArrayByType(writer, value, fieldName, vt);
                    continue;
                }

                // Scalars
                switch (vt)
                {
                    case ValueType.Bool: writer.WriteValue(value.Read<bool>(fieldName)); break;
                    case ValueType.Int8: writer.WriteValue(value.Read<sbyte>(fieldName)); break;
                    case ValueType.UInt8: writer.WriteValue(value.Read<byte>(fieldName)); break;
                    case ValueType.Int16: writer.WriteValue(value.Read<short>(fieldName)); break;
                    case ValueType.UInt16: writer.WriteValue(value.Read<ushort>(fieldName)); break;
                    case ValueType.Int32: writer.WriteValue(value.Read<int>(fieldName)); break;
                    case ValueType.UInt32: writer.WriteValue(value.Read<uint>(fieldName)); break;
                    case ValueType.Int64: writer.WriteValue(value.Read<long>(fieldName)); break;
                    case ValueType.UInt64: writer.WriteValue(value.Read<ulong>(fieldName)); break;
                    case ValueType.Float32: writer.WriteValue(value.Read<float>(fieldName)); break;
                    case ValueType.Float64: writer.WriteValue(value.Read<double>(fieldName)); break;

                    case ValueType.Char16: writer.WriteValue(new string(value.Read<char>(fieldName), 1)); break;
                    case ValueType.Ref:
                        {
                            // If GetObject(fieldName) returns a null-wrapper when id==0, Write() will emit null due to IsNull check.
                            // If not, consider reading ulong id first and writing null explicitly when id==0.
                            var objChild = value.GetObject(fieldName);
                            Write(writer, objChild);
                            break;
                        }
                }
            }
        }

        // 2) Arrays

        private void WriteRefArray(JsonWriter writer, StorageObject value, string fieldName)
        {
            using var arr = writer.WriteArrayScope();
            var objArray = value.GetObjectArray(fieldName);
            for (int i = 0; i < objArray.Count; i++)
            {
                var element = objArray[i];
                var obj = element.GetObjectNoAllocate();
                // Write() handles IsNull¡únull already
                Write(writer, obj);
            }
        }

        private void WritePrimitiveArrayByType(JsonWriter writer, StorageObject value, string fieldName, ValueType vt)
        {
            using var arr = writer.WriteArrayScope();
            switch (vt)
            {
                case ValueType.Bool: WritePrimitiveArray<bool>(writer, value, fieldName); break;
                case ValueType.Int8: WritePrimitiveArray<sbyte>(writer, value, fieldName); break;
                case ValueType.UInt8: WritePrimitiveArray<byte>(writer, value, fieldName); break;
                case ValueType.Int16: WritePrimitiveArray<short>(writer, value, fieldName); break;
                case ValueType.UInt16: WritePrimitiveArray<ushort>(writer, value, fieldName); break;
                case ValueType.Int32: WritePrimitiveArray<int>(writer, value, fieldName); break;
                case ValueType.UInt32: WritePrimitiveArray<uint>(writer, value, fieldName); break;
                case ValueType.Int64: WritePrimitiveArray<long>(writer, value, fieldName); break;
                case ValueType.UInt64: WritePrimitiveArray<ulong>(writer, value, fieldName); break;
                case ValueType.Float32: WritePrimitiveArray<float>(writer, value, fieldName); break;
                case ValueType.Float64: WritePrimitiveArray<double>(writer, value, fieldName); break;
                case ValueType.Char16:  /* handled above as string; shouldn't reach here */    break;
                default:
                    // Safety: unknown should not reach here; fall back to bytes if it does.
                    WritePrimitiveArray<byte>(writer, value, fieldName);
                    break;
            }
        }

        private void WritePrimitiveArray<T>(JsonWriter writer, StorageObject value, string fieldName) where T : unmanaged
        {
            // Note: interface-per-element incurs virtual calls; acceptable for now as a minimal change.
            IPrimitiveWriter<T> primitiveWriter = (PrimitiveWriter.Default as IPrimitiveWriter<T>);
            var span = value.GetArray<T>(fieldName).AsSpan();
            for (int i = 0; i < span.Length; i++)
                primitiveWriter.WriteValue(writer, span[i]);
        }

        // 3) Unknown bytes ¡ª always emit as byte array (no base64, per your rule)

        private void WriteUnknownBytes(JsonWriter writer, StorageObject value, FieldDescriptor field)
        {
            using var arr = writer.WriteArrayScope();
            var bytes = value.GetArray<byte>(field.Name).AsSpan();
            for (int i = 0; i < bytes.Length; i++)
                writer.WriteValue(bytes[i]);
        }

        // 4) Primitive writer table (unchanged)
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
}
