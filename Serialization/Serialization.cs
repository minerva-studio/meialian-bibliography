using System;
using System.Linq;
using Unity.Serialization.Json;

namespace Amlos.Container.Serialization
{
    public class StorageAdapter : IJsonAdapter<Storage>, IJsonAdapter
    {
        public Storage Deserialize(in JsonDeserializationContext<Storage> context)
        {
            if (context.SerializedValue.IsNull()) return null;

            // Construct storage with current root schema (could be empty)
            var storage = new Storage();
            ReadObject(context.SerializedValue.AsObjectView(), storage.Root, maxDepth: 1000);
            return storage;
        }

        private void ReadObject(SerializedObjectView obj, StorageObject target, int maxDepth)
        {
            if (maxDepth <= 0) throw new InvalidOperationException("Max depth exceeded.");

            ObjectBuilder b = new();
            foreach (var member in obj)
            {
                var name = member.Name().ToString();
                var val = member.Value();

                // 1) If field missing ¡ú infer FieldDescriptor and rebuild schema, then continue.
                if (!target.HasField(name))
                {
                    InferField(name, val, b);
                }
            }
            b.WriteTo(ref target.Buffer);

            // read child objects
            foreach (var member in obj)
            {
                var name = member.Name().ToString();
                var val = member.Value();

                if (val.Type == TokenType.Object)
                {
                    // Single ref: get/create child and recurse
                    var child = target.GetObject(name);
                    ReadObject(val.AsObjectView(), child, maxDepth - 1);
                }
                else if (val.Type == TokenType.Object && target.GetFieldView(name).IsRef)
                {
                    var objArray = target.GetObjectArray(name);
                    for (int i = 0; i < objArray.Length; i++)
                    {
                        var child = objArray[i];
                        ReadObject(val.AsObjectView(), child.Object, maxDepth - 1);
                    }
                }
            }
        }

        private void InferField(string name, SerializedValueView tok, ObjectBuilder b)
        {
            switch (tok.Type)
            {
                case TokenType.Object:
                    b.SetRef(name);
                    return;
                case TokenType.Array:
                    {
                        var arr = tok.AsArrayView();
                        int n = arr.Count();

                        // empty array ¡ú zero-length fixed field
                        if (n == 0)
                        {
                            b.SetArray<byte>(name, 0);
                        }
                        // Find first non-null element
                        SerializedValueView first = default;
                        bool found = false;
                        foreach (var el in arr) { if (!el.IsNull()) { first = el; found = true; break; } }

                        if (!found)
                        {
                            // all nulls ¡ú ref-array of size n
                            b.SetRefArray(name, n);
                            return;
                        }

                        if (first.Type == TokenType.Object)
                        {
                            // ref-array
                            b.SetRefArray(name, n);
                            return;
                        }

                        // value array : must be homogeneous primitive/bool
                        // scan to decide element kind
                        bool anyFloat = false, allBool = true, allNumeric = true;

                        foreach (var el in arr)
                        {
                            if (el.Type != TokenType.Primitive)
                            {
                                allNumeric = false;
                                allBool = false; break;
                            }
                            var pv = el.AsPrimitiveView();
                            if (pv.IsBoolean()) { anyFloat |= false; allNumeric &= false; }
                            else if (pv.IsDecimal()) { anyFloat = true; allBool = false; }
                            else if (pv.IsIntegral()) { allBool = false; }
                            else { allNumeric = false; allBool = false; break; }
                        }

                        if (!allNumeric && !allBool)
                            throw new InvalidOperationException($"Mixed types in array for field '{name}' are not supported.");

                        if (allBool)
                        {
                            ReadArrayContent(arr, n, v => v.AsBoolean());
                            return;
                        }

                        if (anyFloat)
                        {
                            ReadArrayContent(arr, n, v => v.AsDouble());
                            return;
                        }

                        ReadArrayContent(arr, n, v => v.AsInt64());
                        return;
                    }

                case TokenType.String:
                    {
                        var s = tok.AsStringView().ToString();
                        if (s.Length == 1)
                        {
                            // Char16 scalar (2B)
                            b.SetScalar<char>(name, s[0]);
                            return;
                        }

                        // Char16 array
                        b.SetArray<char>(name, s);
                        return;
                    }

                case TokenType.Primitive:
                    {
                        var pv = tok.AsPrimitiveView();
                        if (pv.IsBoolean())
                        {
                            b.SetScalar<bool>(name, pv.AsBoolean());
                            return;
                        }
                        else
                        if (pv.IsDecimal())
                        {
                            b.SetScalar<double>(name, pv.AsDouble());
                            return;
                        }
                        else if (pv.IsIntegral())
                        {
                            b.SetScalar<long>(name, pv.AsInt64());
                            return;
                        }
                        break;
                    }
            }

            // Fallback: unknown/unsupported ¡ú zero-length field (safe no-op) 
            return;

            void ReadArrayContent<T>(SerializedArrayView arr, int count, Func<SerializedValueView, T> getter) where T : unmanaged
            {
                b.SetArray<T>(name, count);
                var buffer = b.GetBuffer<T>(name);
                int i = 0;
                foreach (SerializedValueView el in arr)
                {
                    buffer[i++] = getter(el);
                }
            }
        }





        #region Serialize

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

            if (value.IsString)
            {
                var str = value.ReadString();
                writer.WriteValue(str);
                return;
            }

            using var obj = writer.WriteObjectScope();

            // Iterate fields in layout order; if you need name order do it at build time.
            for (int i = 0; i < value.FieldCount; i++)
            {
                var field = value.GetField(i);
                var fieldName = field.Name.ToString();

                var t = field.FieldHeader.FieldType;
                var vt = t.Type;
                bool isArray = t.IsArray;

                // Always write the key for now (no omit-default policy here)
                writer.WriteKey(fieldName);

                // Unknown: always emit raw bytes (byte array), regardless of isArray.
                if (vt == ValueType.Unknown)
                {
                    WriteUnknownBytes(writer, value, fieldName);
                    continue;
                }

                // Arrays
                if (isArray)
                {
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
            for (int i = 0; i < objArray.Length; i++)
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
                case ValueType.Char16: WritePrimitiveArray<char>(writer, value, fieldName); break;
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
            var array = value.GetArray(fieldName);
            for (int i = 0; i < array.Length; i++)
                primitiveWriter.WriteValue(writer, array[i].Read<T>());
        }

        // 3) Unknown bytes ¡ª always emit as byte array (no base64, per your rule)

        private void WriteUnknownBytes(JsonWriter writer, StorageObject value, string str)
        {
            using var arr = writer.WriteArrayScope();
            var bytes = value.GetFieldView(str).Data;
            for (int i = 0; i < bytes.Length; i++)
                writer.WriteValue(bytes[i]);
        }

        #endregion


    }
}
