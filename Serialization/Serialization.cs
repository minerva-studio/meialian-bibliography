using System;
using System.Collections.Generic;
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

            // rebuild scehema
            List<FieldDescriptor> newFields = new();
            foreach (var member in obj)
            {
                var name = member.Name().ToString();
                var val = member.Value();

                // 1) If field missing ¡ú infer FieldDescriptor and rebuild schema, then continue.
                if (!target.HasField(name))
                {
                    var fd = InferField(name, val);
                    newFields.Add(fd);
                }
            }
            target.Rescheme(SchemaBuilder.FromFields(newFields));


            foreach (var member in obj)
            {
                var name = member.Name().ToString();
                var val = member.Value();

                // 2) Dispatch by token and write.
                switch (val.Type)
                {
                    case TokenType.Object:
                        // Single ref: get/create child and recurse
                        var child = target.GetObject(name);
                        ReadObject(val.AsObjectView(), child, maxDepth - 1);
                        break;

                    case TokenType.Array:
                        ReadArray(val.AsArrayView(), target, name, maxDepth - 1);
                        break;

                    case TokenType.String:
                        // Char16 scalar or Char16[]
                        var s = val.AsStringView().ToString();
                        if (s.Length == 1)
                            target.Write(name, s[0]);
                        else
                            target.WriteString(name, s);
                        break;

                    case TokenType.Primitive:
                        ReadPrimitive(val.AsPrimitiveView(), target, name);
                        break;

                    default:
                        // ignore comments/undefined
                        break;
                }
            }
        }

        private FieldDescriptor InferField(string name, SerializedValueView tok)
        {
            switch (tok.Type)
            {
                case TokenType.Object:
                    // Single ref (8B)
                    return FieldDescriptor.Reference(name);

                case TokenType.Array:
                    {
                        var arr = tok.AsArrayView();
                        int n = arr.Count();

                        // empty array ¡ú zero-length fixed field
                        if (n == 0) return FieldDescriptor.Fixed(name, 0);

                        // Find first non-null element
                        SerializedValueView first = default;
                        bool found = false;
                        foreach (var el in arr) { if (!el.IsNull()) { first = el; found = true; break; } }

                        if (!found)
                        {
                            // all nulls ¡ú ref-array of size n
                            return FieldDescriptor.ReferenceArray(name, n);
                        }

                        if (first.Type == TokenType.Object)
                        {
                            // ref-array
                            return FieldDescriptor.ReferenceArray(name, n);
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
                            return FieldDescriptor.Fixed(name, sizeof(bool) * n);

                        if (anyFloat)
                            return FieldDescriptor.Fixed(name, sizeof(double) * n); // conservative Float64

                        // integers only
                        return FieldDescriptor.Fixed(name, sizeof(long) * n);       // conservative Int64
                    }

                case TokenType.String:
                    {
                        var s = tok.AsStringView().ToString();
                        if (s.Length == 1)
                            return FieldDescriptor.Type<char>(name);        // Char16 scalar (2B)
                        return FieldDescriptor.Fixed(name, sizeof(char) * s.Length); // Char16 array
                    }

                case TokenType.Primitive:
                    {
                        var pv = tok.AsPrimitiveView();
                        if (pv.IsBoolean()) return FieldDescriptor.Type<bool>(name);
                        if (pv.IsDecimal()) return FieldDescriptor.Type<double>(name);
                        if (pv.IsIntegral()) return FieldDescriptor.Type<long>(name);
                        break;
                    }
            }

            // Fallback: unknown/unsupported ¡ú zero-length field (safe no-op)
            return FieldDescriptor.Fixed(name, 0);
        }


        private void ReadArray(SerializedArrayView arr, StorageObject target, string name, int maxDepth)
        {
            // First non-null element decides ref-array vs value-array (sameÂß¼­ as above).
            SerializedValueView first = default;
            bool found = false;
            foreach (var el in arr) { if (!el.IsNull()) { first = el; found = true; break; } }

            if (!found)
            {
                // all nulls ¡ú ref-array clear
                var objArr = target.GetObjectArray(name);
                for (int i = 0; i < objArr.Count; i++) objArr.ClearAt(i);
                return;
            }

            if (first.Type == TokenType.Object)
            {
                // ref-array path
                var objArr = target.GetObjectArray(name);
                int i = 0;
                foreach (var el in arr)
                {
                    if (i >= objArr.Count) break;
                    if (el.IsNull()) objArr.ClearAt(i);
                    else ReadObject(el.AsObjectView(), objArr[i], maxDepth - 1);
                    i++;
                }
                return;
            }

            // value-array path: write into fixed T[], truncate/zero-fill 
            bool anyFloat = false, allBool = true;
            foreach (var el in arr)
            {
                if (el.Type != TokenType.Primitive) { allBool = false; break; }
                var pv = el.AsPrimitiveView();
                if (pv.IsDecimal()) { anyFloat = true; allBool = false; }
                else if (!pv.IsBoolean() && !pv.IsIntegral()) { allBool = false; }
                else if (!pv.IsBoolean()) allBool = false;
            }

            if (allBool)
            {
                var dst = target.GetArray<bool>(name).AsSpan();
                int i = 0;
                foreach (var el in arr) { if (i >= dst.Length) break; dst[i++] = el.AsPrimitiveView().AsBoolean(); }
                if (i < dst.Length) dst.Slice(i).Clear();
                return;
            }

            if (anyFloat)
            {
                var dst = target.GetArray<double>(name).AsSpan();
                int i = 0;
                foreach (var el in arr) { if (i >= dst.Length) break; dst[i++] = el.AsPrimitiveView().AsDouble(); }
                if (i < dst.Length) dst.Slice(i).Clear();
                return;
            }

            // integers
            {
                var dst = target.GetArray<long>(name).AsSpan();
                int i = 0;
                foreach (var el in arr) { if (i >= dst.Length) break; dst[i++] = el.AsPrimitiveView().AsInt64(); }
                if (i < dst.Length) dst.Slice(i).Clear();
                return;
            }
        }

        private void ReadPrimitive(SerializedPrimitiveView prim, StorageObject target, string name)
        {
            if (prim.IsBoolean()) { target.Write(name, prim.AsBoolean()); return; }
            if (prim.IsDecimal()) { target.Write(name, prim.AsDouble()); return; }
            if (prim.IsIntegral()) { target.Write(name, prim.AsInt64()); return; }
        }


        //public void Read(SerializedValueView view, StorageObject storageObject)
        //{
        //    switch (view.Type)
        //    {
        //        case TokenType.Object:
        //            var objectView = view.AsObjectView();
        //            ReadObject(storageObject, objectView);
        //            break;
        //        case TokenType.Array:
        //            break;
        //        case TokenType.String:
        //            break;
        //        case TokenType.Primitive:
        //            break;
        //        case TokenType.Comment:
        //        default:
        //        case TokenType.Undefined:
        //            break;
        //    }
        //}

        //private void ReadObject(StorageObject storageObject, SerializedObjectView objectView)
        //{
        //    foreach (var item in objectView)
        //    {
        //        var name = item.Name().ToString();
        //        SerializedValueView valueView = item.Value();
        //        switch (valueView.Type)
        //        {
        //            case TokenType.Object:
        //                ReadObject(storageObject.GetObject(name), valueView.AsObjectView());
        //                break;
        //            case TokenType.Array:
        //                ReadArray(storageObject, name, valueView.AsArrayView());
        //                break;
        //            // reading char array
        //            case TokenType.String:
        //                var str = valueView.AsStringView().ToString();
        //                storageObject.WriteString(name, str);
        //                break;
        //            case TokenType.Primitive:
        //                ReadPrimitive(storageObject, name, valueView.AsPrimitiveView());
        //                break;
        //            case TokenType.Comment:
        //            default:
        //            case TokenType.Undefined:
        //                break;
        //        }
        //    }
        //}

        //private void ReadArray(StorageObject storageObject, string name, SerializedArrayView serializedArrayView)
        //{
        //    var collection = serializedArrayView.ToArray();
        //    // determine array type and read accordingly
        //}

        //private void ReadPrimitive(StorageObject storageObject, string name, SerializedPrimitiveView serializedPrimitiveView)
        //{
        //    if (serializedPrimitiveView.IsBoolean())
        //    {
        //        storageObject.Write(name, serializedPrimitiveView.AsBoolean());
        //        return;
        //    }
        //    //... Other primitive types to be handled here.

        //}


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

        #endregion


    }
}
