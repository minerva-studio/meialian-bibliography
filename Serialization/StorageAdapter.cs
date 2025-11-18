using System;
using System.Linq;
using Unity.Serialization.Json;

namespace Minerva.DataStorage.Serialization
{
    public class StorageAdapter : IJsonAdapter<Storage>, IJsonAdapter
    {
        public void Serialize(in JsonSerializationContext<Storage> context, Storage value)
        {
            context.Writer.WriteValueLiteral(value.ToJson().ToString());
        }

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

                // 1) If field missing infer FieldDescriptor and rebuild schema, then continue.
                if (!target.HasField(name))
                {
                    InferField(name, val, b);
                }
            }
            b.WriteTo(ref target.Memory);

            // read child objects
            foreach (var member in obj)
            {
                var val = member.Value();
                var name = member.Name().ToString();

                if (val.Type == TokenType.Object)
                {
                    // Single ref: get/create child and recurse
                    var child = target.GetObject(name);
                    ReadObject(val.AsObjectView(), child, maxDepth - 1);
                }
                else if (val.Type == TokenType.Array)
                {
                    var objArray = target.GetArray(name);
                    if (objArray.Type != ValueType.Ref) continue;
                    var arr = val.AsArrayView();
                    var i = 0;
                    foreach (var item in arr)
                    {
                        var child = objArray.GetObject(i);
                        if (item.IsNull()) objArray.ClearAt(i);
                        else ReadObject(item.AsObjectView(), child, maxDepth - 1);
                        i++;
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

                        // empty array zero-length fixed field
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
                            // all nulls  ref-array of size n
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

            // Fallback: unknown/unsupported  zero-length field (safe no-op) 
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
    }
}
