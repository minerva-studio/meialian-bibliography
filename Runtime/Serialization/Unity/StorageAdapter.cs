#if UNITY_2021_3_OR_NEWER
using System;
using System.Collections.Generic;
using System.Linq;
using Unity.Serialization.Json;

namespace Minerva.DataStorage.Serialization
{
    /// <summary>
    /// Unity.Serialization JSON adapter for <see cref="Storage"/>.
    ///
    /// This adapter deserializes from Unity's SerializedValueView hierarchy
    /// directly into a Storage tree, using the same mapping semantics as
    /// <see cref="JsonSerialization.Parse(ReadOnlySpan{char}, int)"/>.
    /// </summary>
    public class StorageAdapter : IJsonAdapter<Storage>
    {
        /// <summary>
        /// Deserializes a Storage instance from a SerializedValueView.
        /// Root is required to be a JSON object.
        /// </summary>
        public Storage Deserialize(in JsonDeserializationContext<Storage> context)
        {
            var value = context.SerializedValue;

            if (value.IsNull())
                return null;

            if (value.Type != TokenType.Object)
                throw new InvalidOperationException("Root JSON value must be an object.");

            var storage = new Storage();
            ReadObject(value.AsObjectView(), storage.Root, depth: 1000);
            return storage;
        }

        /// <summary>
        /// Serialization is intentionally not handled through Unity.Json.
        /// Use JsonSerialization.ToJson instead when you need Storage → JSON.
        /// </summary> 
        public void Serialize(in JsonSerializationContext<Storage> context, Storage value)
        {
            if (value == null)
            {
                context.Writer.WriteNull();
                return;
            }
            context.Writer.WriteValueLiteral(value.ToJson().ToString());
        }

        #region Object / value reading

        /// <summary>
        /// Reads all properties of a SerializedObjectView into the target StorageObject.
        /// </summary>
        private static void ReadObject(
            SerializedObjectView obj,
            StorageObject target,
            int depth)
        {
            if (depth <= 0)
                throw new InvalidOperationException("Max depth exceeded while reading object.");

            foreach (var member in obj)
            {
                var nameView = member.Name();
                // Field name itself; this is not the whole JSON string.
                string name = nameView.ToString();

                var val = member.Value();
                ReadValueIntoField(target, name, val, depth - 1);
            }
        }

        /// <summary>
        /// Dispatches a JSON value into the appropriate field on a StorageObject.
        /// </summary>
        private static void ReadValueIntoField(
            StorageObject target,
            string fieldName,
            SerializedValueView value,
            int depth)
        {
            if (fieldName == JsonSerialization.VersionName)
            {
                target.Version = (int)value.AsPrimitiveView().AsInt64();
                return;
            }
            switch (value.Type)
            {
                case TokenType.Object:
                    {
                        var objView = value.AsObjectView();

                        // First, check if this is a blob wrapper: { "$blob": "..." }.
                        if (TryReadBlob(objView, out var blobBytes))
                        {
                            target.Override(fieldName, blobBytes, ValueType.Blob);
                            return;
                        }

                        // Regular nested object.
                        var child = target.GetObject(fieldName);
                        ReadObject(objView, child, depth);
                        return;
                    }

                case TokenType.Array:
                    {
                        var arrayObject = target.GetObject(fieldName);
                        ReadArrayOn(arrayObject, fieldName, value.AsArrayView(), depth);
                        return;
                    }

                case TokenType.String:
                    {
                        string s = value.AsStringView().ToString();
                        if (s.Length == 1)
                        {
                            // Char16 scalar.
                            target.Write(fieldName, s[0]);
                        }
                        else
                        {
                            // UTF-16 string.
                            target.WriteString(fieldName, s);
                        }
                        return;
                    }

                case TokenType.Primitive:
                    {
                        var prim = value.AsPrimitiveView();

                        if (prim.IsNull())
                        {
                            // Null → interpreted as missing field; do not write anything.
                            return;
                        }

                        if (prim.IsBoolean())
                        {
                            bool b = prim.AsBoolean();
                            target.Write(fieldName, b);
                            return;
                        }

                        if (prim.IsIntegral())
                        {
                            long l = prim.AsInt64();
                            target.Write(fieldName, l);
                            return;
                        }

                        if (prim.IsDecimal())
                        {
                            double d = prim.AsDouble();
                            target.Write(fieldName, d);
                            return;
                        }

                        // Fallback: treat as double.
                        target.Write(fieldName, prim.AsDouble());
                        return;
                    }

                //case TokenType.Null:
                //    // Explicit null: treated as "no field".
                //    return;

                default:
                    throw new InvalidOperationException(
                        $"Unsupported JSON token type {value.Type} at field '{fieldName}'.");
            }
        }

        #endregion

        #region Blob wrapper

        /// <summary>
        /// Checks whether the given object view matches the blob wrapper pattern
        /// { "$blob": "<base64>" } and, if so, decodes its content.
        /// </summary>
        private static bool TryReadBlob(SerializedObjectView obj, out byte[] bytes)
        {
            bytes = null;

            int count = 0;
            SerializedStringView keyView = default;
            SerializedValueView valueView = default;

            foreach (var member in obj)
            {
                count++;
                if (count > 1)
                    break;

                keyView = member.Name();
                valueView = member.Value();
            }

            if (count != 1)
                return false;

            // Compare key to JsonSerialization.BlobName ("$blob").
            if (!SerializedStringEquals(keyView, JsonSerialization.BlobName))
                return false;

            if (valueView.Type != TokenType.String)
                throw new InvalidOperationException(
                    $"Blob wrapper '{JsonSerialization.BlobName}' must contain a string.");

            string base64 = valueView.AsStringView().ToString();
            try
            {
                bytes = Convert.FromBase64String(base64);
                return true;
            }
            catch (FormatException e)
            {
                throw new InvalidOperationException("Invalid base64 inside blob wrapper.", e);
            }
        }

        /// <summary>
        /// Compares a SerializedStringView to a managed string.
        /// This is only used for small keys (e.g., "$blob"), so a ToString() is acceptable.
        /// </summary>
        private static bool SerializedStringEquals(SerializedStringView view, string s)
        {
            return view.ToString() == s;
        }

        #endregion

        #region Arrays

        /// <summary>
        /// Reads a JSON array value into the given field on a StorageObject.
        /// The Storage mapping follows JsonToStorageReader.ReadArrayOn.
        /// </summary>
        private static void ReadArrayOn(
            StorageObject target,
            string fieldName,
            SerializedArrayView arrayView,
            int depth)
        {
            if (depth <= 0)
                throw new InvalidOperationException("Max depth exceeded while reading array.");

            // Empty array → encode as an empty byte[] (or other convention as needed).
            if (arrayView.Count() == 0)
            {
                target.WriteArray<byte>(Array.Empty<byte>());
                return;
            }

            ValueType arrayType = 0;
            var scalarValues = new List<ElementValue>();
            var containers = new List<Container>();
            var blobs = new List<byte[]>();

            try
            {
                foreach (var item in arrayView)
                {
                    switch (item.Type)
                    {
                        case TokenType.Primitive:
                            {
                                var prim = item.AsPrimitiveView();

                                if (prim.IsNull())
                                {
                                    // Null inside arrays is not supported in the simple mapping.
                                    SetArrayType(ref arrayType, ValueType.Ref, fieldName);
                                    containers.Add(null);
                                    // We do not store a concrete value; element type is Ref.
                                }
                                else if (prim.IsBoolean())
                                {
                                    SetArrayType(ref arrayType, ValueType.Bool, fieldName);
                                    scalarValues.Add(prim.AsBoolean());
                                }
                                else if (prim.IsIntegral())
                                {
                                    SetArrayType(ref arrayType, ValueType.Int64, fieldName);
                                    scalarValues.Add(prim.AsInt64());
                                }
                                else
                                {
                                    SetArrayType(ref arrayType, ValueType.Float64, fieldName);
                                    scalarValues.Add(prim.AsDouble());
                                }

                                break;
                            }

                        case TokenType.String:
                            {
                                SetArrayType(ref arrayType, ValueType.Ref, fieldName);

                                // In the text parser, strings in arrays are wrapped into
                                // a wild container that holds a single string field.
                                var wild = Container.Registry.Shared.CreateWild(ContainerLayout.Empty, fieldName);
                                var child = new StorageObject(wild);
                                string s = item.AsStringView().ToString();
                                child.WriteString(s);
                                containers.Add(wild);
                                break;
                            }

                        case TokenType.Object:
                            {
                                var objView = item.AsObjectView();

                                if (TryReadBlob(objView, out var blobBytes))
                                {
                                    SetArrayType(ref arrayType, ValueType.Blob, fieldName);
                                    blobs.Add(blobBytes);
                                }
                                else
                                {
                                    SetArrayType(ref arrayType, ValueType.Ref, fieldName);
                                    var wild = Container.Registry.Shared.CreateWild(ContainerLayout.Empty, fieldName);
                                    var child = new StorageObject(wild);
                                    ReadObject(objView, child, depth - 1);
                                    containers.Add(wild);
                                }

                                break;
                            }

                        case TokenType.Array:
                            {
                                SetArrayType(ref arrayType, ValueType.Ref, fieldName);
                                var wild = Container.Registry.Shared.CreateWild(ContainerLayout.Empty, fieldName);
                                var child = new StorageObject(wild);
                                ReadNestedArray(array: item.AsArrayView(), arrayObject: child, depth: depth - 1);
                                containers.Add(wild);
                                break;
                            }

                        default:
                            {
                                SetArrayType(ref arrayType, ValueType.Ref, fieldName);
                                break;
                            }
                    }
                }

                // Materialize into Storage arrays based on the resolved arrayType.
                if (arrayType == ValueType.Bool ||
                    arrayType == ValueType.Int64 ||
                    arrayType == ValueType.Float64)
                {
                    target.MakeArray((TypeData)arrayType, scalarValues.Count);
                    var array = target.AsArray();

                    for (int i = 0; i < scalarValues.Count; i++)
                    {
                        switch (arrayType)
                        {
                            case ValueType.Bool:
                                array.Raw[i].Write(scalarValues[i].BoolValue);
                                break;

                            case ValueType.Int64:
                                array.Raw[i].Write(scalarValues[i].IntValue);
                                break;

                            case ValueType.Float64:
                                array.Raw[i].Write(scalarValues[i].FloatValue);
                                break;
                        }
                    }

                    return;
                }

                if (arrayType == ValueType.Blob)
                {
                    if (blobs.Count == 0)
                    {
                        target.WriteArray<byte>(Array.Empty<byte>());
                        return;
                    }

                    target.MakeArray(TypeData.Blob(blobs[0].Length), blobs.Count);
                    var array = target.AsArray();
                    for (int i = 0; i < blobs.Count; i++)
                    {
                        blobs[i].CopyTo(array.Raw[i].Bytes);
                    }

                    return;
                }

                if (arrayType == ValueType.Ref)
                {
                    target.MakeArray(TypeData.Ref, containers.Count);
                    var array = target.AsArray();
                    for (int i = 0; i < containers.Count; i++)
                    {
                        Container container = containers[i];
                        if (container == null)
                        {
                            array.References[i] = Container.Registry.ID.Empty;
                        }
                        else
                        {
                            Container.Registry.Shared.Register(container, target.Container);
                            array.References[i] = containers[i].ID;
                        }
                    }

                    return;
                }

                // Fallback: unsupported types → encode as empty array.
                target.WriteArray<byte>(Array.Empty<byte>());
            }
            catch
            {
                // Cleanup any temporary wild containers on failure.
                foreach (var container in containers)
                {
                    if (container == null) continue;
                    if (container.ID == Container.Registry.ID.Wild)
                        Container.Registry.Shared.Return(container);
                    else
                        Container.Registry.Shared.Unregister(container);
                }

                throw;
            }
        }

        /// <summary>
        /// Helper for nested arrays inside an array element.
        /// This mirrors the "array inside wild container" behavior.
        /// </summary>
        private static void ReadNestedArray(
            SerializedArrayView array,
            StorageObject arrayObject,
            int depth)
        {
            // We simply delegate to the same logic but use a fixed field name
            // for the array contents (e.g., ContainerLayout.ArrayName).
            if (depth <= 0)
                throw new InvalidOperationException("Max depth exceeded while reading nested array.");

            // Assuming you have a well-known array field name in ContainerLayout.
            ReadArrayOn(arrayObject, ContainerLayout.ArrayName.ToString(), array, depth);
        }

        #endregion

        #region Helpers

        /// <summary>
        /// Merges the incoming element type into the current arrayType,
        /// applying promotion rules (e.g., Int64 + Float64 → Float64)
        /// and rejecting incompatible mixes.
        /// </summary>
        private static void SetArrayType(ref ValueType current, ValueType incoming, string fieldName)
        {
            if (current == 0)
            {
                current = incoming;
                return;
            }

            if (current == incoming)
                return;

            // Allow Int64 + Float64 → Float64 promotion.
            if ((current == ValueType.Int64 && incoming == ValueType.Float64) ||
                (current == ValueType.Float64 && incoming == ValueType.Int64))
            {
                current = ValueType.Float64;
                return;
            }

            // Anything else is considered an unsupported mix.
            throw new InvalidOperationException(
                $"Mixed or unsupported element types in array for field '{fieldName}'.");
        }

        #endregion
    }
}
#endif
