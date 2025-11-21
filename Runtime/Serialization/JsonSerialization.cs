using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Minerva.DataStorage.Serialization
{
    public static class JsonSerialization
    {
        public const string BlobName = "$blob";

        public static ReadOnlySpan<char> ToJson(this Storage storage)
        {
            var writer = new ArrayBufferWriter<char>();
            storage.Root.WriteJsonTo(writer);
            return writer.WrittenSpan;
        }

        private static void WriteJsonTo(this in StorageObject storage, IBufferWriter<char> writer)
        {
            // --- special case: Object Array --------------------------
            if (storage.IsArray())
            {
                ref FieldHeader field = ref storage._container.GetFieldHeader(0);
                // --- special case: UTF-16 string (Char16) --------------------------
                if (storage.IsString)
                {
                    WriteJsonString(ref writer, storage._container.GetFieldData<char>(in field));
                }
                else if (field.IsRef)
                {
                    var ids = storage._container.GetFieldData<ContainerReference>(in field);
                    WriteObject(in field, ids, writer);
                }
                else
                {
                    var span = storage._container.GetFieldData(in field);
                    WriteScalarArray(in field, span, writer);
                }
                return;
            }

            writer.Write("{");
            int fieldCount = storage.FieldCount;
            for (int i = 0; i < fieldCount; i++)
            {
                ref var field = ref storage._container.GetFieldHeader(i);
                WriteField(in storage, field, writer);
                if (i < fieldCount - 1) writer.Write(",");
            }
            writer.Write("}");
        }

        private static void WriteField(in StorageObject storage, in FieldHeader field, IBufferWriter<char> writer)
        {
            // --- field name -----------------------------------------------------
            ReadOnlySpan<char> name = storage._container.GetFieldName(field);
            writer.Write("\"");
            writer.Write(name);
            writer.Write("\":");

            // --- special case: UTF-16 string (Char16) --------------------------
            if (field.Type == ValueType.Char16)
            {
                var span = storage._container.GetFieldData(in field);
                WriteJsonString(ref writer, MemoryMarshal.Cast<byte, char>(span));
                return;
            }

            // --- special case: Blob -> base64 string ---------------------------
            if (field.Type == ValueType.Blob)
            {
                var span = storage._container.GetFieldData(in field);
                WriteBlob(span, writer); return;
            }

            // --- case: ref ------------------------------------------------------
            if (field.IsRef)
            {
                var ids = storage._container.GetFieldData<ContainerReference>(in field);
                WriteObject(in field, ids, writer);
                return;
            }

            // --- case: inline primitive array ----------------------------------
            if (field.IsInlineArray)
            {
                var span = storage._container.GetFieldData(in field);
                WriteScalarArray(in field, span, writer);
                return;
            }

            // --- case: single primitive value ----------------------------------
            {
                var span = storage._container.GetFieldData(in field);
                WriteScalar(field.Type, span, writer);
            }
        }

        private static void WriteObject(in FieldHeader field, Span<ContainerReference> ids, IBufferWriter<char> writer)
        {
            int length = ids.Length;
            if (field.IsInlineArray)
                writer.Write("[");

            for (int i = 0; i < length; i++)
            {
                if (i > 0) writer.Write(",");
                var obj = ids[i].GetNoAllocate();

                if (obj.IsNull) writer.Write("null");
                else obj.WriteJsonTo(writer);

            }

            if (field.IsInlineArray)
                writer.Write("]");
        }

        private static void WriteScalarArray(in FieldHeader field, Span<byte> span, IBufferWriter<char> writer)
        {
            writer.Write("[");

            if (span.Length > 0)
            {
                int elemSize = field.ElemSize;
                if (elemSize <= 0 || span.Length % elemSize != 0)
                    throw new InvalidOperationException(
                        $"Invalid element size for {field.Type}, span length = {span.Length}, elemSize = {elemSize}");

                int count = span.Length / elemSize;
                for (int i = 0; i < count; i++)
                {
                    if (i > 0) writer.Write(",");
                    var elemSpan = span.Slice(i * elemSize, elemSize);
                    WriteScalar(field.Type, elemSpan, writer);
                }
            }

            writer.Write("]");
        }

        private static void WriteScalar(ValueType type, ReadOnlySpan<byte> span, IBufferWriter<char> writer)
        {
            switch (type)
            {
                case ValueType.Bool:
                    writer.Write(span[0] == 0 ? "false" : "true");
                    break;

                case ValueType.Int8:
                    writer.Write(((sbyte)span[0]).ToString(CultureInfo.InvariantCulture));
                    break;

                case ValueType.UInt8:
                    writer.Write(span[0].ToString(CultureInfo.InvariantCulture));
                    break;

                case ValueType.Int16:
                    {
                        short v = BinaryPrimitives.ReadInt16LittleEndian(span);
                        writer.Write(v.ToString(CultureInfo.InvariantCulture));
                        break;
                    }
                case ValueType.UInt16:
                    {
                        ushort v = BinaryPrimitives.ReadUInt16LittleEndian(span);
                        writer.Write(v.ToString(CultureInfo.InvariantCulture));
                        break;
                    }

                case ValueType.Int32:
                    {
                        int v = BinaryPrimitives.ReadInt32LittleEndian(span);
                        writer.Write(v.ToString(CultureInfo.InvariantCulture));
                        break;
                    }
                case ValueType.UInt32:
                    {
                        uint v = BinaryPrimitives.ReadUInt32LittleEndian(span);
                        writer.Write(v.ToString(CultureInfo.InvariantCulture));
                        break;
                    }

                case ValueType.Int64:
                    {
                        long v = BinaryPrimitives.ReadInt64LittleEndian(span);
                        writer.Write(v.ToString(CultureInfo.InvariantCulture));
                        break;
                    }
                case ValueType.UInt64:
                    {
                        ulong v = BinaryPrimitives.ReadUInt64LittleEndian(span);
                        writer.Write(v.ToString(CultureInfo.InvariantCulture));
                        break;
                    }

                case ValueType.Float32:
                    {
                        float v = MemoryMarshal.Read<float>(span);
                        writer.Write(v.ToString("R", CultureInfo.InvariantCulture));
                        break;
                    }
                case ValueType.Float64:
                    {
                        double v = MemoryMarshal.Read<double>(span);
                        writer.Write(v.ToString("R", CultureInfo.InvariantCulture));
                        break;
                    }
                case ValueType.Blob:
                    {
                        WriteBlob(span, writer);
                        break;
                    }
                case ValueType.Char16:
                    writer.Write("\"\"");
                    break;

                case ValueType.Unknown:
                default:
                    writer.Write("null");
                    break;
            }
        }

        private static void WriteBlob(ReadOnlySpan<byte> span, IBufferWriter<char> writer)
        {
            string base64 = Convert.ToBase64String(span.ToArray());
            writer.Write("{");
            writer.Write("\"");
            writer.Write(BlobName);
            writer.Write("\"");
            writer.Write(":\"");
            writer.Write(base64);
            writer.Write("\"}");
        }

        // Escapes a UTF-16 string as a JSON string literal: " ... "
        // Handles: quotes, backslashes, control chars, and common escapes.
        private static void WriteJsonString(ref IBufferWriter<char> writer, ReadOnlySpan<char> value)
        {
            // Opening quote
            writer.Write("\"");

            for (int i = 0; i < value.Length; i++)
            {
                char c = value[i];
                switch (c)
                {
                    case '\"':
                        writer.Write("\\\"");  // quote
                        break;
                    case '\\':
                        writer.Write("\\\\");  // backslash
                        break;
                    case '\b':
                        writer.Write("\\b");
                        break;
                    case '\f':
                        writer.Write("\\f");
                        break;
                    case '\n':
                        writer.Write("\\n");
                        break;
                    case '\r':
                        writer.Write("\\r");
                        break;
                    case '\t':
                        writer.Write("\\t");
                        break;
                    default:
                        if (c < ' ')
                        {
                            // Control chars 0x00¨C0x1F must be escaped as \u00XX
                            Span<char> buf = stackalloc char[6];
                            buf[0] = '\\';
                            buf[1] = 'u';
                            buf[2] = ToHexChar((c >> 12) & 0xF);
                            buf[3] = ToHexChar((c >> 8) & 0xF);
                            buf[4] = ToHexChar((c >> 4) & 0xF);
                            buf[5] = ToHexChar(c & 0xF);
                            writer.Write(buf);
                        }
                        else
                        {
                            // Normal printable char, write as-is
                            writer.GetSpan(1)[0] = c;
                            writer.Advance(1);
                        }
                        break;
                }
            }

            // Closing quote
            writer.Write("\"");
        }

        private static char ToHexChar(int value)
        {
            return (char)(value < 10 ? ('0' + value) : ('A' + (value - 10)));
        }






        /// <summary>
        /// Deserializes JSON text directly into a <see cref="Storage"/> tree
        /// using a hand-written state machine, without building an intermediate
        /// JSON DOM. Storage itself is treated as the in-memory DOM.
        ///
        /// Mapping (suggested contract):
        ///  - Root JSON object        -> storage.Root
        ///  - JSON property "x":
        ///      * object    -> child object: target.GetObject("x") then recurse
        ///      * array     -> child "array container" object with a single inline array
        ///      * string    -> char / char[] (length == 1 => scalar char, otherwise UTF-16 array)
        ///      * true/false-> bool scalar
        ///      * number    -> long (if it fits Int64) or double otherwise
        ///      * null      -> field omitted (no write)
        ///
        ///  For arrays:
        ///   - Pure bool array    -> child array container with bool[]
        ///   - Pure integer array -> long[]
        ///   - Any float present  -> double[]
        ///
        ///  Mixed-type arrays (e.g., numbers + strings) are rejected with an exception.
        /// </summary> 
        public static Storage Parse(string json, int maxDepth = 1000) => Parse(json.AsSpan(), maxDepth);

        /// <summary>
        /// Deserializes JSON text directly into a <see cref="Storage"/> tree
        /// using a hand-written state machine, without building an intermediate
        /// JSON DOM. Storage itself is treated as the in-memory DOM.
        ///
        /// Mapping (suggested contract):
        ///  - Root JSON object        -> storage.Root
        ///  - JSON property "x":
        ///      * object    -> child object: target.GetObject("x") then recurse
        ///      * array     -> child "array container" object with a single inline array
        ///      * string    -> char / char[] (length == 1 => scalar char, otherwise UTF-16 array)
        ///      * true/false-> bool scalar
        ///      * number    -> long (if it fits Int64) or double otherwise
        ///      * null      -> field omitted (no write)
        ///
        ///  For arrays:
        ///   - Pure bool array    -> child array container with bool[]
        ///   - Pure integer array -> long[]
        ///   - Any float present  -> double[]
        ///
        ///  Mixed-type arrays (e.g., numbers + strings) are rejected with an exception.
        /// </summary> 
        public static Storage Parse(ReadOnlySpan<char> text, int maxDepth = 1000)
        {
            var storage = new Storage();
            var reader = new JsonToStorageReader(text, maxDepth);
            reader.ParseInto(storage.Root);
            return storage;
        }

        /// <summary>
        /// Hand-written JSON reader that streams values directly into a StorageObject.
        /// </summary>
        internal ref struct JsonToStorageReader
        {
            private readonly ReadOnlySpan<char> _text;
            private readonly int _maxDepth;
            private int _pos;

            public JsonToStorageReader(ReadOnlySpan<char> text, int maxDepth)
            {
                _text = text;
                _maxDepth = maxDepth;
                _pos = 0;
            }

            /// <summary>
            /// Entry point: read the root object into the given StorageObject.
            /// </summary>
            public void ParseInto(StorageObject root)
            {
                SkipWhitespace();
                if (Peek() != '{')
                    throw new InvalidOperationException("Root JSON value must be an object.");

                ReadObject(root, _maxDepth);

                SkipWhitespace();
                if (_pos != _text.Length)
                    throw new InvalidOperationException("Extra characters after root JSON object.");
            }

            #region Core object / value reading

            private void ReadObject(StorageObject target, int depth)
            {
                if (depth <= 0)
                    throw new InvalidOperationException("Max depth exceeded while parsing JSON.");

                Expect('{');
                SkipWhitespace();

                // Empty object
                if (Peek() == '}')
                {
                    _pos++;
                    return;
                }

                while (true)
                {
                    SkipWhitespace();
                    if (Peek() != '"')
                        throw new InvalidOperationException("Expected string property name in JSON object.");

                    string name = ReadString();
                    SkipWhitespace();
                    Expect(':');
                    SkipWhitespace();

                    ReadValueIntoField(target, name, depth);

                    SkipWhitespace();
                    char c = Peek();
                    if (c == ',')
                    {
                        _pos++;
                        continue;
                    }
                    if (c == '}')
                    {
                        _pos++;
                        break;
                    }

                    throw new InvalidOperationException($"Expected ',' or '}}' in object, found '{c}'.");
                }
            }

            /// <summary>
            /// Parse a JSON value and write it into the given field on the target object.
            /// </summary>
            private void ReadValueIntoField(StorageObject target, string name, int depth)
            {
                char c = Peek();
                switch (c)
                {
                    case '{':
                        {
                            if (TryReadBlob(out var b64, name))
                            {
                                var bytes = Convert.FromBase64CharArray(b64.ToArray(), 0, b64.Length);
                                target.Override(name, bytes, ValueType.Blob);
                            }
                            else
                            {
                                var child = target.GetObject(name);
                                ReadObject(child, depth - 1);
                            }
                            return;
                        }
                    case '[':
                        {
                            ReadArrayIntoField(target, name, depth - 1);
                            return;
                        }
                    case '"':
                        {
                            string s = ReadString();
                            if (s.Length == 1)
                            {
                                // Char16 scalar
                                target.Write(name, s[0]);
                            }
                            else
                            {
                                // UTF-16 string (char array)
                                // This uses your existing Write(string fieldName, string value) overload,
                                // which internally calls WriteString and WriteArray on a child object.
                                target.Write(name, s);
                            }
                            return;
                        }
                    case 't':
                    case 'f':
                        {
                            bool b = ReadBoolean();
                            target.Write(name, b);
                            return;
                        }
                    case 'n':
                        {
                            ReadNull();
                            // Represent null as "missing field" => do nothing.
                            return;
                        }
                    default:
                        {
                            if (c == '-' || c >= '0' && c <= '9')
                            {
                                if (ReadNumber(out long l, out double d))
                                    target.Write(name, l);
                                else
                                    target.Write(name, d);
                                return;
                            }

                            throw new InvalidOperationException($"Unexpected character '{c}' while reading value for field '{name}'.");
                        }
                }
            }

            /// <summary>
            /// Parse a JSON array and store it as a "child array container" under the given field.
            /// The child container will represent an inline value array.
            /// </summary>
            private void ReadArrayIntoField(StorageObject target, string name, int depth)
            {
                if (depth <= 0)
                    throw new InvalidOperationException("Max depth exceeded while parsing JSON array.");

                var arrayObject = target.GetObject(name);
                ReadArrayOn(arrayObject, name, depth);
            }

            private void ReadArrayOn(StorageObject arrayObject, string name, int depth)
            {
                Expect('[');
                SkipWhitespace();

                // Empty array -> model as an empty byte array (or empty typed array if you want).
                if (Peek() == ']')
                {
                    _pos++;
                    arrayObject.WriteArray<byte>(Array.Empty<byte>());
                    return;
                }

                ValueType arrayType = 0;
                var scalarValues = new List<ElementValue>();
                var containers = new List<Container>();
                var blobs = new List<byte[]>();
                try
                {
                    while (true)
                    {
                        SkipWhitespace();
                        char c = Peek();

                        if (c == ']')
                        {
                            _pos++;
                            break;
                        }

                        if (c == 't' || c == 'f')
                        {
                            SetValueType(ValueType.Bool);
                            bool v = ReadBoolean();
                            scalarValues.Add(v);
                        }
                        else if (c == '-' || c >= '0' && c <= '9')
                        {
                            if (ReadNumber(out long l, out double d))
                            {
                                SetValueType(ValueType.Int64);
                                scalarValues.Add(l);
                            }
                            else
                            {
                                SetValueType(ValueType.Float64);
                                scalarValues.Add(d);
                            }
                        }
                        else if (c == 'n')
                        {
                            SetValueType(ValueType.Ref);
                            // null inside a value array is not supported in this simple mapping.
                            ReadNull();
                            containers.Add(null);
                        }
                        else if (c == '{')
                        {
                            if (TryReadBlob(out var b64, name))
                            {
                                SetValueType(ValueType.Blob);
                                var bytes = Convert.FromBase64CharArray(b64.ToArray(), 0, b64.Length);
                                blobs.Add(bytes);
                            }
                            else
                            {
                                SetValueType(ValueType.Ref);
                                var wildContainer = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
                                var childObject = new StorageObject(wildContainer);
                                ReadObject(childObject, depth - 1);
                                containers.Add(wildContainer);
                            }
                        }
                        else if (c == '"')
                        {
                            SetValueType(ValueType.Ref);
                            var wildContainer = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
                            var childObject = new StorageObject(wildContainer);
                            var str = ReadString();
                            childObject.WriteString(str);
                            containers.Add(wildContainer);
                        }
                        else if (c == '[')
                        {
                            SetValueType(ValueType.Ref);
                            var wildContainer = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
                            var childObject = new StorageObject(wildContainer);
                            ReadArrayOn(childObject, ContainerLayout.ArrayName, depth - 1);
                            containers.Add(wildContainer);
                        }
                        else
                        {
                            throw new InvalidOperationException($"Unexpected character '{c}' inside JSON array.");
                        }

                        SkipWhitespace();
                        char sep = Peek();
                        if (sep == ',')
                        {
                            _pos++;
                            continue;
                        }
                        if (sep == ']')
                        {
                            _pos++;
                            break;
                        }

                        throw new InvalidOperationException($"Expected ',' or ']' in array, found '{sep}'.");

                        void SetValueType(ValueType v)
                        {
                            if (arrayType != 0 && arrayType != v)
                            {
                                // as long as not 
                                if (arrayType == ValueType.Blob || v == ValueType.Blob) throw new InvalidOperationException($"Mixed or unsupported element types in array for field '{name}'.");
                                if ((arrayType == ValueType.Ref) != (v == ValueType.Ref)) throw new InvalidOperationException($"Mixed or unsupported element types in array for field '{name}'.");
                            }
                            // always max
                            arrayType = v > arrayType ? v : arrayType;
                        }
                    }

                    if (arrayType == ValueType.Bool || arrayType == ValueType.Float64 || arrayType == ValueType.Int64)
                    {
                        arrayObject.MakeArray(arrayType, scalarValues.Count);
                        var arrayView = arrayObject.AsArray();
                        for (int i = 0; i < scalarValues.Count; i++)
                        {
                            switch (arrayType)
                            {
                                case ValueType.Bool:
                                    arrayView.Raw[i].Write(scalarValues[i].BoolValue);
                                    break;
                                case ValueType.Int64:
                                    arrayView.Raw[i].Write(scalarValues[i].IntValue);
                                    break;
                                case ValueType.Float64:
                                    arrayView.Raw[i].Write(scalarValues[i].FloatValue);
                                    break;
                            }
                        }
                        return;
                    }
                    if (arrayType == ValueType.Blob)
                    {
                        arrayObject.MakeArray(ValueType.Blob, blobs.Count, blobs[0].Length);
                        var arrayView = arrayObject.AsArray();
                        for (int i = 0; i < blobs.Count; i++)
                        {
                            blobs[i].CopyTo(arrayView.Raw[i].Bytes);
                        }
                        return;
                    }
                    if (arrayType == ValueType.Ref)
                    {
                        arrayObject.MakeArray(ValueType.Ref, containers.Count);
                        var arrayView = arrayObject.AsArray();
                        for (int i = 0; i < containers.Count; i++)
                        {
                            Container container = containers[i];
                            if (container != null)
                            {
                                Container.Registry.Shared.Register(container);
                                arrayView.References[i] = containers[i].ID;
                            }
                            else
                            {
                                arrayView.References[i] = Container.Registry.ID.Empty;
                            }
                        }
                        return;
                    }


                    //Fallback for empty / spurious array->treat as empty byte[] to keep layout consistent.
                    arrayObject.WriteArray<byte>(Array.Empty<byte>());
                }
                catch (Exception)
                {
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

                return;
            }

            #endregion

            #region Primitive readers

            private void SkipWhitespace()
            {
                while (_pos < _text.Length && char.IsWhiteSpace(_text[_pos]))
                    _pos++;
            }

            private readonly char Peek() => _pos < _text.Length ? _text[_pos] : '\0';
            private char Next() => _text[_pos++];
            private char NextToken()
            {
                Next();
                SkipWhitespace();
                return Peek();
            }

            private void Expect(char ch)
            {
                SkipWhitespace();
                if (_pos >= _text.Length || _text[_pos] != ch)
                    throw new InvalidOperationException(
                        $"Expected '{ch}' but found '{(_pos < _text.Length ? _text[_pos] : '\0')}'.");
                _pos++;
            }

            private bool TryReadBlob(out ReadOnlySpan<char> base64, string fieldName)
            {
                int start = _pos;
                base64 = default;
                SkipWhitespace();
                if (Peek() != '{')
                    goto failed;
                if (NextToken() != '"')
                    goto failed;
                _pos++;
                for (int i = 0; i < BlobName.Length; i++)
                {
                    if (Peek() != BlobName[i])
                        goto failed;

                    _pos++;
                }
                if (Peek() != '"')
                    goto failed;

                // throw after this
                if (NextToken() != ':')
                    goto throwException;
                if (NextToken() != '"')
                    goto throwException;
                _pos++; // skip opening quote
                int dataStart = _pos;
                while (_pos < _text.Length)
                {
                    char c = _text[_pos++];
                    // terminate
                    if (c == '"')
                    {
                        int length = _pos - dataStart - 1;
                        SkipWhitespace();
                        char n = Peek();
                        if (n != '}')
                            goto throwException;
                        _pos++;
                        base64 = _text.Slice(dataStart, length);
                        return true;
                    }
                    if (!IsBase64Char(c))
                        goto throwException;
                }
            failed:
                _pos = start;
                return false;
            throwException:
                throw new InvalidOperationException($"Unexpected blob data while reading value for blob {fieldName}.");
            }

            private string ReadString()
            {
                SkipWhitespace();
                if (_pos >= _text.Length || _text[_pos] != '"')
                    throw new InvalidOperationException("Expected '\"' at start of JSON string.");

                _pos++; // skip opening quote
                var sb = new StringBuilder();

                while (_pos < _text.Length)
                {
                    char c = _text[_pos++];
                    if (c == '"')
                    {
                        return sb.ToString();
                    }

                    if (c == '\\')
                    {
                        if (_pos >= _text.Length)
                            throw new InvalidOperationException("Unexpected end while parsing string escape.");

                        char esc = _text[_pos++];
                        switch (esc)
                        {
                            case '"': sb.Append('"'); break;
                            case '\\': sb.Append('\\'); break;
                            case '/': sb.Append('/'); break;
                            case 'b': sb.Append('\b'); break;
                            case 'f': sb.Append('\f'); break;
                            case 'n': sb.Append('\n'); break;
                            case 'r': sb.Append('\r'); break;
                            case 't': sb.Append('\t'); break;
                            case 'u':
                                if (_pos + 4 > _text.Length)
                                    throw new InvalidOperationException("Incomplete Unicode escape.");
                                int code = ParseHex16(_text.Slice(_pos, 4));
                                sb.Append((char)code);
                                _pos += 4;
                                break;
                            default:
                                throw new InvalidOperationException($"Unknown escape '\\{esc}' in string.");
                        }
                    }
                    else
                    {
                        sb.Append(c);
                    }
                }

                throw new InvalidOperationException("Unterminated JSON string.");
            }

            private bool ReadBoolean()
            {
                SkipWhitespace();
                if (_pos + 4 <= _text.Length &&
                    _text.Slice(_pos, 4).SequenceEqual("true".AsSpan()))
                {
                    _pos += 4;
                    return true;
                }

                if (_pos + 5 <= _text.Length &&
                    _text.Slice(_pos, 5).SequenceEqual("false".AsSpan()))
                {
                    _pos += 5;
                    return false;
                }

                throw new InvalidOperationException("Invalid boolean literal in JSON.");
            }

            private void ReadNull()
            {
                SkipWhitespace();
                if (_pos + 4 <= _text.Length &&
                    _text.Slice(_pos, 4).SequenceEqual("null".AsSpan()))
                {
                    _pos += 4;
                    return;
                }

                throw new InvalidOperationException("Invalid null literal in JSON.");
            }

            /// <summary>
            /// Read a JSON number and classify it as Int64 when possible,
            /// otherwise as a double.
            /// </summary>
            /// <returns>Whether number is a integer</returns>
            private bool ReadNumber(out long intValue, out double doubleValue)
            {
                SkipWhitespace();

                int start = _pos;
                int len = _text.Length;

                // sign
                if (_text[_pos] == '-')
                    _pos++;

                // integer digits
                if (_pos >= len || !char.IsDigit(_text[_pos]))
                    throw new InvalidOperationException("Invalid JSON number: missing digits.");

                while (_pos < len && char.IsDigit(_text[_pos]))
                    _pos++;

                bool hasDotOrExp = false;

                // fraction
                if (_pos < len && _text[_pos] == '.')
                {
                    hasDotOrExp = true;
                    _pos++;
                    if (_pos >= len || !char.IsDigit(_text[_pos]))
                        throw new InvalidOperationException("Invalid JSON number: missing digits after decimal point.");

                    while (_pos < len && char.IsDigit(_text[_pos]))
                        _pos++;
                }

                // exponent
                if (_pos < len && (_text[_pos] == 'e' || _text[_pos] == 'E'))
                {
                    hasDotOrExp = true;
                    _pos++;
                    if (_pos < len && (_text[_pos] == '+' || _text[_pos] == '-'))
                        _pos++;

                    if (_pos >= len || !char.IsDigit(_text[_pos]))
                        throw new InvalidOperationException("Invalid JSON number: missing digits in exponent.");

                    while (_pos < len && char.IsDigit(_text[_pos]))
                        _pos++;
                }

                var span = _text.Slice(start, _pos - start);

                if (!hasDotOrExp &&
                    long.TryParse(span, NumberStyles.Integer, CultureInfo.InvariantCulture, out long l))
                {
                    intValue = l;
                    doubleValue = l;
                    return true;
                }

                double d = double.Parse(span, NumberStyles.Float, CultureInfo.InvariantCulture);
                intValue = (long)d;
                doubleValue = d;
                return false;
            }

            #endregion


            private static int ParseHex16(ReadOnlySpan<char> span)
            {
                int value = 0;
                for (int i = 0; i < 4; i++)
                {
                    value <<= 4;
                    char c = span[i];
                    if (c >= '0' && c <= '9') value |= c - '0';
                    else if (c >= 'a' && c <= 'f') value |= c - 'a' + 10;
                    else if (c >= 'A' && c <= 'F') value |= c - 'A' + 10;
                    else throw new InvalidOperationException("Invalid hex digit in Unicode escape.");
                }
                return value;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsBase64Char(char c)
            {
                if (c >= 'A' && c <= 'Z') return true;
                if (c >= 'a' && c <= 'z') return true;
                if (c >= '0' && c <= '9') return true;
                if (c == '+' || c == '/') return true;
                if (c == '=') return true; // padding

                return false;
            }
        }
    }

}
