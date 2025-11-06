using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Amlos.Container.TypeUtil;

namespace Amlos.Container
{
    internal sealed partial class Container
    {
        /// <summary>
        /// Migrate this container to a new schema by adding/removing fields.
        /// Rules:
        ///  - New fields (present only in new schema) are zero-initialized.
        ///  - Removed fields (present only in old schema) are discarded.
        ///  - Container identity (ID) is preserved.
        ///  - New buffer is always zero-initialized by default.
        /// </summary> 
        [Obsolete]
        public void Rescheme(Action<SchemaBuilder> edit)
        {
            EnsureNotDisposed();
            Rescheme(Schema.Variate(edit));  // zero-init by default
        }

        /// <summary>
        /// Internal overload allowing to skip zero-initialization when the caller
        /// will fully overwrite the new buffer manually.
        /// </summary> 
        [Obsolete]
        public void Rescheme(Schema_Old newSchema, bool zeroInitNewBuffer = true)
        {
            if (newSchema is null) throw new ArgumentNullException(nameof(newSchema));
            EnsureNotDisposed();

            if (ReferenceEquals(_schema, newSchema) || _schema.Equals(newSchema))
                return;

            // Prepare destination buffer
            byte[] dstBuf = newSchema.Stride == 0
                ? Array.Empty<byte>()
                : newSchema.Pool.Rent(zeroInit: zeroInitNewBuffer);
            var dst = dstBuf.AsSpan();

            // Prepare new header hints (migrate by name)
            var newHints = dstBuf.AsSpan(0, newSchema.HeaderSize);

            // ---- Migrate each old field into new layout ----
            for (int oldIdx = 0; oldIdx < _schema.Fields.Count; oldIdx++)
            {
                var oldField = _schema.Fields[oldIdx];
                byte oldHint = (oldIdx < HeaderSegment_Old.Length) ? HeaderSegment_Old[oldIdx] : (byte)0;

                if (!newSchema.TryGetField(oldField.Name, out var newField))
                {
                    // Removed: if ref, unregister all non-zero children
                    if (oldField.IsRef)
                    {
                        var oldIds = GetRefSpan(oldField);
                        for (int j = 0; j < oldIds.Length; j++)
                            Registry.Shared.Unregister(ref oldIds[j]);
                    }
                    continue;
                }

                int newIdx = newSchema.IndexOf(newField.Name);
                var dstBytes = dst.Slice(newField.Offset, newField.AbsLength); // NEW buffer slice


                if (oldField.IsRef != newField.IsRef)
                {
                    // Removed: if ref, unregister all non-zero children
                    if (oldField.IsRef)
                    {
                        var oldIds = GetRefSpan(oldField);
                        for (int j = 0; j < oldIds.Length; j++)
                            Registry.Shared.Unregister(ref oldIds[j]);
                    }
                    dstBytes.Clear();
                    continue;
                }
                if (!oldField.IsRef)
                {
                    // we dont know what it could be, use unknown for now
                    // Value-to-value migration: convert from oldHint -> targetHint
                    var srcBytes = GetReadOnlySpan(oldField);                 // OLD buffer read-only
                    if (srcBytes.Length == dstBytes.Length)
                    {
                        newHints[newIdx] = oldHint;
                        srcBytes.CopyTo(dstBytes);
                    }
                    else
                    {
                        newHints[newIdx] = Pack(ValueType.Unknown, IsArray(oldHint));
                    }
                }
                else
                {
                    newHints[newIdx] = oldHint;
                    // Ref-to-Ref migration unchanged (copy ids, unregister tails)
                    var oldIds = GetRefSpan(oldField);
                    var newIds = MemoryMarshal.Cast<byte, ulong>(dstBytes);

                    int keep = Math.Min(oldIds.Length, newIds.Length);
                    for (int i = 0; i < keep; i++) newIds[i] = oldIds[i];
                    for (int i = keep; i < newIds.Length; i++) newIds[i] = 0UL;
                    for (int i = keep; i < oldIds.Length; i++) Registry.Shared.Unregister(ref oldIds[i]);
                }
            }

            // ---- Swap schema & buffers ----
            var oldSchema = _schema;
            var oldBuf = _buffer;

            _schema = newSchema;
            _buffer = dstBuf;

            if (oldSchema.Stride > 0 && oldBuf.Length > 0 && !ReferenceEquals(oldBuf, Array.Empty<byte>()))
                oldSchema.Pool.Return(oldBuf);
        }




        public int GetFieldIndexOrRescheme<T>(string fieldName) where T : unmanaged
        {
            int index = IndexOf(fieldName);
            if (index < 0)
            {
                index = ReschemeForNew<T>(fieldName);
            }

            return index;
        }

        public int GetFieldIndexOrReschemeObject(string fieldName)
        {
            int index = IndexOf(fieldName);
            if (index < 0) index = ReschemeForNewObject(fieldName);
            return index;
        }

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public int ReschemeForNew<T>(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) where T : unmanaged => ReschemeForField_Internal(fieldName, TypeUtil.PrimOf<T>(), inlineArrayLength);

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public int ReschemeForNewObject(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) => ReschemeForField_Internal(fieldName, ValueType.Ref, inlineArrayLength);

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public int ReschemeFor<T>(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) where T : unmanaged => ReschemeForField_Internal(fieldName, TypeUtil.PrimOf<T>(), inlineArrayLength);

        private int ReschemeForField_Internal(ReadOnlySpan<char> fieldName, ValueType valueType, int? inlineArrayLength = null)
        {
            var objectBuilder = new ObjectBuilder();
            int minimumLength = View.Header.DataOffset - View.Header.NameOffset;

            char[] fieldNameBuffer = ArrayPool<char>.Shared.Rent(fieldName.Length);
            char[] nameBuffer = ArrayPool<char>.Shared.Rent(minimumLength / sizeof(char));
            try
            {
                View.NameSegment.CopyTo(MemoryMarshal.AsBytes(nameBuffer.AsSpan(0, minimumLength)));
                fieldName.CopyTo(fieldNameBuffer);
                if (inlineArrayLength.HasValue)
                {
                    objectBuilder.SetArray(fieldNameBuffer.AsMemory(), new FieldType(valueType, true), inlineArrayLength.Value);
                }
                else objectBuilder.SetScalar(fieldNameBuffer.AsMemory(), new FieldType(valueType, false));

                int baseOffset = View.Header.NameOffset;
                for (int i = 0; i < FieldCount; i++)
                {
                    FieldView fieldView = View[i];
                    ReadOnlyMemory<char> name;

                    // todo: get name from name buffer
                    int fixedOffset = ((int)fieldView.Header.NameOffset - baseOffset) / sizeof(char);
                    name = nameBuffer.AsMemory(fixedOffset, fieldView.Name.Length);

                    // field to rescheme
                    if (!name.Span.SequenceEqual(fieldName))
                        objectBuilder.SetScalar(name, fieldView.FieldType, fieldView.Data);
                }

                int newSize = objectBuilder.CountByte();
                byte[] newBuffer = DefaultPool.Rent(newSize);
                try
                {
                    objectBuilder.Build(newBuffer);
                    // switch buffer now
                    var oldBuffer = this._buffer;
                    this._buffer = newBuffer;
                    DefaultPool.Return(oldBuffer);
                }
                finally
                {
                    DefaultPool.Return(newBuffer);
                }
            }
            finally
            {
                ArrayPool<char>.Shared.Return(fieldNameBuffer);
                ArrayPool<char>.Shared.Return(nameBuffer);
            }

            var index = IndexOf(fieldName);
            View.Fields[index].FieldType.Type = valueType;
            return index;
        }







        /// <summary>
        /// Ensure that 'fieldName' exists as a non-ref value field sized to sizeof(T).
        /// If it exists with same size but different type hint, convert the in-place bytes and update the hint,
        /// without rescheming. If it doesn't exist or size mismatches, rebuild schema to replace the field.
        /// </summary>
        public void EnsureFieldForRead<T>(int index, bool isExplicit = false) where T : unmanaged
        {
            FieldType fieldType = View.Fields[index].FieldType;
            FieldType targetType = FieldType.Of<T>(fieldType.IsArray);
            ValueType valueType = fieldType.Type;
            ValueType target = targetType.Type;

            // is same type
            if (valueType == target)
                return;

            // if we don't know current type, assume new type is valid
            if (valueType == ValueType.Unknown)
            {
                View.Fields[index].FieldType = Pack(target, fieldType.IsArray);
                return;
            }
            Migrate<T>(index, target);
        }

        public bool Migrate<T>(int index, ValueType target) where T : unmanaged
        {
            FieldType fieldType = View.Fields[index].FieldType;
            bool isArray = fieldType.IsArray;
            ValueType valueType = fieldType.Type;


            var field = View[index];
            // 2) Existing but ref ¡ú not allowed
            if (field.IsRef)
                throw new InvalidOperationException($"Field '{field.Name.ToString()}' is a reference; cannot read value T={typeof(T).Name}.");

            int oldElementSize = TypeUtil.SizeOf(valueType);
            int newElementSize = TypeUtil.SizeOf(PrimOf<T>());
            int dataLength = (int)field.Length;
            int arrayLength = isArray ? dataLength / oldElementSize : 1;

            // inplace conversion, given element same size
            if (oldElementSize == newElementSize)
            {
                Span<byte> data = field.Data;
                if (isArray) MigrationConverter.ConvertArrayInPlaceSameSize(data, arrayLength, valueType, target);
                else MigrationConverter.ConvertScalarInPlace(data, valueType, target);
                View[index].Header.FieldType = Pack(target, isArray);
            }
            // different size
            else
            {
                // copy old data
                int nameLength = field.Name.Length * sizeof(byte);
                int minimumLength = nameLength + dataLength;
                byte[] array = ArrayPool<byte>.Shared.Rent(minimumLength);
                try
                {
                    Span<char> name = MemoryMarshal.Cast<byte, char>(array.AsSpan(0, nameLength));
                    Span<byte> buffer = array.AsSpan(nameLength);
                    // store name + data
                    field.Name.CopyTo(name);
                    field.Data.CopyTo(buffer);

                    // rescheme
                    //Rescheme(b =>
                    //{
                    //    b.RemoveField(field.Name);
                    //    if (isArray) b.AddArrayOf<T>(field.Name, arrayLength);
                    //    else b.AddFieldOf<T>(field.Name);
                    //});
                    ReschemeFor<T>(field.Name, isArray ? arrayLength : null);
                    var newIndex = IndexOf(name);
                    var newField = View[newIndex];
                    var newSpan = newField.Data;
                    // fill back
                    MigrationConverter.MigrateValueFieldBytes(buffer, newSpan, valueType, target, true);
                    newField.Header.FieldType = Pack(target, isArray);
                }
                catch
                {
                    return false;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(array);
                }
            }

            return true;
        }




        /// <summary>
        /// Try read scalar with implicit conversion if needed. will not change type hint if type known.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool TryReadScalarImplicit<T>(int index, out T value) where T : unmanaged
        {
            value = default;
            if (View[index].Type == ValueType.Unknown) return false;

            var view = GetValueView(index);
            return view.TryRead(out value);
        }

        /// <summary>
        /// Try write scalar with implicit conversion if needed. will not change type hint if type known.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool TryWriteScalarImplicit<T>(int index, T value) where T : unmanaged
        {
            var field = View[index];
            var dstType = field.Type;
            var dstSpan = field.Data;
            var srcType = TypeUtil.PrimOf<T>();
            // current type unknown, update hint
            if (dstType == ValueType.Unknown)
            {
                // too large to fit
                if (Unsafe.SizeOf<T>() > dstSpan.Length)
                    return false;

                field.Header.FieldType = TypeUtil.Pack(srcType, View[index].IsArray);
                MemoryMarshal.Write(dstSpan, ref Unsafe.AsRef(value));
                return true;
            }
            // type match, direct write
            if (dstType == srcType)
            {
                MemoryMarshal.Write(dstSpan, ref Unsafe.AsRef(value));
                return true;
            }

            // implicit conversion
            var srcSpan = MemoryMarshal.Cast<T, byte>(MemoryMarshal.CreateSpan(ref value, 1));
            var view = new ValueView(srcSpan, srcType);
            return view.TryWrite(dstSpan, dstType);
        }

        /// <summary>
        /// Try read scalar with explicit conversion if needed. will not change type hint if type known.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool TryReadScalarExplicit<T>(int index, out T value) where T : unmanaged
        {
            var view = GetValueView(index);
            return view.TryRead(out value, true);
        }

        private bool TryWriteScalarExplicit<T>(int index, T value) where T : unmanaged
        {
            var field = View[index];
            var dstType = field.Type;
            var dstSpan = field.Data;
            var srcType = TypeUtil.PrimOf<T>();

            // conversion
            var srcSpan = MemoryMarshal.Cast<T, byte>(MemoryMarshal.CreateSpan(ref value, 1));
            var view = new ValueView(srcSpan, srcType);
            return view.TryWrite(dstSpan, dstType, true);
        }
    }
}
