using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
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
        public void Rescheme(Action<ObjectBuilder> edit)
        {
            EnsureNotDisposed();
            Rescheme(ObjectBuilder.FromContainer(this).Variate(edit).BuildLayout());  // zero-init by default
        }

        /// <summary>
        /// Internal overload allowing to skip zero-initialization when the caller
        /// will fully overwrite the new buffer manually.
        /// </summary>  
        public void Rescheme(ContainerLayout newSchema)
        {
            if (newSchema is null) throw new ArgumentNullException(nameof(newSchema));
            EnsureNotDisposed();

            // same header
            if (HeadersSegment.SequenceEqual(newSchema.Span))
                return;

            // Prepare destination buffer
            //byte[] dstBuf = DefaultPool.Rent(newSchema.TotalLength);
            AllocatedMemory dstBuf = AllocatedMemory.Create(newSchema.TotalLength);
            var dst = dstBuf.Span;
            dst.Clear();
            newSchema.Span.CopyTo(dst);

            // Prepare new header hints (migrate by name)            
            ContainerView newView = new ContainerView(dst);

            int fieldCount = FieldCount;
            // ---- Migrate each old field into new layout ----
            for (int oldIdx = 0; oldIdx < fieldCount; oldIdx++)
            {
                ref var oldField = ref GetFieldHeader(oldIdx);
                var name = GetFieldName(in oldField);

                var newIdx = newView.IndexOf(name);
                if (newIdx < 0)
                {
                    // Removed: if ref, unregister all non-zero children
                    if (oldField.IsRef)
                    {
                        var oldIds = GetFieldData<ContainerReference>(in oldField);
                        for (int j = 0; j < oldIds.Length; j++)
                            Registry.Shared.Unregister(ref oldIds[j]);
                    }
                    continue;
                }

                ref var newField = ref newView.GetFieldHeader(newIdx);
                var dstBytes = newView.GetFieldBytes(newIdx); // NEW buffer slice


                // diff in ref/non-ref
                if (oldField.IsRef != newField.IsRef)
                {
                    // Removed: if ref, unregister all non-zero children
                    if (oldField.IsRef)
                    {
                        var oldIds = GetFieldData<ContainerReference>(in oldField);
                        for (int j = 0; j < oldIds.Length; j++)
                            Registry.Shared.Unregister(ref oldIds[j]);
                    }
                    dstBytes.Clear();
                    continue;
                }
                if (!oldField.IsRef)
                {
                    // truely same type
                    // value type the same, but is array/non array, only diff in length (arr or non arr)
                    if (oldField.Type == newField.Type)
                    {
                        var srcBytes = GetFieldData(in oldField);           // OLD buffer read-only
                        // copy as much as possible
                        int min = Math.Min(srcBytes.Length, dstBytes.Length);
                        srcBytes[..min].CopyTo(dstBytes[..min]);
                        continue;
                    }
                    // implicit conversion needed
                    else if (oldField.Type != ValueType.Blob && newField.Type != ValueType.Blob && newField.Type != ValueType.Ref)
                    {
                        var srcBytes = GetFieldData(in oldField);           // OLD buffer read-only
                        Migration.MigrateValueFieldBytes(srcBytes, dstBytes, oldField.Type, newField.Type, true);
                        continue;
                    }
                }
                else
                {
                    // Ref-to-Ref migration unchanged (copy ids, unregister tails)
                    var oldIds = GetFieldData<ContainerReference>(in oldField);
                    var newIds = MemoryMarshal.Cast<byte, ContainerReference>(dstBytes);

                    int min = Math.Min(oldIds.Length, newIds.Length);
                    for (int i = 0; i < min; i++) newIds[i] = oldIds[i];
                    for (int i = min; i < oldIds.Length; i++) Registry.Shared.Unregister(ref oldIds[i]);
                }
            }

            // ---- Swap schema & buffers ----
            var oldBuf = _memory;
            ChangeContent(dstBuf);
            oldBuf.Dispose();
        }




        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeForObject(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) => ReschemeFor(fieldName, ValueType.Ref, Unsafe.SizeOf<ContainerReference>(), inlineArrayLength);

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeFor<T>(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) where T : unmanaged => ReschemeFor(fieldName, TypeUtil<T>.ValueType, TypeUtil<T>.Size, inlineArrayLength);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeFor(ReadOnlySpan<char> fieldName, ValueType valueType, int elementSize, int? inlineArrayLength)
        {
            int index = IndexOf(fieldName);
            int elementCount = inlineArrayLength ?? 1;

            bool isNewField = index < 0;
            int targetIndex = isNewField ? ~index : index;
            var existedHeader = isNewField ? default : GetFieldHeader(index);
            int newDataLength = elementSize * elementCount;
            ref var currentHeader = ref Header;
            int newLength = isNewField
                // new field, add header, name, data length
                ? currentHeader.Length + FieldHeader.Size + fieldName.Length * sizeof(char) + newDataLength
                // already exist, then no header size change, no name change, only data size change
                : currentHeader.Length - existedHeader.Length + newDataLength;
            FieldType newFieldType = new(valueType, inlineArrayLength.HasValue);

            if (!isNewField)
            {
                // no rescheme needed (exist, same type, same inline length)
                if (existedHeader.FieldType == newFieldType && existedHeader.ElementCount == elementCount)
                    return index;
                // length is fine, just need to reset header
                if (existedHeader.Length >= newDataLength)
                {
                    existedHeader.Length = newDataLength;
                    existedHeader.FieldType = newFieldType;
                    existedHeader.ElemSize = (short)elementSize;
                    return index;
                }
            }

            AllocatedMemory next = AllocatedMemory.Create(newLength);
            AllocatedMemory curr = _memory;
            try
            {
                // header copy
                Span<byte> span = next.Span;
                ref var nextHeader = ref ContainerHeader.FromSpan(span);
                nextHeader = currentHeader;
                nextHeader.FieldCount += isNewField ? 1 : 0; // count increment
                nextHeader.DataOffset += isNewField ? FieldHeader.Size + fieldName.Length * sizeof(char) : 0;
                nextHeader.Length = newLength;

                int newFieldCount = nextHeader.FieldCount;
                int nameOffset = nextHeader.NameOffset;
                int dataOffset = nextHeader.DataOffset;
                // copy header
                for (int i = 0, j = 0; i < newFieldCount; i++)
                {
                    ref var f = ref FieldHeader.FromSpanAndFieldIndex(span, i);
                    // match insertion
                    if (i == targetIndex)
                    {
                        f.NameLength = (short)fieldName.Length;
                        f.Length = newDataLength;
                        f.ElemSize = (short)elementSize;
                        f.FieldType = newFieldType;
                        f.NameOffset = nameOffset;
                        f.DataOffset = dataOffset;
                        // name
                        fieldName.CopyTo(MemoryMarshal.Cast<byte, char>(next.AsSpan(nameOffset, f.NameLength * sizeof(char))));
                        // data
                        next.AsSpan(dataOffset, f.Length).Clear();
                        if (!isNewField) j++;
                    }
                    else
                    {
                        ref FieldHeader currentFieldHeader = ref GetFieldHeader(j++);
                        f = currentFieldHeader;
                        f.NameOffset = nameOffset;
                        f.DataOffset = dataOffset;
                        // name
                        GetFieldName(in currentFieldHeader).CopyTo(MemoryMarshal.Cast<byte, char>(next.AsSpan(nameOffset, f.NameLength * sizeof(char))));
                        // data
                        GetFieldData(in currentFieldHeader).CopyTo(next.AsSpan(dataOffset, f.Length));
                    }

                    nameOffset += f.NameLength * sizeof(char);
                    dataOffset += f.Length;
                }

                ChangeContent(next);
                curr.Dispose();
            }
            catch (Exception)
            {
                next.Dispose();
                throw;
            }
            return targetIndex;
        }

        private void ChangeContent(AllocatedMemory next)
        {
            _memory = next;
            _schemaVersion++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [Obsolete]
        public int ReschemeFor_Old<T>(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) where T : unmanaged => ReschemeForField_Internal_Old(fieldName, TypeUtil<T>.ValueType, inlineArrayLength);
        [Obsolete]
        private int ReschemeForField_Internal_Old(ReadOnlySpan<char> fieldName, ValueType valueType, int? inlineArrayLength = null)
        {
            var objectBuilder = new ObjectBuilder();
            ref var containerHeader = ref this.Header;
            int minimumLength = containerHeader.DataOffset - containerHeader.NameOffset;

            char[] fieldNameBuffer = ArrayPool<char>.Shared.Rent(fieldName.Length);
            char[] nameBuffer = ArrayPool<char>.Shared.Rent(minimumLength / sizeof(char));
            try
            {
                NameSegment.CopyTo(MemoryMarshal.AsBytes(nameBuffer.AsSpan()));
                fieldName.CopyTo(fieldNameBuffer);
                Memory<char> tempName = fieldNameBuffer.AsMemory(0, fieldName.Length);
                if (inlineArrayLength.HasValue)
                {
                    objectBuilder.SetArray(tempName, valueType, inlineArrayLength.Value);
                }
                else objectBuilder.SetScalar(tempName, valueType);

                int baseOffset = containerHeader.NameOffset;
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
                AllocatedMemory newBuffer = AllocatedMemory.Create(newSize);
                objectBuilder.WriteTo(ref newBuffer);

                // switch buffer now
                var oldBuffer = this._memory;
                try { ChangeContent(newBuffer); }
                finally { oldBuffer.Dispose(); }

                return IndexOf(tempName.Span);
            }
            finally
            {
                ArrayPool<char>.Shared.Return(fieldNameBuffer);
                ArrayPool<char>.Shared.Return(nameBuffer);
            }
        }





        ///// <summary>
        ///// Try read scalar with implicit conversion if needed. will not change type hint if type known.
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="index"></param>
        ///// <param name="value"></param>
        ///// <returns></returns>
        //public bool TryReadScalarImplicit<T>(int index, out T value) where T : unmanaged
        //{
        //    value = default;
        //    if (View[index].Type == ValueType.Unknown) return false;

        //    var view = GetValueView(index);
        //    return view.TryRead(out value);
        //}

        /// <summary>
        /// Try write scalar with implicit conversion if needed. will not change type hint if type known.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool TryWriteScalarImplicit<T>(ref FieldHeader field, T value) where T : unmanaged
        {
            ValueType srcType = TypeUtil<T>.ValueType;
            // type match, direct write
            if (field.FieldType.b == (byte)srcType)
            {
                unsafe
                {
                    var p = GetFieldData_Unsafe(in field);
                    Unsafe.Write(p, value);
                    return true;
                }
            }
            Span<byte> dstSpan = GetFieldData(in field);
            // current type unknown, update hint
            if (field.FieldType == FieldType.ScalarUnknown)
            {
                // too large to fit
                if (TypeUtil<T>.Size > dstSpan.Length)
                    return false;

                field.FieldType = srcType;
                MemoryMarshal.Write(dstSpan, ref value);
                return true;
            }

            // implicit conversion
            var dstType = field.Type;
            var srcSpan = MemoryMarshal.Cast<T, byte>(MemoryMarshal.CreateSpan(ref value, 1));
            return Migration.TryWriteTo(srcSpan, srcType, dstSpan, dstType, false);
        }

        /// <summary>
        /// Try read scalar with explicit conversion if needed. will not change type hint if type known.
        /// </summary>
        /// <typeparam name="T"></typeparam> 
        /// <param name="value"></param>
        /// <returns></returns> 
        public bool TryReadScalarExplicit<T>(ref FieldHeader field, out T value) where T : unmanaged
        {
            var readType = TypeUtil<T>.ValueType;
            // same type
            if ((byte)readType == field.FieldType.b)
            {
                unsafe
                {
                    var p = GetFieldData_Unsafe(in field);
                    fixed (void* pdst = &value)
                    {
                        Unsafe.Write(pdst, *(T*)p);
                        return true;
                    }
                }
            }

            var view = new ValueView(GetFieldData(in field), field.Type);
            return view.TryRead(out value, true);
        }

        public bool TryWriteScalarExplicit<T>(ref FieldHeader field, T value) where T : unmanaged
        {
            var dstType = field.Type;
            var dstSpan = GetFieldData(in field);
            var srcType = TypeUtil<T>.ValueType;

            // conversion
            var srcSpan = MemoryMarshal.Cast<T, byte>(MemoryMarshal.CreateSpan(ref value, 1));
            var view = new ReadOnlyValueView(srcSpan, srcType);
            return view.TryWriteTo(dstSpan, dstType, true);
        }
    }
}
