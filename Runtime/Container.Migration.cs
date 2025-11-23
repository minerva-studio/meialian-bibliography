using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    internal sealed partial class Container
    {
        public void Rename(ReadOnlySpan<char> newContainerName)
        {
            var nameBytes = MemoryMarshal.AsBytes(newContainerName);
            ThrowHelper.ThrowIfOverlap(_memory.Buffer.Span, nameBytes);
            ref var containerHeader = ref this.Header;
            int newNameByteLength = nameBytes.Length;
            int offsetDelta = newNameByteLength - containerHeader.ContainerNameLength;
            var newSize = _memory.Buffer.Length + offsetDelta;
            var newMemory = AllocatedMemory.Create(newSize);

            // Copy old data
            // headers
            int preName = ContainerHeader.Size + FieldHeader.Size * containerHeader.FieldCount;
            //  copy up to container name
            _memory.Buffer.Span[..preName].CopyTo(newMemory.Buffer.Span);
            ref var newContainerHeader = ref Unsafe.As<byte, ContainerHeader>(ref newMemory.Buffer.Span[0]);
            newContainerHeader.Length = newSize;
            newContainerHeader.DataOffset += offsetDelta;
            newContainerHeader.ContainerNameLength = checked((short)newNameByteLength);
            // apply offset delta
            for (int i = 0; i < containerHeader.FieldCount; i++)
            {
                ref var fieldHeader = ref Unsafe.As<byte, FieldHeader>(ref newMemory.Buffer.Span[ContainerHeader.Size + FieldHeader.Size * i]);
                fieldHeader.NameOffset += offsetDelta;
                fieldHeader.DataOffset += offsetDelta;
            }

            // copy new container name
            nameBytes.CopyTo(newMemory.Buffer.Span.Slice(preName, newNameByteLength));
            // copy after container name
            _memory.Buffer.Span[(preName + Header.ContainerNameLength)..].CopyTo(newMemory.Buffer.Span[(preName + newNameByteLength)..]);

            ChangeContent(newMemory);
        }




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
        public void Rescheme(ContainerLayout newLayout, bool quiet = false)
        {
            if (newLayout is null) throw new ArgumentNullException(nameof(newLayout));
            EnsureNotDisposed();

            // same header
            if (newLayout.MatchesHeader(HeadersSegment))
                return;

            // Prepare destination buffer
            //byte[] dstBuf = DefaultPool.Rent(newSchema.TotalLength);
            using UnregisterBuffer unregister = UnregisterBuffer.New(this);
            using FieldDeleteEventBuffer delete = FieldDeleteEventBuffer.New(this);
            ref ContainerHeader header = ref Header;
            int newLength = newLayout.TotalLength + header.ContainerNameLength;
            AllocatedMemory dstBuf = AllocatedMemory.Create(newLength);
            var dst = dstBuf.Buffer.Span;
            dst.Clear();

            newLayout.WriteTo(dst, _memory.AsSpan(header.ContainerNameOffset, header.ContainerNameLength));

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
                        unregister.Add(oldIds, oldField.IsInlineArray, !quiet);
                    }
                    else
                    {
                        delete.Add(GetFieldName(in oldField).ToString(), oldField.FieldType);
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
                        unregister.Add(oldIds, oldField.IsInlineArray, !quiet);
                    }
                    else
                    {
                        delete.Add(GetFieldName(in oldField).ToString(), oldField.FieldType);
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
                    unregister.Add(oldIds[min..], oldField.IsInlineArray);
                }
            }

            // ---- Swap schema & buffers ----
            var oldBuf = _memory;
            ChangeContent(dstBuf);
            oldBuf.Dispose();
            unregister.Send();
            delete.Send();
        }




        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeForObject(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null)
            => ReschemeFor(fieldName, TypeData.Ref, inlineArrayLength);

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeFor<T>(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) where T : unmanaged
            => ReschemeFor(fieldName, TypeUtil<T>.Type, inlineArrayLength);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeFor(ReadOnlySpan<char> fieldName, TypeData elementType, int? inlineArrayLength)
        {
            int index = IndexOf(fieldName);
            int elementCount = inlineArrayLength ?? 1;
            ValueType valueType = elementType.ValueType;
            int elementSize = elementType.Size;

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
                Span<byte> span = next.Buffer.Span;
                ref var nextHeader = ref ContainerHeader.FromSpan(span);
                nextHeader = currentHeader;
                nextHeader.FieldCount += isNewField ? 1 : 0; // count increment
                nextHeader.DataOffset += isNewField ? FieldHeader.Size + fieldName.Length * sizeof(char) : 0;
                nextHeader.Length = newLength;

                // copy container name
                _memory.AsSpan(currentHeader.ContainerNameOffset, currentHeader.ContainerNameLength).CopyTo(span.Slice(currentHeader.ContainerNameOffset + (isNewField ? FieldHeader.Size : 0), currentHeader.ContainerNameLength));
                nextHeader.ContainerNameLength = currentHeader.ContainerNameLength; // preserve old name length

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




        public void ResizeArrayField(int index, int newLength)
        {
            ref FieldHeader fieldHeader = ref GetFieldHeader(index);
            if (!fieldHeader.IsInlineArray)
                throw new ArgumentException("Field is not an inline array.", nameof(index));
            // good case: single member array container
            if (index == 0 && FieldCount == 1)
            {
                ReschemeForArray(newLength, fieldHeader.ElementType);
                return;
            }
            // bad case: need reschemefor
            else
            {
                var name = GetFieldName(index);
                ReschemeFor(name, fieldHeader.ElementType, newLength);
            }
        }

        public void ReschemeForArray(int length, TypeData type)
        {
            ref ContainerHeader oldHeader = ref Header;
            ValueType valueType = type.ValueType;
            if (oldHeader.FieldCount == 1)
            {
                ref var oldField = ref GetFieldHeader(0);
                // already same type and length
                if (oldField.Type == valueType && oldField.ElementCount == length)
                    return;
            }

            int elementSize = type.Size;
            int dataOffset = ContainerHeader.Size
                + FieldHeader.Size
                + oldHeader.ContainerNameLength
                + ContainerLayout.ArrayName.Length * sizeof(char);
            int dataLength = length * elementSize;
            int size = dataOffset + dataLength;

            AllocatedMemory allocatedMemory = AllocatedMemory.Create(size);
            // copy header
            ref ContainerHeader newHeader = ref Unsafe.As<byte, ContainerHeader>(ref allocatedMemory.Buffer.Span[0]);
            newHeader = oldHeader;
            newHeader.Length = size;
            newHeader.FieldCount = 1;
            newHeader.DataOffset = dataOffset;
            newHeader.ContainerNameLength = oldHeader.ContainerNameLength;
            // copy container name
            _memory.AsSpan(oldHeader.ContainerNameOffset, oldHeader.ContainerNameLength).CopyTo(allocatedMemory.AsSpan(newHeader.ContainerNameOffset, newHeader.ContainerNameLength));
            // write array field header
            ref FieldHeader fieldHeader = ref FieldHeader.FromSpanAndFieldIndex(allocatedMemory.Buffer.Span, 0);
            fieldHeader.NameLength = (short)ContainerLayout.ArrayName.Length;
            fieldHeader.NameOffset = newHeader.ContainerNameOffset + newHeader.ContainerNameLength;
            fieldHeader.DataOffset = dataOffset;
            fieldHeader.Length = dataLength;
            fieldHeader.FieldType = new FieldType(valueType, true);
            fieldHeader.ElemSize = (short)elementSize;
            // write array field name
            var nameBytes = MemoryMarshal.AsBytes(ContainerLayout.ArrayName.AsSpan());
            nameBytes.CopyTo(allocatedMemory.AsSpan(fieldHeader.NameOffset, nameBytes.Length));

            // zero init data area
            Span<byte> dstBytes = allocatedMemory.AsSpan(dataOffset, dataLength);   // NEW buffer slice
            dstBytes.Clear();

            // record references to dispose
            Span<ContainerReference> oldIds = default;

            // try copy old data
            if (FieldCount == 1)
            {
                ref var oldField = ref GetFieldHeader(0);
                // truely same type
                // value type the same, but is array/non array, only diff in length (arr or non arr)
                if (oldField.Type == valueType)
                {
                    var srcBytes = GetFieldData(in oldField);           // OLD buffer read-only
                                                                        // copy as much as possible
                    int min = Math.Min(srcBytes.Length, dstBytes.Length);
                    srcBytes[..min].CopyTo(dstBytes[..min]);
                }
                // implicit conversion needed
                else if (oldField.Type != ValueType.Blob && valueType != ValueType.Blob && valueType != ValueType.Ref)
                {
                    var srcBytes = GetFieldData(in oldField);           // OLD buffer read-only
                    Migration.MigrateValueFieldBytes(srcBytes, dstBytes, oldField.Type, valueType, true);
                }
                // any of them are refs
                if (oldField.Type == ValueType.Ref)
                {
                    // Ref-to-Ref migration unchanged (copy ids, unregister tails)
                    oldIds = GetFieldData<ContainerReference>(in oldField);
                    if (valueType == ValueType.Ref)
                    {
                        var newIds = MemoryMarshal.Cast<byte, ContainerReference>(dstBytes);
                        int min = Math.Min(oldIds.Length, newIds.Length);
                        for (int i = 0; i < min; i++) newIds[i] = oldIds[i];
                        // to dispose
                        oldIds = oldIds[min..];
                    }
                }
            }



            using var oldMemory = _memory;
            ChangeContent(allocatedMemory);

            // dispose references
            for (int i = 0; i < oldIds.Length; i++)
            {
                if (oldIds[i] == Registry.ID.Empty) continue;
                var container = Registry.Shared.GetContainer(oldIds[i]);
                Registry.Shared.Unregister(container);
            }
        }





        private void ChangeContent(AllocatedMemory next)
        {
            _memory = next;
            _schemaVersion++;
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
