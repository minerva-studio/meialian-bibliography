using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    internal sealed partial class Container
    {
        /// <summary>
        /// Rename container, will not invoke any events.
        /// </summary>
        /// <param name="newContainerName"></param>
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

        public void Move(ReadOnlySpan<char> oldFieldName, ReadOnlySpan<char> newFieldName)
        {
            int fieldIndex = IndexOf(oldFieldName);
            if (fieldIndex < 0)
                ThrowHelper.ArgumentException("Field not found.", nameof(oldFieldName));
            // no change
            if (oldFieldName.SequenceEqual(newFieldName))
                return;

            ref var containerHeader = ref this.Header;
            ref var fieldHeader = ref GetFieldHeader(fieldIndex);
            var oldNameBytes = MemoryMarshal.AsBytes(oldFieldName);
            var newNameBytes = MemoryMarshal.AsBytes(newFieldName);
            Span<byte> oldSpan = _memory.Buffer.Span;
            ThrowHelper.ThrowIfOverlap(oldSpan, oldNameBytes);
            ThrowHelper.ThrowIfOverlap(oldSpan, newNameBytes);

            int nameLengthByteDelta = newNameBytes.Length - oldNameBytes.Length;
            int fieldCount = FieldCount;
            int newSize = _memory.Buffer.Length + nameLengthByteDelta;

            var newMemory = AllocatedMemory.Create(newSize);
            var oldMemory = _memory;

            Span<byte> newSpan = newMemory.Buffer.Span;

            try
            {
                // 1) Copy container header ONLY, and adjust its fields
                ref var newContainerHeader = ref Unsafe.As<byte, ContainerHeader>(ref newSpan[0]);
                newContainerHeader = containerHeader;
                newContainerHeader.Length = newSize;
                newContainerHeader.DataOffset += nameLengthByteDelta;

                // 2) Copy container name straight from old buffer
                // Container name sits immediately after the field headers
                int containerNameSrcOffset = ContainerHeader.Size + FieldHeader.Size * containerHeader.FieldCount;
                oldSpan.Slice(containerNameSrcOffset, containerHeader.ContainerNameLength).CopyTo(
                    newSpan.Slice(containerNameSrcOffset, containerHeader.ContainerNameLength));

                // 3) Compute new order for the moved field
                int targetIndex = 0;
                for (int i = 0; i < fieldCount; i++)
                {
                    if (i == fieldIndex) continue;

                    var otherName = GetFieldName(i);
                    int comp = otherName.CompareTo(newFieldName, StringComparison.Ordinal);
                    if (comp < 0)
                        targetIndex++;
                    else if (comp == 0)
                        ThrowHelper.ThrowInvalidOperation("Field with the new name already exists.");
                    else
                        break;
                }

                // 4) Rebuild field headers entirely in destination buffer to avoid overlapping copy errors
                var newHeadersSpan = newSpan.Slice(ContainerHeader.Size, FieldHeader.Size * fieldCount);
                int nameOffset = ContainerHeader.Size + FieldHeader.Size * fieldCount + containerHeader.ContainerNameLength;
                int dataOffset = newContainerHeader.DataOffset;

                // Prepare a temporary array of headers in new order
                for (int i = 0; i < fieldCount; i++)
                {
                    ref var newFh = ref FieldHeader.FromSpanAndFieldIndex(newSpan, i);

                    if (i == targetIndex)
                    {
                        // Moved field goes here with new name length
                        ref var srcMoved = ref FieldHeader.FromSpanAndFieldIndex(oldSpan, fieldIndex);
                        newFh = srcMoved;
                        newFh.NameLength = (short)newFieldName.Length;
                    }
                    else
                    {
                        int srcIndex = ReverseTranslate(i, fieldIndex, targetIndex);
                        ref var srcFh = ref FieldHeader.FromSpanAndFieldIndex(oldSpan, srcIndex);
                        newFh = srcFh;
                    }

                    // Adjust offsets for the new layout
                    newFh.NameOffset = nameOffset;
                    newFh.DataOffset += nameLengthByteDelta;

                    nameOffset += newFh.NameLength * sizeof(char);
                    dataOffset += newFh.Length;
                }

                // 5) Write names and data according to new headers 
                for (int i = 0; i < fieldCount; i++)
                {
                    ref var newFh = ref FieldHeader.FromSpanAndFieldIndex(newSpan, i);
                    Span<byte> dstName = newMemory.AsSpan(newFh.NameOffset, newFh.NameLength * sizeof(char));
                    Span<byte> dstData = newMemory.AsSpan(newFh.DataOffset, newFh.Length);

                    if (i == targetIndex)
                    {
                        // new name bytes
                        newNameBytes.CopyTo(dstName);
                        // data from the originally moved field
                        var srcData = GetFieldData(in fieldHeader);
                        srcData.CopyTo(dstData);
                    }
                    else
                    {
                        // Determine source header index mapping
                        int srcIndex = ReverseTranslate(i, fieldIndex, targetIndex);

                        ref var srcFh = ref GetFieldHeader(srcIndex);
                        var srcName = GetFieldName(in srcFh);
                        MemoryMarshal.AsBytes(srcName).CopyTo(dstName);

                        var srcData = GetFieldData(in srcFh);
                        srcData.CopyTo(dstData);
                    }
                }
                ChangeContent(newMemory);
            }
            catch (Exception)
            {
                newMemory.Dispose();
                throw;
            }
            oldMemory.Dispose();

            static int Translate(int old, int src, int dst)
            {
                int min = Math.Min(src, dst);
                int max = Math.Max(src, dst);
                if (old == src)
                    return dst;
                if (old < min || old > max)
                    return old;
                else if (src < dst)
                    return old - 1; // shift left
                else
                    return old + 1; // shift right
            }

            static int ReverseTranslate(int n, int src, int dst)
            {
                int min = Math.Min(src, dst);
                int max = Math.Max(src, dst);
                if (n == dst)
                    return src;
                if (n < min || n > max)
                    return n;
                else if (src < dst)
                    return n + 1; // shift right
                else
                    return n - 1; // shift left
            }
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
            ContainerLayout newLayout = ObjectBuilder.FromContainer(this).Variate(edit).BuildLayout();
            Rescheme(newLayout);  // zero-init by default
        }

        /// <summary>
        /// Internal overload allowing to skip zero-initialization when the caller
        /// will fully overwrite the new buffer manually.
        /// </summary>  
        /// <remarks>
        /// Invoke all necessary events manually if <paramref name="quiet"/> is false.
        /// </remarks>
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
            try
            {
                var dst = dstBuf.Buffer.Span;
                dst.Clear();

                newLayout.WriteTo(dst, _memory.AsSpan(header.ContainerNameOffset, header.ContainerNameLength));
                ref ContainerHeader newContainerHeader = ref Unsafe.As<byte, ContainerHeader>(ref dst[0]);
                newContainerHeader.Version = header.Version; // preserve version
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
            }
            catch (Exception)
            {
                dstBuf.Dispose();
                throw;
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

        /// <summary>
        /// Rescheme for a field, will not invoke events.
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="elementType"></param>
        /// <param name="inlineArrayLength"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeFor(ReadOnlySpan<char> fieldName, TypeData elementType, int? inlineArrayLength)
        {
            using UnregisterBuffer unregisterBuffer = UnregisterBuffer.New(this);
            int index = ReschemeFor(fieldName, elementType, inlineArrayLength, unregisterBuffer);
            unregisterBuffer.Send();
            return index;
        }

        public int ReschemeFor(ReadOnlySpan<char> fieldName, TypeData elementType, int? inlineArrayLength, UnregisterBuffer unregisterBuffer)
        {
            int index = IndexOf(fieldName);
            int elementCount = inlineArrayLength ?? 1;
            ValueType valueType = elementType.ValueType;
            int elementSize = elementType.Size;

            bool isNewField = index < 0;
            int targetIndex = isNewField ? ~index : index;
            int newDataLength = elementSize * elementCount;
            ref var currentHeader = ref Header;
            int newLength = isNewField
                // new field, add header, name, data length
                ? currentHeader.Length + FieldHeader.Size + fieldName.Length * sizeof(char) + newDataLength
                // already exist, then no header size change, no name change, only data size change
                : currentHeader.Length + newDataLength; // (have not reduce the old field data size yet here)
            FieldType newFieldType = new(valueType, inlineArrayLength.HasValue);

            if (!isNewField)
            {
                ref var existedHeader = ref GetFieldHeader(index);
                newLength -= existedHeader.Length; // reduced
                // no rescheme needed (exist, same type, same inline length)
                if (existedHeader.FieldType == newFieldType && existedHeader.ElementCount == elementCount)
                {
                    return index;
                }
                // length is fine, just need to reset header
                if (existedHeader.Length >= newDataLength)
                {
                    if (!existedHeader.IsRef)
                    {
                        Span<byte> old = GetFieldData(existedHeader);
                        using var temp = AllocatedMemory.Create(old);
                        var oldFieldType = existedHeader.FieldType;
                        existedHeader.Length = newDataLength;
                        existedHeader.FieldType = newFieldType;
                        existedHeader.ElemSize = (short)elementSize;
                        Span<byte> newSpan = GetFieldData(existedHeader);
                        Migration.MigrateValueFieldBytes(temp.Buffer.Span, newSpan, oldFieldType.Type, newFieldType.Type, true);
                    }
                    else
                    {
                        var old = GetFieldData<ContainerReference>(existedHeader);
                        existedHeader.Length = newDataLength;
                        existedHeader.FieldType = newFieldType;
                        existedHeader.ElemSize = (short)elementSize;
                        unregisterBuffer.Add(old, existedHeader.IsInlineArray);
                        GetFieldData(existedHeader).Clear();
                    }
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
                        if (!isNewField)
                        {
                            ref FieldHeader currentFieldHeader = ref GetFieldHeader(j++);
                            // check object unregister
                            if (currentFieldHeader.IsRef)
                            {
                                var rs = GetFieldData<ContainerReference>(in currentFieldHeader);
                                unregisterBuffer.Add(rs, currentFieldHeader.IsInlineArray);
                            }
                        }
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

        public void ResizeArrayField(int index, int newLength, UnregisterBuffer unregisterBuffer)
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
                ReschemeFor(name, fieldHeader.ElementType, newLength, unregisterBuffer);
            }
        }

        public void ReschemeForArray(int length, TypeData type)
        {
            ref ContainerHeader oldHeader = ref Header;
            ValueType valueType = type.ValueType;
            ReadOnlySpan<byte> nameBytes;
            if (oldHeader.FieldCount == 1)
            {
                ref var oldField = ref GetFieldHeader(0);
                nameBytes = MemoryMarshal.AsBytes(GetFieldName(in oldField));
                // already same type and length
                if (oldField.Type == valueType && oldField.ElementCount == length)
                    return;
            }
            else
            {
                nameBytes = MemoryMarshal.AsBytes(ContainerLayout.ArrayName.AsSpan());
            }

            int elementSize = type.Size;
            int dataOffset = ContainerHeader.Size
                + FieldHeader.Size
                + oldHeader.ContainerNameLength
                + nameBytes.Length;
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
            fieldHeader.NameLength = (short)(nameBytes.Length / sizeof(char));
            fieldHeader.NameOffset = newHeader.ContainerNameOffset + newHeader.ContainerNameLength;
            fieldHeader.DataOffset = dataOffset;
            fieldHeader.Length = dataLength;
            fieldHeader.FieldType = new FieldType(valueType, true);
            fieldHeader.ElemSize = (short)elementSize;
            // write array field name
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
