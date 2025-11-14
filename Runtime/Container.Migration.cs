using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Minerva.DataStorage.TypeUtil;

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
        public void Rescheme(ContainerLayout newSchema, bool zeroInitNewBuffer = true)
        {
            if (newSchema is null) throw new ArgumentNullException(nameof(newSchema));
            EnsureNotDisposed();

            // same header
            if (HeadersSegment.SequenceEqual(newSchema.Span))
                return;

            // Prepare destination buffer
            byte[] dstBuf = DefaultPool.Rent(newSchema.TotalLength);
            var dst = dstBuf.AsSpan();
            dst.Clear();
            newSchema.Span.CopyTo(dst);

            // Prepare new header hints (migrate by name)            
            ContainerView newView = new ContainerView(dst);
            ContainerView oldView = View;

            // ---- Migrate each old field into new layout ----
            for (int oldIdx = 0; oldIdx < oldView.FieldCount; oldIdx++)
            {
                var oldFieldView = oldView[oldIdx];

                var newIdx = newView.IndexOf(oldFieldView.Name);
                if (newIdx < 0)
                {
                    // Removed: if ref, unregister all non-zero children
                    if (oldFieldView.IsRef)
                    {
                        var oldIds = oldFieldView.GetSpan<ContainerReference>();
                        for (int j = 0; j < oldIds.Length; j++)
                            Registry.Shared.Unregister(ref oldIds[j]);
                    }
                    continue;
                }

                ref var newField = ref newView.GetFieldHeader(newIdx);
                var dstBytes = newView.GetFieldBytes(newIdx); // NEW buffer slice


                // diff in ref/non-ref
                if (oldFieldView.IsRef != newField.IsRef)
                {
                    // Removed: if ref, unregister all non-zero children
                    if (oldFieldView.IsRef)
                    {
                        var oldIds = oldFieldView.GetSpan<ContainerReference>();
                        for (int j = 0; j < oldIds.Length; j++)
                            Registry.Shared.Unregister(ref oldIds[j]);
                    }
                    dstBytes.Clear();
                    continue;
                }
                if (!oldFieldView.IsRef)
                {
                    // truely same type
                    // value type the same, but is array/non array, only diff in length (arr or non arr)
                    if (oldFieldView.Type == newField.Type)
                    {
                        var srcBytes = oldFieldView.Data;                 // OLD buffer read-only
                        // copy as much as possible
                        int min = Math.Min(srcBytes.Length, dstBytes.Length);
                        srcBytes[..min].CopyTo(dstBytes[..min]);
                        continue;
                    }
                }
                else
                {
                    // Ref-to-Ref migration unchanged (copy ids, unregister tails)
                    var oldIds = oldFieldView.GetSpan<ContainerReference>();
                    var newIds = MemoryMarshal.Cast<byte, ContainerReference>(dstBytes);

                    int min = Math.Min(oldIds.Length, newIds.Length);
                    for (int i = 0; i < min; i++) newIds[i] = oldIds[i];
                    for (int i = min; i < oldIds.Length; i++) Registry.Shared.Unregister(ref oldIds[i]);
                }
            }

            // ---- Swap schema & buffers ----
            var oldBuf = _buffer;
            _buffer = dstBuf;
            if (oldBuf.Length > 0 && !ReferenceEquals(oldBuf, Array.Empty<byte>()))
                DefaultPool.Return(oldBuf);
        }




        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeForNew<T>(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) where T : unmanaged => ReschemeForField_Internal(fieldName, TypeUtil.PrimOf<T>(), inlineArrayLength);

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeForNewObject(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) => ReschemeForField_Internal(fieldName, ValueType.Ref, inlineArrayLength);

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReschemeFor<T>(ReadOnlySpan<char> fieldName, int? inlineArrayLength = null) where T : unmanaged => ReschemeForField_Internal(fieldName, TypeUtil.PrimOf<T>(), inlineArrayLength);

        private int ReschemeForField_Internal(ReadOnlySpan<char> fieldName, ValueType valueType, int? inlineArrayLength = null)
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
                    objectBuilder.SetArray(tempName, new FieldType(valueType, true), inlineArrayLength.Value);
                }
                else objectBuilder.SetScalar(tempName, new FieldType(valueType, false));

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
                byte[] newBuffer = DefaultPool.Rent(newSize);
                objectBuilder.WriteTo(ref newBuffer);

                // switch buffer now
                var oldBuffer = this._buffer;
                this._buffer = newBuffer;
                DefaultPool.Return(oldBuffer);

                return IndexOf(tempName.Span);
            }
            finally
            {
                ArrayPool<char>.Shared.Return(fieldNameBuffer);
                ArrayPool<char>.Shared.Return(nameBuffer);
            }
        }







        /// <summary>
        /// Ensure that 'fieldName' exists as a non-ref value field sized to sizeof(T).
        /// If it exists with same size but different type hint, convert the in-place bytes and update the hint,
        /// without rescheming. If it doesn't exist or size mismatches, rebuild schema to replace the field.
        /// </summary>
        //public void EnsureFieldForRead<T>(int index) where T : unmanaged
        //{
        //    ref var header = ref GetFieldHeader(index);
        //    FieldType fieldType = header.FieldType;
        //    FieldType targetType = FieldType.Of<T>(fieldType.IsInlineArray);
        //    ValueType valueType = fieldType.Type;
        //    ValueType target = targetType.Type;

        //    // is same type
        //    if (valueType == target)
        //        return;

        //    // if we don't know current type, assume new type is valid
        //    if (valueType == ValueType.Unknown)
        //    {
        //        header.FieldType.Type = target;
        //        return;
        //    }
        //    Migrate<T>(index, target);
        //}

        public bool Migrate<T>(int index, ValueType target) where T : unmanaged
        {
            ref var fieldHeader = ref GetFieldHeader(index);
            bool isArray = fieldHeader.IsInlineArray;
            ValueType valueType = fieldHeader.Type;


            //var field = View[index];
            // 2) Existing but ref �� not allowed
            if (fieldHeader.IsRef)
                throw new InvalidOperationException($"Field '{GetFieldName(in fieldHeader).ToString()}' is a reference; cannot read value T={typeof(T).Name}.");

            int oldElementSize = TypeUtil.SizeOf(valueType);
            int newElementSize = TypeUtil.SizeOf(PrimOf<T>());
            int dataLength = (int)fieldHeader.Length;
            int arrayLength = isArray ? dataLength / oldElementSize : 1;
            Span<byte> fieldData = GetFieldData(in fieldHeader);

            // inplace conversion, given element same size
            if (oldElementSize == newElementSize)
            {
                if (isArray) Migration.ConvertArrayInPlaceSameSize(fieldData, arrayLength, valueType, target);
                else Migration.ConvertScalarInPlace(fieldData, valueType, target);
                fieldHeader.FieldType = Pack(target, isArray);
            }
            // different size
            else
            {
                // copy old data
                ReadOnlySpan<char> fieldName = GetFieldName(in fieldHeader);
                int nameLength = fieldName.Length * sizeof(byte);
                int minimumLength = nameLength + dataLength;
                byte[] array = ArrayPool<byte>.Shared.Rent(minimumLength);
                try
                {
                    Span<char> name = MemoryMarshal.Cast<byte, char>(array.AsSpan(0, nameLength));
                    Span<byte> buffer = array.AsSpan(nameLength);
                    // store name + data
                    fieldName.CopyTo(name);
                    fieldData.CopyTo(buffer);

                    // rescheme
                    //Rescheme(b =>
                    //{
                    //    b.RemoveField(field.Name);
                    //    if (isArray) b.AddArrayOf<T>(field.Name, arrayLength);
                    //    else b.AddFieldOf<T>(field.Name);
                    //});
                    ReschemeFor<T>(fieldName, isArray ? arrayLength : null);

                    var newIndex = IndexOf(name);
                    ref var newField = ref GetFieldHeader(newIndex);// View[newIndex];
                    var newSpan = GetFieldData(in newField);
                    // fill back
                    Migration.MigrateValueFieldBytes(buffer, newSpan, valueType, target, true);
                    newField.FieldType = new(target, isArray);
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
            ValueType srcType = TypeUtil.PrimOf<T>();
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
                if (Unsafe.SizeOf<T>() > dstSpan.Length)
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
            var readType = TypeUtil.PrimOf<T>();
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
            var srcType = TypeUtil.PrimOf<T>();

            // conversion
            var srcSpan = MemoryMarshal.Cast<T, byte>(MemoryMarshal.CreateSpan(ref value, 1));
            var view = new ReadOnlyValueView(srcSpan, srcType);
            return view.TryWriteTo(dstSpan, dstType, true);
        }
    }
}
