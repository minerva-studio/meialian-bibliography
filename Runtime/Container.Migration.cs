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
            if (View.HeadersSegment.SequenceEqual(newSchema.Span))
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
                View.NameSegment.CopyTo(MemoryMarshal.AsBytes(nameBuffer.AsSpan()));
                fieldName.CopyTo(fieldNameBuffer);
                Memory<char> tempName = fieldNameBuffer.AsMemory(0, fieldName.Length);
                if (inlineArrayLength.HasValue)
                {
                    objectBuilder.SetArray(tempName, new FieldType(valueType, true), inlineArrayLength.Value);
                }
                else objectBuilder.SetScalar(tempName, new FieldType(valueType, false));

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
                objectBuilder.WriteTo(ref newBuffer);
                // switch buffer now
                var oldBuffer = this._buffer;
                this._buffer = newBuffer;
                DefaultPool.Return(oldBuffer);
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
            FieldType targetType = FieldType.Of<T>(fieldType.IsInlineArray);
            ValueType valueType = fieldType.Type;
            ValueType target = targetType.Type;

            // is same type
            if (valueType == target)
                return;

            // if we don't know current type, assume new type is valid
            if (valueType == ValueType.Unknown)
            {
                View.Fields[index].FieldType = Pack(target, fieldType.IsInlineArray);
                return;
            }
            Migrate<T>(index, target);
        }

        public bool Migrate<T>(int index, ValueType target) where T : unmanaged
        {
            FieldType fieldType = View.Fields[index].FieldType;
            bool isArray = fieldType.IsInlineArray;
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
                if (isArray) Migration.ConvertArrayInPlaceSameSize(data, arrayLength, valueType, target);
                else Migration.ConvertScalarInPlace(data, valueType, target);
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
                    Migration.MigrateValueFieldBytes(buffer, newSpan, valueType, target, true);
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

                field.Header.FieldType.Type = srcType;
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
            var view = new ReadOnlyValueView(srcSpan, srcType);
            return view.TryWriteTo(dstSpan, dstType);
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
            var view = new ReadOnlyValueView(srcSpan, srcType);
            return view.TryWriteTo(dstSpan, dstType, true);
        }
    }
}
