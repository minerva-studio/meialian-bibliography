using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Minerva.DataStorage.StorageArrayExtension;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Abstraction view of any storage array
    /// </summary>
    public ref struct StorageArray
    {
        private FieldHandle _handle;

        /// <summary>
        /// Create an object array view
        /// </summary>
        /// <param name="container"></param>
        /// <param name="fieldIndex"></param>
        internal StorageArray(Container container)
        {
            var name = container.GetFieldName(0);
            if (name.SequenceEqual(ContainerLayout.ArrayName))
                this._handle = new FieldHandle(container, ContainerLayout.ArrayName);
            else
                this._handle = new FieldHandle(container, name.ToString()); // have to create a new string then
        }

        internal StorageArray(Container container, ReadOnlySpan<char> fieldName)
        {
            // Determine if we might lose the name when schema changed. 
            this._handle = new FieldHandle(container, fieldName);
        }

        public StorageArray(FieldHandle fieldIndex) : this()
        {
            this._handle = fieldIndex;
        }

        public readonly bool IsDisposed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _handle.Container == null || _handle.IsDisposed || _handle.Index < 0;
        }

        /// <summary>
        /// Is an external array? (not inline to the parent object)
        /// </summary>
        public readonly bool IsExternalArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _handle.EnsureNotDisposed();
                return _handle.Container.IsArray;
            }
        }

        /// <summary>
        /// Is an object array? (a ref array)
        /// </summary>
        public readonly bool IsObjectArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _handle.EnsureNotDisposed();
                return Header.Type == ValueType.Ref;
            }
        }

        /// <summary>
        /// Is a string? (a ref array of char16)
        /// </summary>
        public readonly bool IsString
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _handle.EnsureNotDisposed();
                return Header.Type == ValueType.Char16;
            }
        }

        /// <summary>
        /// Array Length
        /// </summary>
        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _handle.EnsureNotDisposed();
                ref var header = ref Header;
                return header.ElementCount;
            }
        }

        public readonly ValueType Type
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _handle.EnsureNotDisposed();
                return Header.Type;
            }
        }

        public readonly TypeData ElementType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _handle.EnsureNotDisposed();
                return Header.ElementType;
            }
        }

        public readonly FieldType FieldType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _handle.EnsureNotDisposed();
                return Header.FieldType;
            }
        }

        internal readonly FieldHandle Handle
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _handle;
        }

        internal readonly ref FieldHeader Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref _handle.Container.GetFieldHeader(_handle.Index);
        }

        internal readonly ScalarView Scalar => new(in _handle);

        internal WriteView Raw => new(ref this);




        /// <summary>
        /// Value
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public readonly StorageMember this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(_handle, index);
        }



        /// <summary>Direct access to the underlying ID span (use with care).</summary>
        internal Span<ContainerReference> References
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                ref FieldHeader header = ref Header;
                return _handle.Container.GetFieldData<ContainerReference>(in header);
            }
        }

        public bool Write<T>(int index, T value) where T : unmanaged
        {
            var src = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref value, 1));
            return Write(index, src, TypeUtil<T>.ValueType);
        }

        public bool Write(int index, Span<byte> src, ValueType valueType)
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            int elementSize = header.ElemSize;
            var span = _handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
            bool result = Migration.TryWriteTo(src, valueType, span, header.Type, true);
            if (result)
                StorageEventRegistry.NotifyFieldWrite(_handle.Container, fieldIndex);
            return result;
        }

        public void Write(string value)
        {
            ThrowHelper.ThrowIfNull(value, nameof(value));
            ReadOnlySpan<char> valueSpan = value.AsSpan();
            Write(valueSpan);
        }

        public void Write(ReadOnlySpan<char> valueSpan)
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            if (Header.Type != ValueType.Char16)
                ThrowHelper.ArgumentException("Cannot write string to non-string array.");

            // just call write array on object
            if (this._handle.Container.IsArray)
            {
                new StorageObject(_handle.Container).WriteArray(valueSpan);
                return;
            }

            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            int length = valueSpan.Length;

            // no event would occur for this invoke
            _handle.Container.ReschemeFor(_handle.Name, TypeUtil<char>.Type, length, default);

            header = ref _handle.Container.GetFieldHeader(fieldIndex);
            var span = _handle.Container.GetFieldData(in header);
            MemoryMarshal.AsBytes(valueSpan).CopyTo(span);

            StorageEventRegistry.NotifyFieldWrite(_handle.Container, fieldIndex);
        }

        public bool TryWrite<T>(int index, T value, bool isExplicit = true) where T : unmanaged
        {
            var src = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref value, 1));
            return TryWrite(index, src, TypeUtil<T>.ValueType, isExplicit);
        }

        public bool TryWrite(int index, Span<byte> src, ValueType valueType, bool isExplicit = true)
        {
            if (IsDisposed)
                return false;
            ref FieldHeader header = ref _handle.Header;
            if (index < 0 || index > header.ElementCount)
                return false;
            int elementSize = header.ElemSize;
            var span = _handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
            bool result = Migration.TryWriteTo(src, valueType, span, header.Type, isExplicit);
            if (result)
                StorageEventRegistry.NotifyFieldWrite(_handle.Container, _handle.Index);
            return result;
        }


        public readonly T Read<T>(int index) where T : unmanaged => Scalar[index].Read<T>();

        public readonly bool TryRead<T>(int index, out T value) where T : unmanaged
        {
            value = default;
            if (IsDisposed)
                return false;
            if (index < 0 || index > Header.ElementCount)
                return false;
            return Scalar[index].TryRead(out value, true);
        }






        /// <summary> Get a child as a StorageObject </summary>
        public StorageObject GetObject(int index)
        {
            ref ContainerReference reference = ref References[index];
            return reference.TryGet(out var obj) ? obj : CreateObject(ref reference, index, ContainerLayout.Empty);
        }

        /// <summary> Get a child as a StorageObject.</summary>
        public StorageObject GetObject(int index, ContainerLayout layout)
        {
            ref ContainerReference reference = ref References[index];
            if (layout == null) return StorageObjectFactory.GetNoAllocate(reference);
            return reference.TryGet(out var obj) ? obj : CreateObject(ref reference, index, layout);
        }

        private readonly StorageObject CreateObject(ref ContainerReference reference, int index, ContainerLayout layout)
        {
            using var tempString = TempString.Create(_handle.Name);
            tempString.Append('[');
            Span<char> span = stackalloc char[11];
            if (!index.TryFormat(span, out var len))
                ThrowHelper.ArgumentException(nameof(index));
            tempString.Append(span[..len]);
            tempString.Append(']');
            return StorageObjectFactory.GetOrCreate(ref reference, _handle.Container, layout, tempString);
        }

        /// <summary> Get a child as a StorageObject.</summary>
        public StorageObject GetObjectNoAllocate(int index)
        {
            return References[index].GetNoAllocate();
        }

        /// <summary>Try get a child; returns false if slot is 0 or container is missing.</summary>
        public bool TryGetObject(int index, out StorageObject child, bool instantiate = false)
        {
            Span<ContainerReference> references = References;
            if (references.Length <= index)
            {
                child = default;
                return false;
            }

            if (instantiate) return !(child = references[index].TryGet(out var obj) ? obj : CreateObject(ref references[index], index, ContainerLayout.Empty)).IsNull;
            return references[index].TryGet(out child);
        }

        /// <summary>
        /// Retrieves the value of type <typeparamref name="T"/> at the specified index from the scalar field.
        /// </summary>
        /// <remarks>This method throws an exception if the field is a reference field. The caller must
        /// ensure that the index is within the valid range of the scalar field.</remarks>
        /// <typeparam name="T">The unmanaged value type to retrieve from the field.</typeparam>
        /// <param name="index">The zero-based index of the value to retrieve within the scalar field.</param>
        /// <returns>The value of type <typeparamref name="T"/> at the specified index.</returns>
        public T GetValue<T>(int index) where T : unmanaged
        {
            ref FieldHeader header = ref Header;
            if (header.IsRef)
                ThrowHelper.ThrowInvalidOperation("Cannot get value from a ref field.");
            return this.Scalar[index].Read<T>();
        }

        /// <summary>
        /// Sets the value of the field at the specified index.
        /// </summary>
        /// <remarks>Throws an exception if the field at the specified index is a reference field. This
        /// method notifies listeners of the field write operation after the value is set.</remarks>
        /// <typeparam name="T">The type of value to set. Must be an unmanaged type.</typeparam>
        /// <param name="index">The zero-based index of the field to set.</param>
        /// <param name="value">The value to assign to the field at the specified index.</param>
        public void SetValue<T>(int index, T value) where T : unmanaged
        {
            ref FieldHeader header = ref Header;
            if (header.IsRef)
                ThrowHelper.ThrowInvalidOperation("Cannot set value to a ref field.");
            this.Raw[index].Write(value);
            StorageEventRegistry.NotifyFieldWrite(_handle.Container, _handle.Index);
        }

        /// <summary>
        /// Retrieves the storage member at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the member to retrieve. Must be within the valid range of available members.</param>
        /// <returns>A <see cref="StorageMember"/> representing the member at the specified index.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly StorageMember GetMember(int index) => new(_handle, index);

        /// <summary>
        /// Creates a persistent member representation for the specified field index.
        /// </summary>
        /// <param name="index">The zero-based index of the field to retrieve. Must be greater than or equal to zero and within the bounds
        /// of the available fields.</param>
        /// <returns>A <see cref="StorageMember.Persistent"/> instance representing the persistent member at the specified index.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly StorageMember.Persistent GetPersistentMember(int index) => new(new FieldHandle.Persistent(_handle), index);








        public int CopyFrom<T>(ReadOnlySpan<T> values) where T : unmanaged
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);

            int length = Math.Min(header.ElementCount, values.Length);

            Span<byte> data = _handle.Container.GetFieldData(in header);
            ValueType dstType = header.FieldType.Type;
            ValueType srcType = TypeUtil<T>.ValueType;
            int i;
            for (i = 0; i < length; i++)
            {
                ReadOnlySpan<byte> src = MemoryMarshal.Cast<T, byte>(values[i..(i + 1)]);
                var dst = data.Slice(header.ElemSize * i, header.ElemSize);
                Migration.TryWriteTo(src, srcType, dst, dstType, false);
            }
            return i;
        }

        /// <summary>
        /// Override content by given value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="values"></param>
        /// <param name="allowResize">if array length is less than given size, can we resize the array?</param> 
        public int CopyFrom<T>(ReadOnlySpan<T> values, bool allowResize = true, bool isExplicit = true) where T : unmanaged
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            using var unregisterBuffer = UnregisterBuffer.New(_handle.Container);

            int length = header.ElementCount;
            if (values.Length > length)
            {
                if (allowResize)
                {
                    _handle.Container.ResizeArrayField(_handle.Index, values.Length);
                    header = ref _handle.Container.GetFieldHeader(fieldIndex);
                    length = header.ElementCount;
                }
            }

            Span<byte> data = _handle.Container.GetFieldData(in header);
            ValueType dstType = header.FieldType.Type;
            ValueType srcType = TypeUtil<T>.ValueType;
            if (!Migration.MigrateValueFieldBytes(MemoryMarshal.AsBytes(values), data, srcType, dstType, isExplicit, zeroFillRemaining: false))
                return 0;

            unregisterBuffer.Send();
            StorageEventRegistry.NotifyFieldWrite(_handle.Container, fieldIndex);
            return Math.Min(values.Length, length);
        }

        /// <summary>
        /// Set array content to given values
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="values"></param>
        public void Override<T>(ReadOnlySpan<T> values) where T : unmanaged
        {
            using var unregisterBuffer = UnregisterBuffer.New(_handle.Container);

            _handle.Container.ReschemeFor(_handle.Name, TypeUtil<T>.Type, values.Length, unregisterBuffer);
            int fieldIndex = _handle.EnsureNotDisposed();
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            ReadOnlySpan<byte> src = MemoryMarshal.Cast<T, byte>(values);
            Span<byte> dst = _handle.Container.GetFieldData(in header);
            src.CopyTo(dst);

            unregisterBuffer.Send();
            StorageEventRegistry.NotifyFieldWrite(_handle.Container, fieldIndex);
        }





        /// <summary>Clear the slot (set ID to 0).</summary>
        public void ClearAt(int index)
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref var header = ref _handle.Container.GetFieldHeader(fieldIndex);

            if (!header.IsRef)
            {
                int elementSize = header.ElemSize;
                var span = _handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
                span.Clear();
            }
            else
            {
                _handle.Container.GetFieldData<ContainerReference>(in header)[index].Unregister();
            }
            StorageEventRegistry.NotifyFieldWrite(_handle.Container, _handle.Name.ToString(), FieldType);
        }

        /// <summary>Clear all bytes (zero-fill).</summary>
        public void Clear()
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref var header = ref _handle.Container.GetFieldHeader(fieldIndex);
            if (header.IsRef)
            {
                Span<ContainerReference> ids = _handle.Container.GetFieldData<ContainerReference>(in header);
                using UnregisterBuffer buffer = UnregisterBuffer.New(_handle.Container);
                buffer.AddArray(ids);
                ids.Clear();
                buffer.Send();
            }
            else
            {
                _handle.Container.GetFieldData(in header).Clear();
            }
            StorageEventRegistry.NotifyFieldWrite(_handle.Container, _handle.Name.ToString(), FieldType);
        }

        /// <summary>
        /// Resize the array
        /// </summary>
        /// <param name="newLength"></param>
        public void Resize(int newLength)
        {
            _handle.EnsureNotDisposed();
            if (newLength == _handle.Header.ElementCount)
                return;
            int index = _handle.Index;
            _handle.Container.ResizeArrayField(index, newLength);
            StorageEventRegistry.NotifyFieldWrite(_handle.Container, index);
        }

        /// <summary>
        /// Resize the array
        /// </summary>
        /// <param name="length"></param>
        public void EnsureLength(int length)
        {
            _handle.EnsureNotDisposed();
            if (length <= _handle.Header.ElementCount)
                return;
            _handle.Container.ResizeArrayField(_handle.Index, length);
            StorageEventRegistry.NotifyFieldWrite(_handle.Container, _handle.Index);
        }

        /// <summary>
        /// Rescheme array to match type
        /// </summary>
        /// <param name="type"></param>
        /// <param name="newLength"></param>
        public void Rescheme(TypeData type, int? newLength = null)
        {
            _handle.EnsureNotDisposed();
            newLength ??= Header.ElementCount;
            _handle.Container.ReschemeFor(_handle.Name, type, newLength);
            StorageEventRegistry.NotifyFieldWrite(_handle.Container, _handle.Index);
        }

        /// <summary>
        /// Can array be treated as typeof T?
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool IsConvertibleTo<T>() where T : unmanaged => AcceptTypeConversion<T>(in Header);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool IsConvertibleTo(TypeData type) => AcceptTypeConversion(in Header, type);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool IsConvertibleTo(ValueType valueType, int? elementSize = null) => AcceptTypeConversion(in Header, valueType, elementSize);




        public T[] ToArray<T>() where T : unmanaged
        {
            // 1) Disallow ref fields for value extraction.
            ref var header = ref Header;
            if (header.IsRef)
                throw new InvalidOperationException("Cannot call ToArray<T>() on a ref field. Use object accessors instead.");

            return StorageArrayExtension.ToArray<T>(in header, _handle.Container);
        }

        public string AsString()
        {
            // 1) Disallow ref fields for value extraction.
            ref var header = ref Header;
            Container container = _handle.Container;
            return StorageArrayExtension.AsString(header, container);
        }

        /// <summary>
        /// Convert to persistent representation
        /// </summary>
        /// <param name="array"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Persistent ToPersistent() => new(this);

        /// <summary>
        /// Convert to string for display
        /// </summary>
        /// <remarks>
        /// For a char16 array, returns the string representation.
        /// </remarks>
        /// <returns></returns>
        public override string ToString()
        {
            _handle.EnsureNotDisposed();
            ref var header = ref _handle.Header;
            if (header.Type == ValueType.Char16)
            {
                return AsString();
            }
            return $"{TypeUtil.ToString(header.Type)}[{header.ElementCount}]";
        }


        public static explicit operator Persistent(StorageArray arr) => new Persistent(arr);



        public Enumerator GetEnumerator() => new(ref this);

        public ref struct Enumerator
        {
            private readonly StorageArray _array;
            private readonly int _schemaVersion;
            private int _index;

            public Enumerator(ref StorageArray array)
            {
                _array = array;
                _schemaVersion = array._handle.Container.SchemaVersion;
                _index = -1;
            }

            public bool MoveNext()
            {
                if (_schemaVersion != _array._handle.Container.SchemaVersion)
                    ThrowHelper.ThrowInvalidOperation("Collection was modified; enumeration operation may not execute.");
                if (_array.IsDisposed)
                    ThrowHelper.ThrowDisposed("The collection has been disposed.");
                _index++;
                return _index < _array.Header.ElementCount;
            }

            public readonly StorageMember Current => _array[_index];
        }

        public struct Persistent : IEnumerable<StorageMember.Persistent>
        {
            internal FieldHandle.Persistent _handle;

            public Persistent(FieldHandle.Persistent handle) => _handle = handle;
            public Persistent(StorageArray member) => _handle = new FieldHandle.Persistent(member._handle);

            public readonly StorageArray Array
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new(_handle);
            }
            public readonly bool IsDisposed
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _handle.Container == null || _handle.IsDisposed || _handle.Index < 0;
            }
            public readonly int Length
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    _handle.EnsureNotDisposed();
                    return _handle.Header.ElementCount;
                }
            }
            internal readonly FieldHandle Handle
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _handle;
            }

            public readonly StorageMember.Persistent this[int index]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new(_handle, index);
            }

            public readonly Enumerator GetEnumerator() => new(this);
            readonly IEnumerator<StorageMember.Persistent> IEnumerable<StorageMember.Persistent>.GetEnumerator() => new Enumerator(this);
            readonly IEnumerator IEnumerable.GetEnumerator() => new Enumerator(this);


            /// <summary>
            /// Retrieves the storage member at the specified index.
            /// </summary>
            /// <param name="index">The zero-based index of the member to retrieve. Must be within the valid range of available members.</param>
            /// <returns>A <see cref="StorageMember"/> representing the member at the specified index.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly StorageMember GetMember(int index) => new(_handle, index);

            /// <summary>
            /// Creates a persistent member representation for the specified field index.
            /// </summary>
            /// <param name="index">The zero-based index of the field to retrieve. Must be greater than or equal to zero and within the bounds
            /// of the available fields.</param>
            /// <returns>A <see cref="StorageMember.Persistent"/> instance representing the persistent member at the specified index.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly StorageMember.Persistent GetPersistentMember(int index) => new(_handle, index);


            public static implicit operator StorageArray(Persistent member) => new(member._handle);

            public struct Enumerator : IEnumerator<StorageMember.Persistent>, IEnumerator
            {
                private readonly Persistent _array;
                private readonly int _schemaVersion;
                private int _index;

                public Enumerator(Persistent array)
                {
                    _array = array;
                    _schemaVersion = array._handle.Container.SchemaVersion;
                    _index = -1;
                }

                public bool MoveNext()
                {
                    if (_schemaVersion != _array._handle.Container.SchemaVersion)
                        ThrowHelper.ThrowInvalidOperation("Collection was modified; enumeration operation may not execute.");
                    if (_array.IsDisposed)
                        ThrowHelper.ThrowDisposed("The collection has been disposed.");
                    _index++;
                    return _index < _array._handle.Header.ElementCount;
                }

                public readonly StorageMember Current => new(_array._handle, _index);
                readonly StorageMember.Persistent IEnumerator<StorageMember.Persistent>.Current => new(_array._handle, _index);
                readonly object IEnumerator.Current => new StorageMember.Persistent(_array._handle, _index);

                public readonly void Dispose() { }

                public void Reset()
                {
                    _index = -1;
                }
            }
        }


        /// <summary>
        /// No notification on write access
        /// </summary>
        internal readonly ref struct WriteView
        {
            private readonly StorageArray _arr;
            public ValueView this[int index]
            {
                readonly get
                {
                    ref FieldHeader header = ref _arr.Header;
                    int elementSize = header.ElemSize;
                    var span = _arr._handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
                    return new ValueView(span, header.Type);
                }
                set
                {
                    ref FieldHeader header = ref _arr.Header;
                    int elementSize = header.ElemSize;
                    var span = _arr._handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
                    value.TryWriteTo(span, header.Type);
                }
            }

            public WriteView(ref StorageArray arr)
            {
                _arr = arr;
            }
        }

        public ref struct ScalarView
        {
            private FieldHandle _handle;

            public ScalarView(in FieldHandle handle)
            {
                _handle = handle;
            }

            /// <summary>
            /// Value
            /// </summary>
            /// <param name="index"></param>
            /// <returns></returns>
            public ReadOnlyValueView this[int index]
            {
                get
                {
                    ref FieldHeader header = ref _handle.Header;
                    int elementSize = header.ElemSize;
                    var span = _handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
                    return new ValueView(span, header.Type);
                }
                set
                {
                    int fieldIndex = _handle.EnsureNotDisposed();
                    ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
                    int elementSize = header.ElemSize;
                    var span = _handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
                    value.TryWriteTo(span, header.Type);

                    StorageEventRegistry.NotifyFieldWrite(_handle.Container, fieldIndex);
                }
            }
        }
    }


    public static class StorageArrayExtension
    {
        internal static T[] ToArray<T>(in FieldHeader header, Container container) where T : unmanaged
        {
            int length = header.Length / header.ElemSize;
            var result = new T[length];

            var dstType = TypeUtil<T>.ValueType;

            Span<byte> data = container.GetFieldData(in header);
            ValueType type = header.FieldType.Type;
            for (int i = 0; i < length; i++)
            {
                // Get a byte-span view over result[i].
                Span<byte> dstBytes = MemoryMarshal.AsBytes(result.AsSpan(i, 1));

                // Read from container element -> write into dst (target T) using your conversion path. 
                var span = data.Slice(header.ElemSize * i, header.ElemSize);
                Migration.TryWriteTo(span, type, dstBytes, dstType, true);
            }

            return result;
        }

        internal static string AsString(in FieldHeader header, Container container)
        {
            if (header.Type == ValueType.Char16)
            {
                var data = container.GetFieldData<char>(in header);
                return data.ToString();
            }
            throw new InvalidOperationException("Cannot call AsString() on a non-char array.");
        }




        internal static bool AcceptTypeConversion<TTarget>(in FieldHeader fieldHeader) where TTarget : unmanaged
        {
            ValueType valueType = TypeUtil<TTarget>.ValueType;
            int size = TypeUtil<TTarget>.Size;

            return AcceptTypeConversion(in fieldHeader, valueType, size);
        }

        internal static bool AcceptTypeConversion(in FieldHeader fieldHeader, ValueType toValueType, int? elementSize)
        {
            if (toValueType == ValueType.Blob) return fieldHeader.ElemSize == (elementSize ?? TypeUtil.SizeOf(toValueType));
            return TypeUtil.IsImplicitlyConvertible(fieldHeader.Type, toValueType);
        }

        internal static bool AcceptTypeConversion(in FieldHeader fieldHeader, TypeData toType)
        {
            if (toType.ValueType == ValueType.Blob) return fieldHeader.ElemSize == toType.Size;
            return TypeUtil.IsImplicitlyConvertible(fieldHeader.Type, toType.ValueType);
        }
    }
}