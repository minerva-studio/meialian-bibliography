using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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

        public readonly bool IsDisposed => _handle.Container == null || _handle.IsDisposed;

        public readonly bool IsRefArray => _handle.Container.IsArray;

        /// <summary>
        /// Is a string? (a ref array of char16)
        /// </summary>
        public readonly bool IsString => IsRefArray && Type == ValueType.Char16;

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
                return header.Length / header.ElemSize;
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

        public readonly FieldType FieldType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _handle.EnsureNotDisposed();
                return Header.FieldType;
            }
        }

        internal readonly Container Container
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _handle.Container;
        }

        /// <summary>
        /// Value
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public ReadOnlyValueView this[int index]
        {
            readonly get
            {
                ref FieldHeader header = ref Header;
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

                StorageObject.NotifyFieldWrite(_handle.Container, fieldIndex);
            }
        }

        private readonly ref FieldHeader Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref _handle.Container.GetFieldHeader(_handle.Index);
        }

        internal WriteView Raw => new(ref this);

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

        public void Write<T>(int index, T value) where T : unmanaged
        {
            var src = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref value, 1));
            Write(index, src, TypeUtil<T>.ValueType);
        }

        public void Write(int index, Span<byte> src, ValueType valueType)
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            int elementSize = header.ElemSize;
            var span = _handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
            Migration.TryWriteTo(src, valueType, span, header.Type, false);

            StorageObject.NotifyFieldWrite(_handle.Container, fieldIndex);
        }

        public readonly T Read<T>(int index) where T : unmanaged => this[index].Read<T>();



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
            using var tempString = new TempString(_handle.Name);
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
        public bool TryGetObject(int index, out StorageObject child)
        {
            return References[index].TryGet(out child);
        }




        public void CopyFrom<T>(ReadOnlySpan<T> values) where T : unmanaged
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            int length = header.Length / header.ElemSize;

            Span<byte> data = _handle.Container.GetFieldData(in header);
            ValueType dstType = header.FieldType.Type;
            ValueType srcType = TypeUtil<T>.ValueType;
            for (int i = 0; i < length; i++)
            {
                ReadOnlySpan<byte> src = MemoryMarshal.Cast<T, byte>(values[i..(i + 1)]);
                var dst = data.Slice(header.ElemSize * i, header.ElemSize);
                Migration.TryWriteTo(src, srcType, dst, dstType, false);
            }
        }




        /// <summary>Clear the slot (set ID to 0).</summary>
        public void ClearAt(int index)
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref var header = ref _handle.Container.GetFieldHeader(fieldIndex);

            if (header.IsRef)
                Container.Registry.Shared.Unregister(ref _handle.Container.GetFieldData<ContainerReference>(in header)[index]);

            int elementSize = header.ElemSize;
            var span = _handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
            span.Clear();
        }

        /// <summary>Clear all bytes (zero-fill).</summary>
        public void Clear()
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref var header = ref _handle.Container.GetFieldHeader(fieldIndex);
            if (header.IsRef)
            {
                Span<ContainerReference> ids = _handle.Container.GetFieldData<ContainerReference>(in header);
                for (int i = 0; i < ids.Length; i++)
                {
                    Container.Registry.Shared.Unregister(ref ids[i]);
                }
            }
            _handle.Container.GetFieldData(in header).Clear();
        }

        public T[] ToArray<T>() where T : unmanaged
        {
            // 1) Disallow ref fields for value extraction.
            ref var header = ref Header;
            if (header.IsRef)
                throw new InvalidOperationException("Cannot call ToArray<T>() on a ref field. Use object accessors instead.");

            return ToArray<T>(in header, _handle.Container);
        }

        public string AsString()
        {
            // 1) Disallow ref fields for value extraction.
            ref var header = ref Header;
            Container container = _handle.Container;
            return AsString(header, container);
        }

        /// <summary>
        /// Convert to string for display
        /// </summary>
        /// <remarks>
        /// For a char16 array, returns the string representation.
        /// </remarks>
        /// <returns></returns>
        public override string ToString()
        {
            if (Type == ValueType.Char16)
            {
                return AsString();
            }
            return $"{TypeUtil.ToString(Type)}[{Length}]";
        }




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
    }
}