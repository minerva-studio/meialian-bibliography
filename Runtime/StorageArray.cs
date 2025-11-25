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

        /// <summary>
        /// Is an external array? (not inline to the parent object)
        /// </summary>
        public readonly bool IsExternalArray => _handle.Container.IsArray;

        /// <summary>
        /// Is an object array? (a ref array)
        /// </summary>
        public readonly bool IsObjectArray => Type == ValueType.Ref;

        /// <summary>
        /// Is a string? (a ref array of char16)
        /// </summary>
        public readonly bool IsString => Type == ValueType.Char16;

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
                StorageObject.NotifyFieldWrite(_handle.Container, fieldIndex);
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
            if (!IsString)
                ThrowHelper.ArgumentException("Cannot write string to non-string array.");
            int fieldIndex = _handle.EnsureNotDisposed();
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            int length = valueSpan.Length;
            Resize(length);
            header = ref _handle.Container.GetFieldHeader(fieldIndex);
            var span = _handle.Container.GetFieldData(in header);
            MemoryMarshal.AsBytes(valueSpan).CopyTo(span);
            StorageObject.NotifyFieldWrite(_handle.Container, fieldIndex);
        }

        public bool TryWrite<T>(int index, T value, bool isExplicit = true) where T : unmanaged
        {
            var src = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref value, 1));
            return TryWrite(index, src, TypeUtil<T>.ValueType, isExplicit);
        }

        public bool TryWrite(int index, Span<byte> src, ValueType valueType, bool isExplicit = true)
        {
            if (index < 0 || index > Length)
                return false;
            if (_handle.IsDisposed)
                return false;
            int fieldIndex = _handle.Index;
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            int elementSize = header.ElemSize;
            var span = _handle.Container.GetFieldData(in header).Slice(elementSize * index, elementSize);
            bool result = Migration.TryWriteTo(src, valueType, span, header.Type, isExplicit);
            if (result)
                StorageObject.NotifyFieldWrite(_handle.Container, fieldIndex);
            return result;
        }


        public readonly T Read<T>(int index) where T : unmanaged => this[index].Read<T>();

        public bool TryRead<T>(int index, out T value) where T : unmanaged
        {
            value = default;
            if (index < 0 || index > Length)
                return false;
            return this[index].TryRead(out value, true);
        }





        /// <summary>
        /// Override content by given value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="values"></param>
        /// <param name="allowResize">if array length is less than given size, can we resize the array?</param>
        /// <param name="allowTypeRescheme">Can we chagne the array type?</param>
        public void Override<T>(ReadOnlySpan<T> values, bool allowResize = true, bool allowTypeRescheme = false) where T : unmanaged
        {
            int fieldIndex = _handle.EnsureNotDisposed();
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            using var unregisterBuffer = Container.UnregisterBuffer.New(_handle.Container);

            if (!header.ElementType.CanCastTo(TypeUtil<T>.Type, false))
            {
                if (!allowTypeRescheme)
                    ThrowHelper.ArgumentException("Type mismatch in Override.");
                _handle.Container.ReschemeFor(_handle.Name, TypeUtil<T>.Type, values.Length, unregisterBuffer);
                header = ref _handle.Container.GetFieldHeader(fieldIndex);
            }

            int length = header.ElementCount;
            if (values.Length > length)
                if (!allowResize)
                {
                    ThrowHelper.ArgumentException("Length exceed in Override.");
                }
                else
                {
                    Resize(values.Length);
                    header = ref _handle.Container.GetFieldHeader(fieldIndex);
                }

            Span<byte> data = _handle.Container.GetFieldData(in header);
            ValueType dstType = header.FieldType.Type;
            ValueType srcType = TypeUtil<T>.ValueType;
            Migration.MigrateValueFieldBytes(MemoryMarshal.AsBytes(values), data, srcType, dstType, true, zeroFillRemaining: false);

            unregisterBuffer.Send();
            StorageObject.NotifyFieldWrite(_handle.Container, fieldIndex);
        }

        public void Set<T>(ReadOnlySpan<T> values) where T : unmanaged
        {
            using var unregisterBuffer = Container.UnregisterBuffer.New(_handle.Container);

            _handle.Container.ReschemeFor(_handle.Name, TypeUtil<T>.Type, values.Length, unregisterBuffer);
            int fieldIndex = _handle.EnsureNotDisposed();
            ref FieldHeader header = ref _handle.Container.GetFieldHeader(fieldIndex);
            ReadOnlySpan<byte> src = MemoryMarshal.Cast<T, byte>(values);
            Span<byte> dst = _handle.Container.GetFieldData(in header);
            src.CopyTo(dst);

            unregisterBuffer.Send();
            StorageObject.NotifyFieldWrite(_handle.Container, fieldIndex);
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
            Span<ContainerReference> references = References;
            if (references.Length <= index)
            {
                child = default;
                return false;
            }

            ContainerReference containerReference = references[index];
            return containerReference.TryGet(out child);
        }




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
                using Container.UnregisterBuffer buffer = Container.UnregisterBuffer.New(_handle.Container);
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
            if (newLength == Length)
                return;
            _handle.Container.ResizeArrayField(_handle.Index, newLength);
        }

        /// <summary>
        /// Resize the array
        /// </summary>
        /// <param name="length"></param>
        public void EnsureLength(int length)
        {
            if (length <= Length)
                return;
            _handle.Container.ResizeArrayField(_handle.Index, length);
        }

        /// <summary>
        /// Rescheme array to match type
        /// </summary>
        /// <param name="type"></param>
        /// <param name="newLength"></param>
        public void Rescheme(TypeData type, int? newLength = null)
        {
            _handle.EnsureNotDisposed();
            newLength ??= Length;
            _handle.Container.ReschemeFor(_handle.Name, type, newLength);
        }

        /// <summary>
        /// Can array be treated as typeof T?
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public bool IsConvertibleTo<T>() where T : unmanaged => AcceptTypeConversion<T>(in Header);
        public bool IsConvertibleTo(TypeData type) => AcceptTypeConversion(in Header, type);
        public bool IsConvertibleTo(ValueType valueType, int? elementSize = null) => AcceptTypeConversion(in Header, valueType, elementSize);




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