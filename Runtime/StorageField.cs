using System;

namespace Amlos.Container
{
    /// <summary>
    /// Stack-only view that represents a specific field inside a container.
    /// - Works for both value fields (Length>0) and reference fields (Length<0).
    /// - Provides type-safe APIs for reading/writing values and managing child references.
    /// - Does not expose the underlying Container outside; cannot be persisted.
    /// </summary>
    public readonly ref struct StorageField
    {
        private readonly Container _container;
        private readonly FieldDescriptor _field;

        internal StorageField(Container container, FieldDescriptor field)
        {
            _container = container ?? throw new ArgumentNullException(nameof(container));
            _field = field;
        }

        public string Name => _field.Name;
        public FieldDescriptor Descriptor => _field;
        public bool IsRef => _field.IsRef;
        public int RefCount => _field.RefCount;
        public int ByteLength => _field.AbsLength;
        public int Offset => _field.Offset;

        /// <summary>
        /// Single-ref field ref ref
        /// </summary>
        private ref ulong Position
        {
            get
            {
                if (!_field.IsRef || _field.RefCount != 1)
                    throw new InvalidOperationException($"Field '{Name}' is not a single reference slot.");
                return ref _container.GetRef(_field);
            }
        }

        /// <summary>
        /// Get Object from this reference field.
        /// </summary>
        public StorageObject Object
        {
            get
            {
                if (!_field.IsRef || _field.RefCount != 1)
                    throw new InvalidOperationException($"Field '{Name}' is not a single reference slot.");
                return GetObject();
            }
        }






        // -------------------------
        // Value-field helpers
        // -------------------------
        public void Write<T>(in T value) where T : unmanaged
        {
            if (_field.IsRef)
                throw new InvalidOperationException($"Field '{Name}' is a reference field; use ref APIs.");

            _container.WriteNoRescheme(_field, value);
        }

        public T Read<T>() where T : unmanaged
        {
            if (_field.IsRef)
                throw new InvalidOperationException($"Field '{Name}' is a reference field; use ref APIs.");

            return _container.Read<T>(_field);
        }




        internal Span<byte> AsBytes() => _container.GetSpan(_field);

        internal ReadOnlySpan<byte> AsReadOnlyBytes() => _container.GetReadOnlySpan(_field);

        internal Span<T> AsSpan<T>() where T : unmanaged
        {
            if (_field.IsRef)
                throw new InvalidOperationException($"Field '{Name}' is a reference field; use AsObjectArray().");
            return _container.GetSpan<T>(_field);
        }

        public ReadOnlySpan<T> AsReadOnlySpan<T>() where T : unmanaged
        {
            if (_field.IsRef)
                throw new InvalidOperationException($"Field '{Name}' is a reference field; use AsObjectArray().");
            return _container.GetReadOnlySpan<T>(_field);
        }




        /// <summary>
        /// Read as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        public string ReadString() => new(AsSpan<char>());




        // -------------------------
        // Reference-field helpers
        // -------------------------

        internal ulong GetId() => Position;

        internal void SetId(ulong id) => Position = id;

        public void ClearRef()
        {
            if (!_field.IsRef) throw new InvalidOperationException($"Field '{Name}' is not a reference field.");
            var ids = _container.GetRefSpan(_field);
            ids.Clear();
        }

        public void CreateObject(Schema schema) => Container.CreateAt(ref Position, schema ?? Schema.Empty);

        /// <summary>
        /// Return a null-representable StorageObject.
        /// Returns default(StorageObject) when the slot is empty or the id is missing.
        /// </summary>
        public StorageObject GetObjectNoAllocate() => StorageFactory.GetNoAllocate(Position);

        /// <summary>
        /// Get the object, if is currently null, return a new object based on given schema (or default schema if null schema is given)
        /// </summary>
        public StorageObject GetObject() => StorageFactory.Get(ref Position, Schema.Empty);

        /// <summary>
        /// Get the object, if is currently null, return a new object based on given schema (or default schema if null schema is given)
        /// </summary>
        public StorageObject GetObject(Schema defaultSchema = null) => StorageFactory.Get(ref Position, defaultSchema);


        public bool TryGetObject(out StorageObject obj)
        {
            obj = default;
            if (!_field.IsRef || _field.RefCount != 1) return false;
            var id = _container.GetRef(_field);
            return StorageFactory.TryGet(id, out obj);
        }

        // Value-array view (T[] packed as bytes)
        public StorageArray<T> AsArray<T>() where T : unmanaged
        {
            if (_field.IsRef) throw new InvalidOperationException($"Field '{Name}' is a reference field.");
            return StorageArray<T>.CreateView(_container, _field.Name);
        }

        // Ref-array view
        public StorageObjectArray AsObjectArray()
        {
            if (!_field.IsRef || _field.RefCount <= 0)
                throw new InvalidOperationException($"Field '{Name}' is not a ref-array.");
            return new StorageObjectArray(_container, _field);
        }




        public static bool operator ==(StorageField left, StorageField right) => left._container == right._container && left._field == right._field;
        public static bool operator !=(StorageField left, StorageField right) => !(left == right);
        public override int GetHashCode() => HashCode.Combine(_container, _field);
        public override bool Equals(object obj) => false;
    }
}

