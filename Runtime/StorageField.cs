using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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

        // -------------------------
        // Value-field helpers
        // -------------------------
        public void Write<T>(in T value) where T : unmanaged
        {
            if (_field.IsRef)
                throw new InvalidOperationException($"Field '{Name}' is a reference field; use ref APIs.");
            int sz = Unsafe.SizeOf<T>();
            if (sz > _field.AbsLength)
                throw new ArgumentException($"Type {typeof(T).Name} ({sz}B) > field '{Name}' ({_field.AbsLength}B).");

            var span = _container.GetSpan(_field);
            if (sz < _field.AbsLength) span.Clear();
            MemoryMarshal.Write(span, ref Unsafe.AsRef(value));
        }

        public T Read<T>() where T : unmanaged
        {
            if (_field.IsRef)
                throw new InvalidOperationException($"Field '{Name}' is a reference field; use ref APIs.");
            int sz = Unsafe.SizeOf<T>();
            if (sz > _field.AbsLength)
                throw new ArgumentException($"Type {typeof(T).Name} ({sz}B) > field '{Name}' ({_field.AbsLength}B).");

            return MemoryMarshal.Read<T>(_container.GetReadOnlySpan(_field));
        }

        public Span<byte> AsBytes() => _container.GetSpan(_field);

        public ReadOnlySpan<byte> AsReadOnlyBytes() => _container.GetReadOnlySpan(_field);

        public Span<T> AsSpan<T>() where T : unmanaged
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
        public StorageObject GetObject() => StorageObject.Get(Position);

        /// <summary>
        /// Get the object, if is currently null, return a new object based on given schema (or default schema if null schema is given)
        /// </summary>
        public StorageObject GetObjectOrNew(Schema defaultSchema = null) => StorageObject.GetOrNew(ref Position, defaultSchema);


        public bool TryGetObject(out StorageObject obj)
        {
            obj = default;
            if (!_field.IsRef || _field.RefCount != 1) return false;
            var id = _container.GetRef(_field);
            return StorageObject.TryGet(id, out obj);
        }

        internal void SetObject(Container container)
        {
            if (container is null) throw new ArgumentNullException(nameof(container));
            if (!_field.IsRef || _field.RefCount != 1)
                throw new InvalidOperationException($"Field '{Name}' is not a single reference slot.");
            ref ulong slot = ref _container.GetRef(_field);
            slot = container.ID;
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

