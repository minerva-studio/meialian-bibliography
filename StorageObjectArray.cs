using System;

namespace Amlos.Container
{
    /// <summary>
    /// Stack-only view over an array of child references (IDs) stored inside a container field.
    /// - Field must be a ref field (Length < 0).
    /// - Element size is 8 bytes (ulong).
    /// - Provides convenient get/set child operations through the registry.
    /// </summary>
    public readonly ref struct StorageObjectArray
    {
        private readonly Span<ulong> _ids;

        internal StorageObjectArray(Container container, FieldDescriptor field)
        {
            if (!field.IsRef)
                throw new ArgumentException($"Field '{field.Name}' is not a reference field.");
            _ids = container.GetRefSpan(field);
        }

        /// <summary>Number of child reference slots.</summary>
        public int Count => _ids.Length;

        /// <summary>Direct access to the underlying ID span (use with care).</summary>
        internal Span<ulong> Ids => _ids;


        public StorageObjectArrayElement this[int index] => new(this, index);


        /// <summary>Get a child as a StorageObject (throws if not found).</summary>
        public StorageObject Get(int index) => StorageObject.Get(Ids[index]);

        /// <summary>Try get a child; returns false if slot is 0 or container is missing.</summary>
        public bool TryGet(int index, out StorageObject child) => StorageObject.TryGet(_ids[index], out child);

        /// <summary>Assign a raw ID into the slot.</summary>
        public void SetId(int index, ulong id) => _ids[index] = id;

        /// <summary>Clear the slot (set ID to 0).</summary>
        public void ClearAt(int index) => _ids[index] = 0UL;

        /// <summary>Clear all slots (set all IDs to 0).</summary>
        public void ClearAll() => _ids.Clear();
    }

    public readonly ref struct StorageObjectArrayElement
    {
        readonly StorageObjectArray _array;
        readonly int _index;

        internal StorageObjectArrayElement(StorageObjectArray array, int index)
        {
            this._array = array;
            this._index = index;
        }

        /// <summary>
        /// Single-ref field ref ref
        /// </summary>
        private ref ulong Position => ref _array.Ids[_index];

        public StorageObject AsObject() => StorageObject.Get(Position);

        public StorageObject AsObjectOrNew(Schema schema) => StorageObject.GetOrNew(ref Position, schema);
    }
}

