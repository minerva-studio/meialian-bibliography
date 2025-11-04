using System;
using static Amlos.Container.StorageFactory;

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

        /// <summary>Number of child reference slots.</summary>
        public int Count => _ids.Length;

        /// <summary>Direct access to the underlying ID span (use with care).</summary>
        internal Span<ulong> Ids => _ids;

        public StorageObjectArrayElement this[int index] => new(this, index);




        internal StorageObjectArray(Container container, FieldDescriptor field)
        {
            if (!field.IsRef)
                throw new ArgumentException($"Field '{field.Name}' is not a reference field.");
            _ids = container.GetRefSpan(field);
        }



        /// <summary>Get a child as a StorageObject (throws if not found).</summary>
        public StorageObject Get(int index) => GetNoAllocate(Ids[index]);

        /// <summary>Try get a child; returns false if slot is 0 or container is missing.</summary>
        public bool TryGet(int index, out StorageObject child) => StorageFactory.TryGet(_ids[index], out child);

        /// <summary>Assign a raw ID into the slot.</summary>
        public void SetId(int index, ulong id) => _ids[index] = id;

        /// <summary>Clear the slot (set ID to 0).</summary>
        public void ClearAt(int index) => Container.Registry.Shared.Unregister(ref _ids[index]);

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
        /// Is a null slot (ID == 0)
        /// </summary>
        public bool IsNull => Position == 0UL;
        public StorageObject Object => Get(ref Position, Schema.Empty);


        /// <summary>
        /// Single-ref field ref ref
        /// </summary>
        private ref ulong Position => ref _array.Ids[_index];

        public StorageObject GetObjectNoAllocate() => GetNoAllocate(Position);

        public StorageObject GetObject(Schema schema) => Get(ref Position, schema);


        public static implicit operator StorageObject(StorageObjectArrayElement element) => element.GetObjectNoAllocate();
    }
}

