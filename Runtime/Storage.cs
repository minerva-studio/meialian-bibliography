using Minerva.DataStorage.Serialization;
using System;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// User-facing root owner that ensures the whole tree is unregistered/disposed when done.
    /// </summary>
    public sealed class Storage : IDisposable
    {
        private ContainerReference _id;
        private Container _root;
        private bool _disposed;

        public StorageObject Root => new StorageObject(_root);

        /// <summary>
        /// Name fo the storage root container.
        /// </summary>
        public string Name
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Root.Container.Name;
            set => Root.Container.Rename(value);
        }

        /// <summary>
        /// Version of the storage root container.
        /// </summary>
        public int Version
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Root.Container.Version;
        }

        public StorageMember this[ReadOnlySpan<char> path]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Root.GetMember(path);
        }




        public Storage() : this(ContainerLayout.Empty) { }

        public Storage(ContainerLayout rootSchema)
        {
            _root = Container.Registry.Shared.CreateRoot(ref _id, rootSchema);
        }

        internal Storage(Container container)
        {
            if (container.ID == Container.Registry.ID.Wild)
                Container.Registry.Shared.RegisterRoot(container);

            _root = container;
            _id = container.ID;
        }

        ~Storage() { Dispose(false); }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            _disposed = true;

            if (_root != null)
            {
                // This will recursively unregister children and dispose containers
                Container.Registry.Shared.Unregister(_root);
                _root = null;
            }
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetMember(ReadOnlySpan<char> path, out StorageMember member) => Root.TryGetMember(path, out member);

        /// <summary>
        /// Deep clone a storage instance.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public static Storage Clone(Storage source)
        {
            if (source == null)
                ThrowHelper.ThrowArgumentNull(nameof(source));
            return Clone(source.Root);
        }

        /// <summary>
        /// Create a deep clone of a storage object and its children.
        /// </summary>
        /// <param name="storageObject"></param>
        /// <returns></returns>
        public static Storage Clone(StorageObject storageObject)
        {
            if (storageObject == default)
                return new Storage();

            var bytes = BinarySerialization.ToBinary(storageObject);
            return BinarySerialization.Parse(bytes);
        }
    }
}
