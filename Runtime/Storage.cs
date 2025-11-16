using System;

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


        public Storage() : this(ContainerLayout.Empty) { }

        public Storage(ContainerLayout rootSchema)
        {
            _root = Container.Registry.Shared.CreateAt(ref _id, rootSchema);
        }

        internal Storage(Container container)
        {
            if (container.ID == Container.Registry.ID.Wild)
                Container.Registry.Shared.Register(container);

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
    }
}
