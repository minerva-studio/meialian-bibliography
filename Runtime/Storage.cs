using System;

namespace Amlos.Container
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
            //_root = new Container(rootSchema);
        }

        public Storage(Schema_Old rootSchema)
        {
            throw new InvalidOperationException();
            //_root = Container.CreateAt(ref _id, rootSchema);
            //_root = new Container(rootSchema);
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
