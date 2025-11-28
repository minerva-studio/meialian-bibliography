using System;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    public ref struct StorageScalar<T> where T : unmanaged
    {
        private StorageScalar _self;

        public StorageScalar(FieldHandle handle, int index = -1) : this()
        {
            _self = new StorageScalar(handle, index);
        }

        public T Value
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _self.Read<T>();
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => _self.Write(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read(bool isExplicit = true) => _self.Read<T>(isExplicit);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(T value) => _self.Write(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator T(StorageScalar<T> scalar) => scalar.Value;
    }


    public ref struct StorageScalar
    {
        private FieldHandle _handle;

        /// <summary>
        /// Array index, -1 if not an array element.
        /// </summary>
        private readonly int _index;

        public StorageScalar(FieldHandle handle, int index = -1) : this()
        {
            _handle = handle;
            _index = index;
        }

        /// <summary>
        /// Whether the member exists.
        /// </summary>
        public bool Exist
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => !_handle.IsDisposed && _handle.Index >= 0;
        }

        public bool IsDisposed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_handle.IsDisposed) return true;
                int fieldIndex = _handle.Index;
                var _storageObject = new StorageObject(_handle.Container);
                return _index < 0
                    ? (fieldIndex < 0 || fieldIndex > _storageObject.FieldCount)
                    : (!_storageObject.IsArray(fieldIndex) || _index >= _storageObject.GetArray(ref _handle).Length);
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int EnsureFieldIndex() => _handle.Index;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureNoDispose()
        {
            if (IsDisposed)
                throw new ObjectDisposedException("This Member has been disposed.");
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlyValueView ToValueView()
        {
            EnsureNoDispose();
            int index = EnsureFieldIndex();
            var _storageObject = new StorageObject(_handle.Container);
            if (this._index >= 0) return _storageObject.GetArray(ref _handle).Scalar[_index];
            return _storageObject.GetValueView(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>(bool isExplicit = true) where T : unmanaged
        {
            return ToValueView().Read<T>(isExplicit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(T value) where T : unmanaged
        {
            EnsureNoDispose();
            int index = EnsureFieldIndex();
            var _storageObject = new StorageObject(_handle.Container);
            if (this._index >= 0) _storageObject.GetArray(ref _handle).Write(_index, value);
            else _storageObject.Write(index, value);
        }
    }
}
