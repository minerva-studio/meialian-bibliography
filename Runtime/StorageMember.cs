using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Represent a member of a <see cref="StorageObject"/>.
    /// </summary>
    public ref struct StorageMember
    {
        /// <summary>
        /// storage object
        /// </summary>
        private readonly StorageObject _storageObject;
        /// <summary>
        /// Array index, -1 if not an array element.
        /// </summary>
        private readonly int _index;

        private FieldHandle _handle;

        /// <summary>
        /// Whether the member exists.
        /// </summary>
        public bool Exist
        {
            get => !_handle.IsDisposed && _handle.Index >= 0;
        }

        public bool IsDisposed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_handle.IsDisposed) return true;
                int fieldIndex = _handle.Index;
                return _index < 0
                    ? (fieldIndex < 0 || fieldIndex > _storageObject.FieldCount)
                    : (!_storageObject.IsArray(fieldIndex) || _index >= _storageObject.GetArray(ref _handle).Length);
            }
        }


        public TypeData Type
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _storageObject.Container.GetFieldHeader(EnsureFieldIndex()).ElementType;
        }

        public ValueType ValueType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _storageObject.Container.GetFieldHeader(EnsureFieldIndex()).Type;
        }

        public bool IsArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _index < 0 && _storageObject.IsArray(EnsureFieldIndex());
        }

        public bool IsArrayMember
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _index >= 0 && _storageObject.IsArray(EnsureFieldIndex());
        }

        public readonly int ArrayIndex
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _index;
        }

        public int ArrayLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                EnsureNoDispose();
                int index = EnsureFieldIndex();
                if (this._index >= 0) return _storageObject.GetArray(ref _handle).Length;
                return _storageObject.GetArray(ref _handle).Length;
            }
        }

        /// <summary>
        /// Object that owns this member.
        /// </summary>
        public StorageObject StorageObject => _storageObject;
        /// <summary>
        /// Name of the member.
        /// </summary>
        public ReadOnlySpan<char> Name => _handle.Name;
        /// <summary>
        /// Get a nested member by path.
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public StorageMember this[ReadOnlySpan<char> path]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => AsObject().GetMember(path);
        }




        public StorageMember(StorageObject storageObject, ReadOnlySpan<char> fieldName)
        {
            // Determine if we might lose the name when schema changed.
            ThrowHelper.ThrowIfOverlap(storageObject._container.Span, MemoryMarshal.AsBytes(fieldName));

            this._storageObject = storageObject;
            this._index = -1;
            this._handle = storageObject.GetFieldHandle(fieldName);
        }

        public StorageMember(StorageObject storageObject, ReadOnlySpan<char> fieldName, int index) : this(storageObject, fieldName)
        {
            this._index = index;
        }


        private int EnsureFieldIndex() => _handle.Index;

        private void EnsureNoDispose()
        {
            if (IsDisposed)
                throw new ObjectDisposedException("This Member has been disposed.");
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject AsObject()
        {
            EnsureNoDispose();
            int index = EnsureFieldIndex();
            if (this._index >= 0) return _storageObject.GetObjectInArray(index, _index);
            return _storageObject.GetObject(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlyValueView AsScalar()
        {
            EnsureNoDispose();
            int index = EnsureFieldIndex();
            if (this._index >= 0) return _storageObject.GetArray(ref _handle)[_index];
            return _storageObject.GetValueView(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray AsArray()
        {
            EnsureNoDispose();
            int index = EnsureFieldIndex();
            if (this._index >= 0) return _storageObject.GetObjectInArray(index, _index).AsArray();
            return _storageObject.GetArray(ref _handle);
        }


        public void ChangeFieldType(TypeData type, int? inlineArrayLength)
        {
            EnsureNoDispose();
            _storageObject.ChangeFieldType(Name, type, inlineArrayLength);
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetMember(ReadOnlySpan<char> path, out StorageMember member)
        {
            if (IsDisposed)
            {
                member = default;
                return false;
            }
            return AsObject().TryGetMember(path, out member);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>(bool isExplicit = false) where T : unmanaged => AsScalar().Read<T>(isExplicit);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(T value) where T : unmanaged
        {
            EnsureNoDispose();
            int index = EnsureFieldIndex();
            if (this._index >= 0) _storageObject.GetArray(ref _handle).Write(_index, value);
            else _storageObject.Write(index, value);
        }
    }
}
