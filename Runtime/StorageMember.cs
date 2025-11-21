using System;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Represent a member of a <see cref="StorageObject"/>.
    /// </summary>
    public readonly ref struct StorageMember
    {
        /// <summary>
        /// storage object
        /// </summary>
        private readonly StorageObject _storageObject;
        /// <summary>
        /// field name
        /// </summary>
        private readonly ReadOnlySpan<char> _fieldName;
        /// <summary>
        /// Array index, -1 if not an array element.
        /// </summary>
        private readonly int _index;



        public readonly ValueType ValueType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _storageObject.Container.GetFieldHeader(_fieldName).Type;
        }

        public readonly bool IsArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _index < 0 && _storageObject.IsArray(_fieldName);
        }

        public readonly bool IsArrayMember
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _index >= 0 && _storageObject.IsArray(_fieldName);
        }

        public StorageMember this[ReadOnlySpan<char> path]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => AsObject().GetMember(path);
        }




        public StorageMember(StorageObject storageObject, ReadOnlySpan<char> fieldName)
        {
            this._storageObject = storageObject;
            this._fieldName = fieldName;
            this._index = -1;
        }

        public StorageMember(StorageObject storageObject, ReadOnlySpan<char> fieldName, int index) : this(storageObject, fieldName)
        {
            this._index = index;
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject AsObject()
        {
            if (this._index >= 0) return _storageObject.GetObjectInArray(_fieldName, _index);
            return _storageObject.GetObject(_fieldName);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlyValueView AsScalar()
        {
            if (this._index >= 0) return _storageObject.GetArray(_fieldName)[_index];
            return _storageObject.GetValueView(_fieldName);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray AsArray()
        {
            if (this._index >= 0) return _storageObject.GetObjectInArray(_fieldName, _index).AsArray();
            return _storageObject.GetArray(_fieldName);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetMember(ReadOnlySpan<char> path, out StorageMember member) => AsObject().TryGetMember(path, out member);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>(bool isExplicit = false) where T : unmanaged => AsScalar().Read<T>(isExplicit);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(T value) where T : unmanaged
        {
            if (this._index >= 0) _storageObject.GetArray(_fieldName).Write(_index, value);
            else _storageObject.Write(_fieldName, value);
        }
    }
}
