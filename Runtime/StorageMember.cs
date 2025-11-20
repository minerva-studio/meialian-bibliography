using System;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Represent a member of a <see cref="StorageObject"/>.
    /// </summary>
    public readonly ref struct StorageMember
    {
        private readonly StorageObject _storageObject;
        private readonly ReadOnlySpan<char> _fieldName;

        public readonly ValueType ValueType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _storageObject.Container.GetFieldHeader(_fieldName).Type;
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
        }





        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject AsObject() => _storageObject.GetObject(_fieldName);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueView AsScalar() => _storageObject.GetValueView(_fieldName);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray AsArray() => _storageObject.GetArray(_fieldName);
    }
}
