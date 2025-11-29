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
        private FieldHandle _handle;
        /// <summary>
        /// storage object
        /// </summary>
        private readonly StorageObject _storageObject => new StorageObject(_handle.Container);
        /// <summary>
        /// Array index, -1 if not an array element.
        /// </summary>
        private readonly int _index;


        /// <summary>
        /// Whether the member exists.
        /// </summary>
        public bool Exists
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
            get => _handle.Container.GetFieldHeader(EnsureFieldIndex()).ElementType;
        }

        public ValueType ValueType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _handle.Container.GetFieldHeader(EnsureFieldIndex()).Type;
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
        public readonly StorageObject StorageObject => _storageObject;
        /// <summary>
        /// Name of the member.
        /// </summary>
        public readonly ReadOnlySpan<char> Name => _handle.Name;
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
        /// <summary>
        /// Get a member in an array, if this member is an array.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public StorageMember this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => AsArray()[index];
        }


        public readonly int Int
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Read<int>();
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Write(value);
        }

        public readonly float Float
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Read<float>();
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Write(value);
        }

        public readonly double Double
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Read<double>();
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Write(value);
        }

        public readonly long Long
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Read<long>();
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Write(value);
        }

        public readonly string String
        {
#pragma warning disable CS8656 // allow duplicate struct for chained calls
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => AsObject().ReadString();
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => AsObject().WriteString(value);
#pragma warning restore CS8656
        }




        public StorageMember(StorageObject storageObject, ReadOnlySpan<char> fieldName)
        {
            // Determine if we might lose the name when schema changed.
            ThrowHelper.ThrowIfOverlap(storageObject._container.Span, MemoryMarshal.AsBytes(fieldName));

            this._index = -1;
            this._handle = storageObject.GetFieldHandle(fieldName);
        }

        public StorageMember(StorageObject storageObject, ReadOnlySpan<char> fieldName, int index) : this(storageObject, fieldName)
        {
            this._index = index;
        }

        public StorageMember(FieldHandle handle, int index)
        {
            this._handle = handle;
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
        public readonly StorageScalar AsScalar() => new StorageScalar(_handle, _index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly StorageScalar<T> AsScalar<T>() where T : unmanaged => new StorageScalar<T>(_handle, _index);

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
        public readonly T Read<T>(bool isExplicit = true) where T : unmanaged => AsScalar().Read<T>(isExplicit);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void Write<T>(T value) where T : unmanaged => AsScalar().Write(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Persistent ToPersistent() => new(this);



        public static explicit operator StorageObject(StorageMember member) => member.AsObject();
        public static explicit operator StorageScalar(StorageMember member) => member.AsScalar();
        public static explicit operator StorageArray(StorageMember member) => member.AsArray();
        public static explicit operator Persistent(StorageMember member) => new Persistent(member);



        public struct Persistent
        {
            private FieldHandle.Persistent _handle;
            private readonly int _index;
            public Persistent(FieldHandle.Persistent handle, int index)
            {
                _handle = handle;
                _index = index;
            }

            public Persistent(StorageMember member) : this(new(member._handle), member._index) { }


            public readonly bool IsDisposed
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _handle.Container == null || _handle.IsDisposed || _handle.Index < 0;
            }

            public readonly StorageMember Member
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new(_handle, _index);
            }

            public static implicit operator StorageMember(Persistent member) => new StorageMember(member._handle, member._index);
        }
    }
}
