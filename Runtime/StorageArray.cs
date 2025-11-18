using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Abstraction of any storage array
    /// </summary>
    public readonly struct StorageArray
    {
        public const int Inline = 1;
        public const int Object = 2;

        private readonly Container _container;
        private readonly int _fieldIndex;
        private readonly int _generation;


        /// <summary>
        /// Create an array view
        /// </summary>
        /// <param name="container"></param>
        /// <param name="fieldIndex"></param>
        internal StorageArray(Container container, int fieldIndex = 0)
        {
            this._container = container;
            this._fieldIndex = fieldIndex;
            this._generation = container.Generation;
        }


        /// <summary>
        /// Array Length
        /// </summary>
        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                EnsureNotDisposed();
                ref var header = ref Header;
                return header.Length / header.ElemSize;
            }
        }

        public ValueType Type
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                EnsureNotDisposed();
                return Header.Type;
            }
        }

        /// <summary>
        /// Value
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public ValueView this[int index]
        {
            readonly get
            {
                EnsureNotDisposed();
                ref FieldHeader header = ref Header;
                int elementSize = header.ElemSize;
                var span = _container.GetFieldData(in header).Slice(elementSize * index, elementSize);
                return new ValueView(span, header.Type);
            }
            set
            {
                EnsureNotDisposed();
                this[index].Write(value.Bytes, Header.Type);
            }
        }

        internal ref FieldHeader Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref _container.GetFieldHeader(_fieldIndex);
        }

        /// <summary>Direct access to the underlying ID span (use with care).</summary>
        internal Span<ContainerReference> References
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                ref FieldHeader header = ref Header;
                return _container.GetFieldData<ContainerReference>(in header);
            }
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnsureNotDisposed() => _container.EnsureNotDisposed(_generation);





        /// <summary> Get a child as a StorageObject </summary>
        public StorageObject GetObject(int index)
        {
            EnsureNotDisposed();
            return StorageObjectFactory.GetOrCreate(ref References[index], ContainerLayout.Empty);
        }

        /// <summary> Get a child as a StorageObject.</summary>
        public StorageObject GetObject(int index, ContainerLayout layout)
        {
            EnsureNotDisposed();
            return StorageObjectFactory.GetOrCreate(ref References[index], layout);
        }

        /// <summary>Try get a child; returns false if slot is 0 or container is missing.</summary>
        public bool TryGetObject(int index, out StorageObject child)
        {
            EnsureNotDisposed();
            return StorageObjectFactory.TryGet(References[index], out child);
        }

        public void CopyFrom<T>(ReadOnlySpan<T> values) where T : unmanaged
        {
            EnsureNotDisposed();
            ref FieldHeader header = ref Header;
            int length = header.Length / header.ElemSize;

            Span<byte> data = _container.GetFieldData(in header);
            ValueType dstType = header.FieldType.Type;
            ValueType srcType = TypeUtil.PrimOf<T>();
            for (int i = 0; i < length; i++)
            {
                ReadOnlySpan<byte> src = MemoryMarshal.Cast<T, byte>(values[i..(i + 1)]);
                var dst = data.Slice(header.ElemSize * i, header.ElemSize);
                Migration.TryWriteTo(src, srcType, dst, dstType, false);
            }
        }




        /// <summary>Clear the slot (set ID to 0).</summary>
        public void ClearAt(int index)
        {
            EnsureNotDisposed();
            ref var header = ref Header;
            if (header.IsRef)
                Container.Registry.Shared.Unregister(ref References[index]);

            int elementSize = header.ElemSize;
            var span = _container.GetFieldData(in header).Slice(elementSize * index, elementSize);
            span.Clear();
        }

        /// <summary>Clear all bytes (zero-fill).</summary>
        public void Clear()
        {
            EnsureNotDisposed();
            ref var header = ref Header;
            if (header.IsRef)
            {
                Span<ContainerReference> ids = References;
                for (int i = 0; i < ids.Length; i++)
                {
                    Container.Registry.Shared.Unregister(ref ids[i]);
                }
            }
            _container.GetFieldData(in header).Clear();
        }

        public T[] ToArray<T>() where T : unmanaged
        {
            EnsureNotDisposed();
            // 1) Disallow ref fields for value extraction.
            ref var header = ref Header;
            if (header.IsRef)
                throw new InvalidOperationException("Cannot call ToArray<T>() on a ref field. Use object accessors instead.");

            int length = header.Length / header.ElemSize;
            var result = new T[length];

            var dstType = TypeUtil.PrimOf<T>();

            Span<byte> data = _container.GetFieldData(in header);
            ValueType type = header.FieldType.Type;
            for (int i = 0; i < length; i++)
            {
                // Get a byte-span view over result[i].
                Span<byte> dstBytes = MemoryMarshal.AsBytes(result.AsSpan(i, 1));

                // Read from container element -> write into dst (target T) using your conversion path. 
                var span = data.Slice(header.ElemSize * i, header.ElemSize);
                Migration.TryWriteTo(span, type, dstBytes, dstType, true);
            }

            return result;
        }
    }
}
