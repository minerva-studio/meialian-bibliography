using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Stack-only view over a value array stored inside a container field.
    /// - Field must be a non-ref field (Length > 0).
    /// - Field byte length must be a multiple of sizeof(T). 
    /// </summary> 
    [Obsolete]
    public readonly ref struct StorageInlineArray
    {
        readonly FieldView f;


        /// <summary>
        /// Span
        /// </summary>
        private Span<byte> Span => f.Data;

        /// <summary>Direct access to the underlying ID span (use with care).</summary>
        internal Span<ContainerReference> Ids => f.IsRef ? MemoryMarshal.Cast<byte, ContainerReference>(f.Data) : throw new InvalidOperationException();

        /// <summary>
        /// Array Length
        /// </summary>
        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                FieldHeader header = f.Header;
                return header.Length / header.ElemSize;
            }
        }

        public ValueType Type => f.Type;

        /// <summary>
        /// Value
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public ValueView this[int index]
        {
            readonly get
            {
                ref FieldHeader header = ref f.Header;
                int elementSize = header.ElemSize;
                var span = Span.Slice(elementSize * index, elementSize);
                return new ValueView(span, header.Type);
            }
            set => this[index].Write(value.Bytes, f.Type);
        }



        internal StorageInlineArray(FieldView f)
        {
            this.f = f;
        }



        /// <summary>Get a child as a StorageObject (throws if not found).</summary>
        public StorageObject Get(int index) => StorageObjectFactory.GetOrCreate(ref Ids[index], ContainerLayout.Empty);

        /// <summary>Try get a child; returns false if slot is 0 or container is missing.</summary>
        public bool TryGet(int index, out StorageObject child) => StorageObjectFactory.TryGet(Ids[index], out child);





        internal void CopyFrom<T>(ReadOnlySpan<T> values) where T : unmanaged
        {
            for (int i = 0; i < Length; i++)
            {
                this[i].Write(values[i]);
            }
        }





        /// <summary>Clear the slot (set ID to 0).</summary>
        public void ClearAt(int index)
        {
            if (f.IsRef)
                Container.Registry.Shared.Unregister(ref Ids[index]);

            int elementSize = f.Header.ElemSize;
            var span = Span.Slice(elementSize * index, elementSize);
            span.Clear();
        }

        /// <summary>Clear all bytes (zero-fill).</summary>
        public void Clear()
        {
            if (f.IsRef)
            {
                Span<ContainerReference> ids = Ids;
                for (int i = 0; i < ids.Length; i++)
                {
                    Container.Registry.Shared.Unregister(ref ids[i]);
                }
            }
            Span.Clear();
        }

        public T[] ToArray<T>() where T : unmanaged
        {
            // 1) Disallow ref fields for value extraction.
            if (f.IsRef)
                throw new InvalidOperationException("Cannot call ToArray<T>() on a ref field. Use object accessors instead.");

            int length = Length;
            var result = new T[length];

            // 2) Source value type (stored in the container).
            var srcType = f.Type;

            // 3) Target value type (requested T).
            var dstType = TypeUtil<T>.ValueType;

            for (int i = 0; i < length; i++)
            {
                // Get a byte-span view over result[i].
                Span<byte> dstBytes = MemoryMarshal.AsBytes(result.AsSpan(i, 1));

                // Read from container element -> write into dst (target T) using your conversion path.
                // IMPORTANT: Use ValueView.WriteTo, not Write.
                this[i].TryWriteTo(dstBytes, dstType, true);
            }

            return result;
        }

    }
}
