using System;
using System.Runtime.CompilerServices;

namespace Amlos.Container
{
    /// <summary>
    /// Stack-only view over a value array stored inside a container field.
    /// - Field must be a non-ref field (Length > 0).
    /// - Field byte length must be a multiple of sizeof(T).
    /// - No heap allocation; wraps a Span<T>.
    /// </summary>
    public readonly ref struct StorageInlineArray<T> where T : unmanaged
    {
        private readonly Span<T> _span;

        internal StorageInlineArray(Span<T> span)
        {
            _span = span; // Container-level guard already rejects ref fields
        }

        /// <summary>Number of T elements.</summary>
        public int Length => _span.Length;

        /// <summary>Get a ref to element at index.</summary>
        public ref T this[int index] => ref _span[index];

        /// <summary>Get the underlying Span&lt;T&gt;.</summary>
        public Span<T> AsSpan() => _span;

        /// <summary>Clear all bytes (zero-fill).</summary>
        public void Clear() => _span.Clear();



        internal static StorageInlineArray<T> CreateView(Container container, int index)
        {
            // Ensure the field exists and matches an array of T (self-heal if needed)
            //container.EnsureArrayFor<T>(fieldName);
            container.EnsureFieldForRead<T>(index);

            var field = container.Schema.Fields[index];

            if (field.IsRef)
                throw new ArgumentException($"Field '{field.Name}' is a reference field; use StorageObjectArray instead.");
            int sz = Unsafe.SizeOf<T>();
            if (field.AbsLength % sz != 0)
                throw new ArgumentException($"Field '{field.Name}' length {field.AbsLength} is not divisible by sizeof({typeof(T).Name})={sz}.");

            // produce view and mark array hint
            StorageInlineArray<T> view = new(container.GetSpan<T>(index));
            container.SetArrayType<T>(index); // your existing API to set Pack(PrimOf<T>(), isArray:true)
            return view;
        }

        public T[] ToArray() => _span.ToArray();
    }
}
