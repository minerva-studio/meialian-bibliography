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
    public readonly ref struct StorageArray<T> where T : unmanaged
    {
        private readonly Span<T> _span;

        private StorageArray(Container container, FieldDescriptor field)
        {
            if (field.IsRef)
                throw new ArgumentException($"Field '{field.Name}' is a reference field; use StorageObjectArray instead.");
            int sz = Unsafe.SizeOf<T>();
            if (field.AbsLength % sz != 0)
                throw new ArgumentException($"Field '{field.Name}' length {field.AbsLength} is not divisible by sizeof({typeof(T).Name})={sz}.");
            _span = container.GetSpan<T>(field); // Container-level guard already rejects ref fields
        }

        /// <summary>Number of T elements.</summary>
        public int Count => _span.Length;

        /// <summary>Get a ref to element at index.</summary>
        public ref T this[int index] => ref _span[index];

        /// <summary>Get the underlying Span&lt;T&gt;.</summary>
        public Span<T> AsSpan() => _span;

        /// <summary>Clear all bytes (zero-fill).</summary>
        public void Clear() => _span.Clear();



        internal static StorageArray<T> CreateView(Container container, string fieldName)
        {
            var index = container.Schema.IndexOf(fieldName);
            if (index < 0)
                throw new ArgumentException($"Field '{fieldName}' does not exist in schema.");
            var field = container.Schema.Fields[index];
            StorageArray<T> storageArray = new(container, field);
            container.SetArrayHint<T>(index);
            return storageArray;
        }

        public T[] ToArray() => _span.ToArray();
    }
}
