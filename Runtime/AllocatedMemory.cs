#nullable enable
using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Wraps either:
    /// - a pooled byte[] owned by this instance, or
    /// - an arbitrary external Memory&lt;byte&gt; that is not owned.
    ///
    /// When owning a buffer, the underlying array is rented from ArrayPool and
    /// will be returned on Dispose(). External memory is never returned or touched.
    /// </summary>
    public struct AllocatedMemory : IDisposable
    {
        /// <summary>
        /// Default array pool used for owned buffers.
        /// You can swap this to ArrayPool&lt;byte&gt;.Create() if you want a dedicated pool.
        /// </summary>
        public static readonly ArrayPool<byte> DefaultPool = ArrayPool<byte>.Shared;

        /// <summary>
        /// Non-null only when this instance owns the underlying array.
        /// When null, _data points to external memory.
        /// </summary>
        private byte[]? _buffer;

        /// <summary>
        /// Current visible window of data. May refer to _buffer or external memory.
        /// </summary>
        internal Memory<byte> Buffer;

        /// <summary>
        /// True when there is no visible data.
        /// </summary>
        public readonly bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Buffer.IsEmpty;
        }

        /// <summary>
        /// Internal array buffer, if exist
        /// </summary>
        internal readonly byte[]? Array
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _buffer;
        }

        public readonly ref byte this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref Buffer.Span[index];
        }






        /// <summary>
        /// Wraps an arbitrary Memory&lt;byte&gt;.
        /// The underlying memory is considered external and will not be returned or disposed.
        /// Expand() may later allocate and switch to an owned buffer if more capacity is needed.
        /// </summary>
        public AllocatedMemory(Memory<byte> buffer)
        {
            _buffer = null;  // not owned
            Buffer = buffer;
        }

        /// <summary>
        /// Wraps the whole byte[] and treats it as owned by this instance.
        /// </summary>
        public AllocatedMemory(byte[] buffer)
            : this(buffer, buffer?.Length ?? 0)
        {
        }

        /// <summary>
        /// Wraps the first 'size' bytes of the given byte[] and treats it as owned.
        /// The underlying capacity is buffer.Length; Size is clamped to [0, buffer.Length].
        /// </summary>
        public AllocatedMemory(byte[] buffer, int size)
        {
            if (size < 0)
                throw new ArgumentOutOfRangeException(nameof(size));

            _buffer = buffer;

            if (buffer == null || buffer.Length == 0)
            {
                Buffer = Memory<byte>.Empty;
            }
            else
            {
                if (size > buffer.Length)
                    size = buffer.Length;

                Buffer = new Memory<byte>(buffer, 0, size);
            }
        }

        /// <summary>
        /// Clear the visible region
        /// </summary>
        public readonly void Clear()
        {
            Buffer.Span.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Span<byte> AsSpan(int start, int length) => Buffer.Slice(start, length).Span;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Span<byte> AsSpan(int start) => Buffer[start..].Span;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly unsafe void* GetPointer(int offset)
        {
            fixed (byte* ptr = Buffer.Span)
            {
                return ptr + offset;
            }
        }

        /// <summary>
        /// Ensures that the visible region has the requested size.
        ///
        /// Rules:
        /// - The underlying buffer is never shrunk or reallocated just to reduce the size.
        ///   Shrinking only adjusts the logical view (Size).
        /// - If the requested size is larger than the current Size:
        ///   - If this instance already owns a buffer and capacity is sufficient, only the view grows.
        ///   - Otherwise, a larger buffer is rented from the pool, existing data is copied,
        ///     and any previously owned buffer is returned to the pool.
        ///   - If the memory was originally external (no owned buffer), this is the point where
        ///     we first allocate and start owning a buffer.
        /// </summary>
        public void Expand(int size)
        {
            if (size < 0)
                throw new ArgumentOutOfRangeException(nameof(size));

            // size <= current logical length: only shrink or keep the view
            if (size <= Buffer.Length)
            {
                Buffer = Buffer.Slice(0, size);
                return;
            }

            // We need to grow the visible size.

            // Case 1: we already own a buffer and it has enough capacity.
            if (_buffer != null && size <= _buffer.Length)
            {
                Buffer = new Memory<byte>(_buffer, 0, size);
                return;
            }

            // Case 2: need a larger buffer (either no owned buffer or capacity is insufficient).
            var newBuffer = DefaultPool.Rent(size);

            // Copy existing data (from either external memory or old owned buffer).
            if (!Buffer.IsEmpty)
            {
                var src = Buffer.Span;
                src.CopyTo(newBuffer.AsSpan(0, src.Length));
            }

            // Return the previous owned buffer if present.
            if (_buffer != null)
            {
                DefaultPool.Return(_buffer);
            }

            _buffer = newBuffer;
            Buffer = new Memory<byte>(newBuffer, 0, size);
        }

        /// <summary>
        /// Releases any owned buffer back to the pool and clears the view.
        /// External memory is not touched.
        /// </summary>
        public void Dispose()
        {
            if (_buffer != null && !ReferenceEquals(_buffer, System.Array.Empty<byte>()))
            {
                DefaultPool.Return(_buffer);
                _buffer = null;
            }

            Buffer = default;
        }

        /// <summary>
        /// Rents a new buffer of at least the requested size from the default pool
        /// and returns an AllocatedMemory instance that owns it.
        /// The logical Size is set to exactly <paramref name="size"/>.
        /// Note that the actual underlying buffer length may be larger than 'size'.
        /// </summary>
        public static AllocatedMemory Create(int size)
        {
            if (size < 0)
                throw new ArgumentOutOfRangeException(nameof(size));

            var buffer = DefaultPool.Rent(size);
            return new AllocatedMemory(buffer, size);
        }

        /// <summary>
        /// Rents a new buffer of at least the requested size from the default pool
        /// and returns an AllocatedMemory instance that owns it.
        /// The logical Size is set to exactly the size of <paramref name="buffer"/>.
        /// Note that the actual underlying buffer length may be larger than 'size'.
        /// </summary>
        public static AllocatedMemory Create(ReadOnlySpan<byte> buffer)
        {
            var m = Create(buffer.Length);
            buffer.CopyTo(m.Buffer.Span);
            return m;
        }
    }
}
