using System;
using System.Collections.Concurrent;

namespace Amlos.Container
{
    /// <summary>
    /// A fixed-size byte[] pool that only rents/returns arrays of ONE exact length.
    /// - Thread-safe.
    /// - Returns exact-sized arrays (no power-of-two buckets like ArrayPool).
    /// - Uses a bounded ConcurrentStack; extra returns are dropped for GC to collect.
    /// </summary>
    public sealed class FixedBytePool
    {
        private readonly int _size;
        private readonly int _maxRetained;
        private readonly ConcurrentStack<byte[]> _stack = new();

        public int ElementSize => _size;
        public int RetainedCount => _stack.Count;

        public FixedBytePool(int size, int maxRetained = 64)
        {
            if (size < 0) throw new ArgumentOutOfRangeException(nameof(size));
            if (maxRetained < 0) throw new ArgumentOutOfRangeException(nameof(maxRetained));
            _size = size;
            _maxRetained = maxRetained;
        }

        /// <summary>
        /// Rent an array of exact size. Optionally clear it before returning.
        /// </summary>
        public byte[] Rent(bool zeroInit = false)
        {
            if (_size == 0) return Array.Empty<byte>();
            if (_stack.TryPop(out var buf))
            {
                if (zeroInit) Array.Clear(buf, 0, _size);
                return buf;
            }
            var fresh = new byte[_size];
            // If caller asked for zeroInit, new array already zeros by CLR.
            return fresh;
        }

        /// <summary>
        /// Return an array to the pool. Optionally clear the used slice for data hygiene.
        /// Arrays of wrong length are rejected.
        /// </summary>
        public void Return(byte[] buffer, bool clear = false)
        {
            if (buffer is null) return;
            if (buffer.Length == 0 && _size == 0) return;
            if (buffer.Length != _size)
                throw new ArgumentException($"Buffer length {buffer.Length} != pool element size {_size}.");

            if (clear) Array.Clear(buffer, 0, _size);
            // Bounded retention: drop extra arrays to avoid unbounded memory retention
            if (_stack.Count < _maxRetained)
                _stack.Push(buffer);
            // else: drop on the floor and let GC reclaim it
        }
    }

}
