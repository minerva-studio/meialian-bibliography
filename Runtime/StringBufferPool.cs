using System;
using System.Buffers;

namespace Amlos.Container
{
    internal readonly struct TemporaryString : IDisposable
    {
        private static readonly ArrayPool<char> pool = ArrayPool<char>.Create();

        public char[] Buffer { get; }
        public ReadOnlyMemory<char> Memory => Buffer;

        internal TemporaryString(ReadOnlySpan<char> str)
        {
            Buffer = pool.Rent(str.Length);
        }

        public void Dispose()
        {
            pool.Return(Buffer);
        }
    }
}
