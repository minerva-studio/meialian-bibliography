using System;
using System.Buffers;

namespace Minerva.DataStorage
{
    public struct TempString : IDisposable
    {
        static ArrayPool<char> Pool = ArrayPool<char>.Create();

        char[] chars;

        public TempString(int capacity)
        {
            chars = Pool.Rent(capacity);
            Length = 0;
        }

        public TempString(string chars) : this(chars.AsSpan()) { }

        public TempString(ReadOnlySpan<char> chars) : this(chars.Length)
        {
            Append(chars);
        }

        public readonly bool IsDisposed => chars == null;
        public readonly Span<char> Span => chars.AsSpan(0, Length);
        public int Length { get; set; }

        public void Append(string str)
        {
            ThrowHelper.ThrowIfNull(chars, nameof(str));
            Append(str.AsSpan());
        }

        public void Append(ReadOnlySpan<char> str)
        {
            EnsureFreeSize(str.Length);
            str.CopyTo(chars.AsSpan(Length));
            Length += str.Length;
        }

        public void Append(char v)
        {
            EnsureFreeSize(1);
            chars[Length++] = v;
        }

        private void EnsureFreeSize(int size)
        {
            int remain = chars.Length - Length;
            if (size > remain)
            {
                // need to grow
                int newSize = Math.Max(chars.Length * 2, Length + size);
                var newChars = Pool.Rent(newSize);
                chars.AsSpan(0, Length).CopyTo(newChars.AsSpan(0, Length));
                Pool.Return(chars);
                chars = newChars;
            }
        }

        public void Dispose()
        {
            Pool.Return(chars);
            chars = null;
            Length = 0;
        }

        public readonly override string ToString()
        {
            return new string(chars, 0, Length);
        }

        public static implicit operator ReadOnlySpan<char>(in TempString tempString)
        {
            return tempString.Span;
        }
    }
}

