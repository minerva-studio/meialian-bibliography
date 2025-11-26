using System;
using System.Buffers;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Simple disposable string builder
    /// </summary>
    public class TempString : IDisposable
    {
        static ObjectPool<TempString> TempStringPool = new ObjectPool<TempString>(() => new TempString(16));
        static ArrayPool<char> Pool = ArrayPool<char>.Create();

        char[] chars;
        string cache;
        int generation;

        private TempString(int capacity)
        {
            Init(capacity);
        }

        public bool IsDisposed => chars == null;
        public Span<char> Span => chars.AsSpan(0, Length);
        public int Length { get; set; }
        public int Generation => generation;
        public ref char this[int index] => ref chars[index];



        private void Init(int capacity)
        {
            chars = Pool.Rent(capacity);
            Length = 0;
            cache = "";
        }

        public void EnsureNotDisposed(int expectedGeneration)
        {
            if (generation != expectedGeneration)
                ThrowHelper.ThrowDisposed(nameof(TempString));
        }



        public TempString Append(string str)
        {
            ThrowHelper.ThrowIfNull(chars, nameof(str));
            return Append(str.AsSpan());
        }

        public TempString Append(ReadOnlySpan<char> str)
        {
            if (str.Length == 0) return this;
            EnsureFreeSize(str.Length);
            str.CopyTo(chars.AsSpan(Length));
            Length += str.Length;
            cache = null;
            return this;
        }

        public TempString Append(char v)
        {
            EnsureFreeSize(1);
            chars[Length++] = v;
            return this;
        }

        public TempString Prepend(string str)
        {
            ThrowHelper.ThrowIfNull(chars, nameof(str));
            return Prepend(str.AsSpan());
        }

        public TempString Prepend(ReadOnlySpan<char> str)
        {
            if (str.Length == 0) return this;
            EnsureFreeSize(str.Length);
            // shift existing
            chars.AsSpan(0, Length).CopyTo(chars.AsSpan(str.Length, Length));
            // copy new
            str.CopyTo(chars.AsSpan(0, str.Length));
            Length += str.Length;
            cache = null;
            return this;
        }

        public TempString Prepend(char v)
        {
            EnsureFreeSize(1);
            // shift existing
            chars.AsSpan(0, Length).CopyTo(chars.AsSpan(1, Length));
            // copy new
            chars[0] = v;
            Length += 1;
            cache = null;
            return this;
        }

        public int IndexOf(char v) => Array.IndexOf(chars, v);
        public int LastIndexOf(char v) => Array.LastIndexOf(chars, v);






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
            if (chars == null) return;
            Pool.Return(chars);
            TempStringPool.Return(this);
            chars = null;
            Length = 0;
            generation++;
        }

        public override string ToString()
        {
            cache ??= new string(chars, 0, Length);
            return cache;
        }

        public static implicit operator ReadOnlySpan<char>(in TempString tempString)
        {
            return tempString.Span;
        }


        public static TempString Create() => Create(16);
        public static TempString Create(int capacity)
        {
            var tempString = TempStringPool.Rent();
            tempString.Init(capacity);
            return tempString;
        }

        public static TempString Create(string chars) => Create(chars.AsSpan());

        public static TempString Create(ReadOnlySpan<char> chars)
        {
            return Create(chars.Length).Append(chars);
        }
    }
}

