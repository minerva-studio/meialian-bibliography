using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Amlos.Container
{
    internal readonly ref struct Header
    {
        public Span<byte> Span { get; }
        public int Length => Span.Length;

        public Header(Span<byte> bytes)
        {
            Span = bytes;
        }

        public FieldTypeRef this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Span.Slice(index, 1));
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Span[index] = value.Span[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsArray(int index) => TypeUtil.IsArray(Span[index]);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetArray(int index, bool isArray) => TypeUtil.SetArray(ref Span[index], isArray);
    }

    internal readonly ref struct FieldTypeRef
    {
        public readonly Span<byte> Span;

        public FieldTypeRef(ref byte b) => Span = MemoryMarshal.CreateSpan(ref b, 1);
        public FieldTypeRef(Span<byte> b) => Span = b;

        public ValueType Type
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => TypeUtil.PrimOf(Span[0]);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => TypeUtil.SetType(ref Span[0], value);
        }

        public bool IsArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => TypeUtil.IsArray(Span[0]);
        }
    }
}