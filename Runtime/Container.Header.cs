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

    internal struct FieldType
    {
        public byte b;

        public FieldType(byte b) => this.b = b;

        public ValueType Type
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => TypeUtil.PrimOf(b);
            set => TypeUtil.SetType(ref b, value);
        }

        public bool IsArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => TypeUtil.IsArray(b);
            set => TypeUtil.SetArray(ref b, value);
        }

        public readonly int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return TypeUtil.SizeOf(Type);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static FieldType Of<T>(bool isArray) where T : unmanaged => TypeUtil.Pack(TypeUtil.PrimOf<T>(), isArray);

        public static implicit operator FieldType(byte b) => new FieldType(b);
    }
}