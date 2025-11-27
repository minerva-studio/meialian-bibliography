using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{

    // =========================
    // On-buffer headers
    // =========================

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct FieldHeader
    {
        public static readonly int Size = Unsafe.SizeOf<FieldHeader>();

        public int NameHash;

        /// <summary>
        /// Position of name, in byte (abs position)
        /// </summary>
        public int NameOffset;
        /// <summary>
        /// Length of name, in char count
        /// </summary>
        public short NameLength;
        /// <summary>
        /// field type
        /// </summary>
        public FieldType FieldType;
        public byte Reserved;//padding
        /// <summary>
        /// Position of data (abs position)
        /// </summary>
        public int DataOffset;
        /// <summary>
        /// Size of element (for inline array)
        /// </summary>
        public short ElemSize;
        /// <summary>
        /// Length of data, in byte
        /// </summary>
        public int Length;      // total bytes


        public readonly bool IsRef
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => FieldType.IsRef;
        }
        public TypeData ElementType
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => new(FieldType.Type, ElemSize);
            set
            {
                FieldType = new FieldType(value.ValueType, IsInlineArray);
                ElemSize = value.Size;
            }
        }
        public readonly ValueType Type
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => FieldType.Type;
        }
        public readonly bool IsInlineArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => FieldType.IsInlineArray;
        }

        /// <summary>
        /// The elememt count (for inline array; 1 for non-array; otherwise some error occured)
        /// </summary>
        public readonly int ElementCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Length / ElemSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref FieldHeader FromSpan(Span<byte> span) => ref MemoryMarshal.Cast<byte, FieldHeader>(span)[0];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref FieldHeader FromSpanAndFieldIndex(Span<byte> span, int i)
        {
            return ref FromSpanAndFieldIndex(span, ContainerHeader.Size, i);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref FieldHeader FromSpanAndFieldIndex(Span<byte> span, int baseOffset, int i)
        {
            int offset = baseOffset + FieldHeader.Size * i;
            return ref MemoryMarshal.Cast<byte, FieldHeader>(span.Slice(offset, Size))[0];
        }
    }
}
