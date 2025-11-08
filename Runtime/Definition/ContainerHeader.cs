using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Amlos.Container
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public unsafe struct ContainerHeader
    {
        public static readonly int Size = Unsafe.SizeOf<ContainerHeader>();
        public static readonly int FieldCountOffset = sizeof(int) * 2;

        public int Length;
        public int Version;
        public int FieldCount;
        /// <summary>
        /// Absolute offset to names
        /// </summary>
        public int NameOffset;
        /// <summary>
        /// Absolute offset to data
        /// </summary>
        public int DataOffset;

        public static ref ContainerHeader FromSpan(Span<byte> span)
        {
            if (span.Length < Size)
                throw new ArgumentException("Buffer too small.");
            return ref Unsafe.As<byte, ContainerHeader>(ref span[0]);
        }

        public static void WriteEmptyHeader(Span<byte> span, int version)
        {
            if (span.Length < Size)
                throw new ArgumentException("Buffer too small.", nameof(span));
            ref ContainerHeader header = ref FromSpan(span);
            header.Length = Size;
            header.Version = version;
            header.FieldCount = 0;
            header.NameOffset = Size;
            header.DataOffset = Size; // data starts right after header
        }

        public static void WriteLength(Span<byte> span, int size)
        {
            if (span.Length < Size)
                throw new ArgumentException("Buffer too small.", nameof(span));
            // first location of the header is the size
            Unsafe.Write(Unsafe.AsPointer(ref span[0]), size);
        }
    }
}
