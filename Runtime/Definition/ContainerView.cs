using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    // =========================
    // Container view (no copies)
    // =========================

    /// <summary>
    /// Zero-allocation view over a contiguous container buffer.
    /// All offsets are interpreted relative to the beginning of the provided buffer.
    /// </summary>
    internal readonly ref struct ContainerView
    {
        private readonly Span<byte> bytes;

        /// <summary>Create a view. The span must contain at least a ContainerHeader.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ContainerView(Span<byte> bytes)
        {
            if (bytes.Length < ContainerHeader.Size)
                throw new ArgumentException("Buffer too small for ContainerHeader.", nameof(bytes));
            var length = Unsafe.ReadUnaligned<int>(ref bytes[0]);
            int sizeNoExceed = bytes.Length > length ? length : bytes.Length;
            // read then, set to within length range
            this.bytes = bytes[..sizeNoExceed];
        }




        /// <summary>Reference to the container header (on the same buffer).</summary>
        public ref ContainerHeader Header => ref ContainerHeader.FromSpan(bytes);

        /// <summary>
        /// Logical length of the container
        /// </summary>
        public int Length => Header.Length;

        /// <summary>
        /// Number of fields.
        /// </summary>
        public int FieldCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return Header.FieldCount;
            }
        }

        /// <summary>
        /// The contiguous array of FieldHeader directly following the container header,
        /// ending right before DataPool begins.
        /// </summary>
        public Span<FieldHeader> Fields
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                int count = FieldCount; // validates structure
                return MemoryMarshal.Cast<byte, FieldHeader>(bytes.Slice(ContainerHeader.Size, count * FieldHeader.Size));
            }
        }

        /// <summary>
        /// Span of the container
        /// </summary>
        internal Span<byte> Span => bytes;

        /// <summary>
        /// Length before data segments
        /// </summary>
        public Span<byte> HeadersSegment
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => bytes[..Header.DataOffset];
        }

        /// <summary>
        /// name segment starts at an ABSOLUTE offset (<see cref="ContainerHeader.NameOffset"/>) and extends to the beginning of the data segment.
        /// </summary>
        internal Span<byte> NameSegment
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => bytes[(int)Header.NameOffset..Header.DataOffset];
        }

        /// <summary>
        /// DataPool starts at an ABSOLUTE offset (<see cref="ContainerHeader.DataOffset"/>)  and extends to the end of the buffer.
        /// </summary>
        public Span<byte> DataSegment
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => bytes[(int)Header.DataOffset..];
        }

        /// <summary>
        /// Get the field view by index.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public readonly FieldView this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => GetField(index);
        }

        /// <summary>
        /// Get the field view by name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public readonly FieldView this[ReadOnlySpan<char> name]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => GetField(IndexOf(name));
        }






        /// <summary>Get a reference to a field view by index.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FieldView GetField(int index) => new(this, index);

        /// <summary>Get a reference to a field header by index.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref FieldHeader GetFieldHeader(int index) => ref Unsafe.As<byte, FieldHeader>(ref bytes[ContainerHeader.Size + FieldHeader.Size * index]);

        /// <summary>Get UTF-16 field name by index without allocations.</summary>
        public ReadOnlySpan<char> GetFieldName(int index)
        {
            var fh = Fields[index];
            var bytes = Span.Slice(fh.NameOffset, fh.NameLength * sizeof(char));
            return MemoryMarshal.Cast<byte, char>(bytes);
        }

        /// <summary>
        /// Get the raw byte slice of a field's value.
        /// Effective ABSOLUTE start = Header.DataOffset + Field.DataOffset.
        /// </summary>
        public Span<byte> GetFieldBytes(int index)
        {
            var fh = Fields[index];
            return Span.Slice(fh.DataOffset, fh.Length);
        }


        /// <summary>
        /// Get the raw byte slice of a field's value.
        /// Effective ABSOLUTE start = Header.DataOffset + Field.DataOffset.
        /// </summary>
        public Span<T> GetFieldBytes<T>(int index) where T : unmanaged => MemoryMarshal.Cast<byte, T>(GetFieldBytes(index));





        /// <summary>
        /// Binary search for a field index by its UTF-16 name (ordinal).
        /// Returns -1 if not found.
        /// Preconditions:
        ///   - The field headers are sorted by name in ascending ordinal order.
        ///   - Names are stored as UTF-16 (char) in the same buffer.
        /// </summary>
        /// <param name="name">Target field name as a read-only char span.</param>
        /// <returns>Index in the Fields array, or -1 if missing.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int IndexOf(ReadOnlySpan<char> name)
        {
            int lo = 0;
            int hi = FieldCount - 1;

            // Standard binary search (ordinal compare on UTF-16 chars)
            while (lo <= hi)
            {
                // Unsigned shift to avoid overflow on large ranges
                int mid = (int)((uint)(lo + hi) >> 1);
                ReadOnlySpan<char> midName = GetFieldName(mid);

                // Ordinal (code-point) lexicographic comparison
                int cmp = midName.SequenceCompareTo(name);

                if (cmp == 0)
                    return mid;

                if (cmp < 0)
                    lo = mid + 1;
                else
                    hi = mid - 1;
            }

            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BoundsCheck(int start, int len)
        {
            if ((uint)start > (uint)bytes.Length || (uint)len > (uint)(bytes.Length - start))
                throw new IndexOutOfRangeException("Slice out of container buffer bounds.");
        }
    }
}
