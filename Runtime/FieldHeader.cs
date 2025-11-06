using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Amlos.Container
{
    // =========================
    // Low-level type metadata
    // =========================
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct ContainerReference
    {
        public ulong id;

        public Container Container
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => Container.Registry.Shared.GetContainer(id);
            set => id = value?.ID ?? 0UL;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref ContainerReference FromSpan(Span<byte> span) => ref MemoryMarshal.Cast<byte, ContainerReference>(span)[0];

        public static implicit operator ContainerReference(ulong id) => new() { id = id };
        public static implicit operator ulong(ContainerReference cr) => cr.id;
        public static implicit operator ContainerReference(Container container) => new() { id = container?.ID ?? 0UL };
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct FieldType
    {
        public byte b;

        public FieldType(byte b) => this.b = b;

        public FieldType(ValueType valueType, bool v) : this()
        {
            b = TypeUtil.Pack(valueType, v);
        }

        /// <summary>Primitive value type (no marshalling semantics).</summary>
        public ValueType Type
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => TypeUtil.PrimOf(b);
            set => TypeUtil.SetType(ref b, value);
        }

        /// <summary>Whether this field stores an array of elements.</summary>
        public bool IsArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => TypeUtil.IsArray(b);
            set => TypeUtil.SetArray(ref b, value);
        }

        /// <summary>Size in bytes of a single element for this type.</summary>
        public readonly int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => TypeUtil.SizeOf(Type);
        }

        public readonly bool IsRef
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return Type == ValueType.Ref;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static FieldType Of<T>(bool isArray) where T : unmanaged => TypeUtil.Pack(TypeUtil.PrimOf<T>(), isArray);

        public static implicit operator FieldType(byte b) => new FieldType(b);
    }

    // =========================
    // On-buffer headers
    // =========================

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct FieldHeader
    {
        public static readonly int Size = Unsafe.SizeOf<FieldHeader>();

        public int NameHash;
        /// <summary>
        /// in byte
        /// </summary>
        public int NameOffset;
        /// <summary>
        /// in char count
        /// </summary>
        public short NameLength;
        public FieldType FieldType;        // ValueKind
        public byte Reserved;
        public int DataOffset;
        public short ElemSize;
        public int Length;      // total bytes

        public readonly bool IsRef
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => FieldType.IsRef;
        }

        public readonly ValueType Type
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => FieldType.Type;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref FieldHeader FromSpan(Span<byte> span) => ref MemoryMarshal.Cast<byte, FieldHeader>(span)[0];
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct ContainerHeader
    {
        public static readonly int Size = Unsafe.SizeOf<ContainerHeader>();

        public ulong Id;
        public int Version;
        public int FieldCount;
        public int NameOffset;
        public int DataOffset;

        public static ref ContainerHeader FromSpan(Span<byte> span)
        {
            if (span.Length < Size)
                throw new ArgumentException("Buffer too small.");
            return ref Unsafe.As<byte, ContainerHeader>(ref MemoryMarshal.GetReference(span));
        }

        public static void WriteEmptyHeader(Span<byte> span, ulong id, int version)
        {
            if (span.Length < Size)
                throw new ArgumentException("Buffer too small.", nameof(span));
            ref ContainerHeader header = ref FromSpan(span);
            header.Id = id;
            header.Version = version;
            header.FieldCount = 0;
            header.NameOffset = Size;
            header.DataOffset = Size; // data starts right after header
        }
    }

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
            this.bytes = bytes;
        }




        /// <summary>Reference to the container header (on the same buffer).</summary>
        public ref ContainerHeader Header => ref ContainerHeader.FromSpan(bytes[..ContainerHeader.Size]);

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
                return MemoryMarshal.Cast<byte, FieldHeader>(
                    bytes.Slice(ContainerHeader.Size, count * FieldHeader.Size));
            }
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
        public ref FieldHeader GetFieldHeader(int index) => ref Fields[index];

        /// <summary>Get UTF-16 field name by index without allocations.</summary>
        public ReadOnlySpan<char> GetFieldName(int index)
        {
            ref readonly FieldHeader fh = ref GetFieldHeader(index);
            int byteOffset = checked((int)fh.NameOffset);
            int byteLength = checked(fh.NameLength * sizeof(char));
            BoundsCheck(byteOffset, byteLength);
            return MemoryMarshal.Cast<byte, char>(bytes.Slice(byteOffset, byteLength));
        }

        /// <summary>
        /// Get the raw byte slice of a field's value.
        /// Effective ABSOLUTE start = Header.DataOffset + Field.DataOffset.
        /// </summary>
        public Span<byte> GetFieldBytes(int index)
        {
            ref readonly FieldHeader fh = ref GetFieldHeader(index);
            int start = checked((int)(Header.DataOffset + fh.DataOffset));
            int size = checked((int)fh.Length);
            BoundsCheck(start, size);
            return bytes.Slice(start, size);
        }





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
