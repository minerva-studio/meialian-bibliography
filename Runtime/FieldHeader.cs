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
        internal static short Size = (short)Unsafe.SizeOf<ContainerReference>();

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
        public static readonly FieldType Ref = new FieldType(ValueType.Ref, false);
        public static readonly FieldType RefArray = new FieldType(ValueType.Ref, true);

        public byte b;

        public FieldType(byte b) => this.b = b;

        public FieldType(ValueType valueType, bool isArray) : this()
        {
            b = TypeUtil.Pack(valueType, isArray);
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
        public static implicit operator byte(FieldType b) => b.b;
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
            return ref Unsafe.As<byte, ContainerHeader>(ref MemoryMarshal.GetReference(span));
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
            MemoryMarshal.Cast<byte, int>(span)[0] = size;
        }
    }
}
