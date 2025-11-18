using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Header-only, schema-like layout built from ObjectBuilder.
    /// Contains a compact header blob: [ContainerHeader][FieldHeader...][Names]
    /// and the total container length (including data segment).
    /// You can instantiate new zero-initialized containers from this layout.
    /// </summary>
    public sealed class ContainerLayout
    {
        public const string ArrayName = "value";
        public static readonly ContainerLayout Empty = CreateEmptyHeaderBytes();




        /// <summary>Header blob bytes: [ContainerHeader][FieldHeader...][Names], without data.</summary>
        readonly byte[] headerBlob;

        /// <summary>
        /// Length of headers [ContainerHeader][FieldHeader...][Names]
        /// also the offset where Data segment begins (absolute).
        /// </summary>
        public int Length => Header.DataOffset;

        /// <summary>Total length of a container for this layout (including data segment).</summary>
        public int TotalLength => Header.Length;

        /// <summary>Offset where Names segment begins (absolute).</summary>
        public int NameOffset => Header.NameOffset;

        /// <summary>Number of fields.</summary>
        public int FieldCount => Header.FieldCount;

        public ContainerHeader Header => ContainerHeader.FromSpan(headerBlob);

        public ReadOnlySpan<FieldHeader> Fields => MemoryMarshal.Cast<byte, FieldHeader>(headerBlob.AsSpan(ContainerHeader.Size, Header.FieldCount * FieldHeader.Size));

        public ReadOnlySpan<byte> Span => headerBlob.AsSpan(0, Length);

        public ContainerLayoutFieldView this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => GetField(index);
        }




        internal ContainerLayout(byte[] headerBlob)
        {
            this.headerBlob = headerBlob ?? throw new ArgumentNullException(nameof(headerBlob));
        }





        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ContainerLayoutFieldView GetField(int index) => new(headerBlob, index);




        private static ContainerLayout CreateEmptyHeaderBytes()
        {
            // container with only header bytes
            byte[] emptyHeader = new byte[ContainerHeader.Size];
            ContainerHeader.WriteEmptyHeader(emptyHeader, Container.Version);
            return new ContainerLayout(emptyHeader);
        }




        /// <summary>
        /// Build array of scalar element
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="length"></param>
        /// <returns></returns>
        public static ContainerLayout BuildArray<T>(int length) where T : unmanaged
        {
            if (TypeUtil.PrimOf<T>() == ValueType.Blob)
                return BuildBlobArray(Unsafe.SizeOf<T>(), length);

            var b = new ObjectBuilder();
            b.SetArray<T>(ArrayName, length);
            return b.BuildLayout();
        }

        /// <summary>
        /// Build array of fixed sized element
        /// </summary>
        /// <param name="valueType"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public static ContainerLayout BuildFixedArray(ValueType valueType, int length)
        {
            var b = new ObjectBuilder();
            b.SetArray(ArrayName, valueType, length);
            return b.BuildLayout();
        }

        /// <summary>
        /// Build a blob array
        /// </summary>
        /// <param name="elementSize"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public static ContainerLayout BuildBlobArray(int elementSize, int length)
        {
            var b = new ObjectBuilder();
            b.SetBlobArray(ArrayName, elementSize, length);
            return b.BuildLayout();
        }




        public override string ToString()
        {
            StringBuilder sb = new();
            var view = new ContainerView(headerBlob);
            for (int i = 0; i < FieldCount; i++)
            {
                var f = view[i];
                sb.Append(f.FieldType);
                sb.Append(" ");
                sb.Append(f.Name);
                sb.Append(": ");
                sb.Append(f.Length);
            }
            return sb.ToString();
        }
    }



    public readonly ref struct ContainerLayoutFieldView
    {
        private readonly Span<byte> bytes;
        private readonly int index;

        public ContainerLayoutFieldView(Span<byte> bytes, int fieldIndex)
        {
            this.bytes = bytes;
            this.index = fieldIndex;
        }

        public FieldHeader Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return MemoryMarshal.Cast<byte, FieldHeader>(bytes.Slice(ContainerHeader.Size + index * FieldHeader.Size, FieldHeader.Size))[0];
            }
        }

        public ReadOnlySpan<char> Name => MemoryMarshal.Cast<byte, char>(bytes.Slice(Header.NameOffset, Header.NameLength * sizeof(char)));
        /// <summary>
        /// Logical Length of the data
        /// </summary>
        public int Length => Header.Length;
        public bool IsRef => Header.IsRef;
        public bool IsArray => Header.FieldType.IsInlineArray;
        public int Index => index;
        public ValueType Type => Header.Type;
        public FieldType FieldType => Header.FieldType;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FieldInfo ToFieldInfo() => new FieldInfo(Name, Header);
    }
}
