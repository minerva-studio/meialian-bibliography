using System;
using System.Runtime.CompilerServices;

namespace Amlos.Container
{
    internal readonly ref struct FieldView
    {
        private readonly ContainerView container;
        private readonly int index;

        public FieldView(ContainerView container, int fieldIndex)
        {
            this.container = container;
            this.index = fieldIndex;
        }

        public ref FieldHeader Header => ref container.GetFieldHeader(index);
        public ReadOnlySpan<char> Name => container.GetFieldName(index);
        public Span<byte> Data => container.GetFieldBytes(index);
        /// <summary>
        /// Logical Length of the data
        /// </summary>
        public int Length => Header.Length;
        public bool IsRef => Header.IsRef;
        public bool IsArray => Header.FieldType.IsArray;
        public int Index => index;
        public ValueType Type => Header.Type;
        public FieldType FieldType => Header.FieldType;
        public ValueView ValueView => new ValueView(Data, Header.Type);


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FieldInfo ToFieldInfo() => new FieldInfo(Name, Header);
    }

    public readonly struct FieldInfo
    {
        public string Name { get; }
        public FieldHeader FieldHeader { get; }
        public int Length => FieldHeader.Length;
        public bool IsRef => FieldHeader.IsRef;


        internal FieldInfo(ReadOnlySpan<char> name, FieldHeader fieldHeader)
        {
            Name = name.ToString();
            FieldHeader = fieldHeader;
        }
    }
}
