using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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
        public bool IsArray => Header.FieldType.IsInlineArray;
        public int Index => index;
        public ValueType Type => Header.Type;
        public FieldType FieldType => Header.FieldType;
        public ReadOnlyValueView ValueView => new ReadOnlyValueView(Data, Header.Type);

        public readonly Span<T> GetSpan<T>() where T : unmanaged => MemoryMarshal.Cast<byte, T>(Data);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FieldInfo ToFieldInfo() => new FieldInfo(Name, Header);
    }

    public readonly struct FieldInfo
    {
        public string Name { get; }
        public FieldHeader FieldHeader { get; }
        public int Length => FieldHeader.Length;
        public bool IsRef => FieldHeader.IsRef;
        public bool IsInlineArray => FieldHeader.FieldType.IsInlineArray;


        internal FieldInfo(ReadOnlySpan<char> name, in FieldHeader fieldHeader)
        {
            Name = name.ToString();
            FieldHeader = fieldHeader;
        }

    }
}
