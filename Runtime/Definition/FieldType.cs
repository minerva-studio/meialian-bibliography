using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Amlos.Container
{
    /// <summary>
    /// Field Type | array bit 1 | type bit 4 | empty 2 |
    /// </summary>
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
        public bool IsInlineArray
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


        public override readonly string ToString() => TypeUtil.ToString(b);
    }
}
