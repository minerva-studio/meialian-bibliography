using System;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// A record of type information for a specific ValueType.
    /// </summary>
    public readonly struct TypeData : IEquatable<TypeData>
    {
        public static readonly int StructSize = Unsafe.SizeOf<TypeData>();
        public static readonly TypeData Unknown = new TypeData(ValueType.Unknown, 0);
        public static readonly TypeData Ref = new TypeData(ValueType.Ref, ContainerReference.Size);


        public readonly ValueType ValueType;
        public readonly short Size;

        public TypeData(ValueType valueType, short size)
        {
            ValueType = valueType;
            Size = size;
        }

        public static TypeData Of<T>() where T : unmanaged
        {
            ValueType valueType = TypeUtil<T>.ValueType;
            int size = TypeUtil<T>.Size;
            return new TypeData(valueType, (short)size);
        }


        public static TypeData? Of(ValueType? valueType, int? elementSize) => valueType == null ? null : Of(valueType.Value, elementSize);
        public static TypeData Of(ValueType valueType, int size) => new TypeData(valueType, (short)size);
        public static TypeData Of(ValueType valueType, short size) => new TypeData(valueType, size);
        public static TypeData Blob(int size) => new TypeData(ValueType.Blob, (short)size);
        public static TypeData Of(ValueType valueType, int? size = null)
        {
            if (valueType == ValueType.Unknown || valueType == ValueType.Blob)
                ThrowHelper.ArgumentException("Cannot create TypeData for Unknown ValueType", nameof(valueType));

            size ??= TypeUtil.SizeOf(valueType);
            return new TypeData(valueType, (short)size);
        }

        public static explicit operator TypeData(ValueType valueType) => Of(valueType);
        public static implicit operator ValueType(TypeData type) => type.ValueType;





        public bool Equals(TypeData other)
        {
            return ValueType == other.ValueType && Size == other.Size;
        }

        public override string ToString()
        {
            return $"{ValueType} (Size: {Size})";
        }

        public override int GetHashCode()
        {
            return HashCode.Combine((int)ValueType, Size);
        }

        public override bool Equals(object obj) => obj is TypeData other && Equals(other);

        public static bool operator ==(TypeData left, TypeData right) => left.Equals(right);
        public static bool operator !=(TypeData left, TypeData right) => !(left == right);
    }
}
