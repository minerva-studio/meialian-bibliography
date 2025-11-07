using System;
using System.Runtime.CompilerServices;

namespace Amlos.Container
{
    /// <summary>
    /// Value-type descriptor of a field layout (Name, Length, Offset).
    /// - Negative Length encodes a "reference field": its absolute byte length is |Length|, which must be a multiple of REF_SIZE (8).
    /// - Positive Length encodes a plain byte field with that many bytes.
    /// - Offset is the packed byte offset (>= 0).
    /// Immutable-by-contract: create new instances via factories or WithOffset/WithBaseInfo.
    /// </summary>
    public readonly struct FieldDescriptor_Old : IEquatable<FieldDescriptor_Old>
    {
        /// <summary>Size, in bytes, of one reference element (a child ID).</summary>
        public const int REF_SIZE = 8;

        public string Name { get; }
        /// <summary>
        /// Encoded length. If &lt; 0, this is a reference field and the byte length is -Length.
        /// If &gt;= 0, this is a plain field and the byte length is Length.
        /// </summary>
        public int Length { get; }
        public int Offset { get; }

        /// <summary>True if this field stores one or more reference IDs.</summary>
        public bool IsRef => Length < 0;

        /// <summary>Absolute byte length used for packing and slicing.</summary>
        public int AbsLength => Length < 0 ? -Length : Length;

        /// <summary>Number of reference elements (IDs) when IsRef==true; otherwise 0.</summary>
        public int RefCount => Length < 0 ? (-Length / REF_SIZE) : 0;

        /// <summary>True if this is a reference array (2+ elements).</summary>
        public bool IsRefArray => IsRef && RefCount > 1;

        private FieldDescriptor_Old(string name, int length, int offset)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Length = length;
            Offset = offset;
        }

        /// <summary>Create a copy with a different offset.</summary>
        public FieldDescriptor_Old WithOffset(int offset) => new FieldDescriptor_Old(Name, Length, offset);

        /// <summary>Create a copy with offset of 0 (base info only).</summary>
        public FieldDescriptor_Old WithBaseInfo() => new FieldDescriptor_Old(Name, Length, 0);

        public int GetElementCount<T>() where T : unmanaged
        {
            var size = Unsafe.SizeOf<T>();
            return Length / size;
        }





        public bool Equals(FieldDescriptor_Old other)
            => Length == other.Length && Offset == other.Offset &&
               string.Equals(Name, other.Name, StringComparison.Ordinal);

        public override bool Equals(object obj) => obj is FieldDescriptor_Old f && Equals(f);

        public override int GetHashCode() => HashCode.Combine(Name, Offset, Length);

        public static bool operator ==(FieldDescriptor_Old lhs, FieldDescriptor_Old rhs) => lhs.Equals(rhs);
        public static bool operator !=(FieldDescriptor_Old lhs, FieldDescriptor_Old rhs) => !lhs.Equals(rhs);

        // ---------- Factories ----------

        /// <summary>Create a fixed-size value field using sizeof(T).</summary>
        public static FieldDescriptor_Old Type<T>(string name) where T : unmanaged
            => Fixed(name, Unsafe.SizeOf<T>());

        /// <summary>Create a fixed-size value array field of T[count].</summary>
        public static FieldDescriptor_Old ArrayOf<T>(string name, int count) where T : unmanaged
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
            int elem = Unsafe.SizeOf<T>();
            long total = (long)elem * count;
            if (total > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(count), "Total byte length too large.");
            return Fixed(name, (int)total);
        }

        /// <summary>Create a single reference slot (8 bytes).</summary>
        public static FieldDescriptor_Old Reference(string name) => new FieldDescriptor_Old(name, -REF_SIZE, 0);

        /// <summary>Create an array of reference slots (count * 8 bytes).</summary>
        public static FieldDescriptor_Old ReferenceArray(string name, int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
            long total = (long)REF_SIZE * count;
            if (total > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(count), "Total byte length too large.");
            return new FieldDescriptor_Old(name, -(int)total, 0);
        }

        /// <summary>Create a fixed-size plain byte field.</summary>
        public static FieldDescriptor_Old Fixed(string name, int length)
        {
            return new FieldDescriptor_Old(name, length, 0);
        }
    }
}
