using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Amlos.Container
{
    /// <summary>
    /// Object builder that caches field values in memory and emits a compact byte[] container.
    /// Supports add/update/remove. Deterministic order by field name (Ordinal).
    /// </summary>
    public sealed class ObjectBuilder
    {
        class ReadOnlyMemoryComparer : IEqualityComparer<ReadOnlyMemory<char>>, IComparer<ReadOnlyMemory<char>>
        {
            public static readonly ReadOnlyMemoryComparer Default = new();

            public bool Equals(ReadOnlyMemory<char> x, ReadOnlyMemory<char> y) => x.Span.SequenceEqual(y.Span);
            public int Compare(ReadOnlyMemory<char> x, ReadOnlyMemory<char> y) => x.Span.SequenceCompareTo(y.Span);
            int IEqualityComparer<ReadOnlyMemory<char>>.GetHashCode(ReadOnlyMemory<char> obj) => ReadOnlyMemoryComparer.GetHashCode(obj);

            public static int GetHashCode(ReadOnlyMemory<char> obj)
            {
                HashCode h = new HashCode();
                foreach (var c in obj.Span)
                    h.Add(c);
                return h.ToHashCode();
            }

        }

        private struct Entry
        {
            public FieldType Type;   // FieldType.b
            public short ElemSize;  // element size
            public byte[] Data;      // payload bytes (Length = total bytes). For refs: 8 bytes. 
        }




        private readonly SortedList<ReadOnlyMemory<char>, Entry> _map = new(ReadOnlyMemoryComparer.Default);

        int Version { get; set; } = Container.Version;


        // --------------------------
        // Mutating API
        // --------------------------

        /// <summary>Add or replace a raw byte payload.</summary>
        public void SetRaw(ReadOnlyMemory<char> name, byte type, int elemSize, ReadOnlySpan<byte> data)
        {
            if (name.Length == 0) throw new ArgumentNullException(nameof(name));
            if (elemSize < 0 || elemSize > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(elemSize));
            var arr = data.ToArray(); // copy into cache
            _map[name] = new Entry { Type = type, ElemSize = (short)elemSize, Data = arr };
        }

        /// <summary>Remove a field (no-op if absent).</summary>
        public bool Remove(ReadOnlyMemory<char> name) => _map.Remove(name);

        public void Clear() => _map.Clear();

        // Convenience writers:

        /// <summary>Set a scalar value of unmanaged T.</summary>
        public unsafe void SetScalar<T>(ReadOnlyMemory<char> name, FieldType type) where T : unmanaged
        {
            int size = sizeof(T);
            var buf = new byte[size];
            fixed (byte* p = buf)
                *(T*)p = default; // caller may prefer this overload with value; see below
            _map[name] = new Entry { Type = type, ElemSize = (short)size, Data = buf };
        }

        /// <summary>Set a scalar value of unmanaged T.</summary>
        public unsafe void SetScalar(ReadOnlyMemory<char> name, FieldType type)
        {
            var size = TypeUtil.SizeOf(type.Type);
            if (size == 0)
                throw new ArgumentOutOfRangeException(nameof(type));
            var buf = new byte[size];
            _map[name] = new Entry { Type = type, ElemSize = (short)size, Data = buf };
        }

        /// <summary>Set a scalar value of unmanaged T with value.</summary>
        public unsafe void SetScalar<T>(ReadOnlyMemory<char> name, FieldType type, in T value) where T : unmanaged
        {
            int size = sizeof(T);
            var buf = new byte[size];
            fixed (byte* p = buf)
                *(T*)p = value;
            _map[name] = new Entry { Type = type, ElemSize = (short)size, Data = buf };
        }

        public void SetScalar(ReadOnlyMemory<char> name, FieldType type, Span<byte> value)
        {
            int size = value.Length;
            var buf = new byte[size];
            value.CopyTo(buf);
            _map[name] = new Entry { Type = type, ElemSize = (short)size, Data = buf };
        }


        /// <summary>Set a reference id (8 bytes, little-endian).</summary>
        public void SetRef(ReadOnlyMemory<char> name, ulong id, FieldType type /* should carry IsRef bit */)
        {
            var buf = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(buf, id);
            _map[name] = new Entry { Type = type, ElemSize = 8, Data = buf };
        }

        /// <summary>Set an array payload of unmanaged T.</summary>
        public unsafe void SetArray(ReadOnlyMemory<char> name, FieldType type, int arraySize)
        {
            int elem = type.Size;
            int length = elem * arraySize;
            _map[name] = new Entry { Type = type, ElemSize = (short)elem, Data = new byte[length] };
        }

        /// <summary>Set an array payload of unmanaged T.</summary>
        public unsafe void SetArray<T>(ReadOnlyMemory<char> name, FieldType type, ReadOnlySpan<T> items) where T : unmanaged
        {
            int elem = sizeof(T);
            int len = elem * items.Length;
            var buf = new byte[len];
            fixed (T* src = items)
            fixed (byte* dst = buf)
                Buffer.MemoryCopy(src, dst, len, len);
            _map[name] = new Entry { Type = type, ElemSize = (short)elem, Data = buf };
        }

        /// <summary>Set a raw byte array (ElemSize = 1).</summary>
        public void SetBytes(ReadOnlyMemory<char> name, byte type, ReadOnlySpan<byte> bytes)
            => SetRaw(name, type, 1, bytes);



        /// <summary>
        /// Count number of bytes required to serialize the container.
        /// </summary>
        /// <returns></returns>
        internal int CountByte()
        {
            if (_map.Count == 0)
            {
                return ContainerHeader.Size;
            }

            int n = _map.Count;

            // 2) Names blob plan
            int namesBytes = 0;
            for (int i = 0; i < n; i++)
            {
                var name = _map.Keys[i];
                namesBytes += name.Length * sizeof(char);
            }

            // 3) Fixed offsets
            int fhSize = n * FieldHeader.Size;
            int nameStart = ContainerHeader.Size + fhSize;
            int dataStart = nameStart + namesBytes;

            // 4) Compute field headers (DataOffset relative to DataStart)
            uint running = 0;
            //var fhs = new FieldHeader[n];
            for (int i = 0; i < n; i++)
            {
                var e = _map.Values[i];
                running += (uint)e.Data.Length;
            }

            // 5) Allocate final buffer
            int total = dataStart + (int)running;
            return total;
        }

        /// <summary>
        /// Materialize the container as a compact byte[] with fresh layout.
        /// Order: [ContainerHeader][FieldHeader...][Names][Data].
        /// </summary>
        internal Container Build() => Build(null);
        internal Container Build(byte[] target)
        {
            if (_map.Count == 0)
            {
                return Container.Empty;
            }

            int n = _map.Count;

            // 2) Names blob plan
            int namesBytes = 0;
            for (int i = 0; i < n; i++)
            {
                var name = _map.Keys[i];
                namesBytes += name.Length * sizeof(char);
            }

            // 3) Fixed offsets
            int fhSize = n * FieldHeader.Size;
            int nameStart = ContainerHeader.Size + fhSize;
            int dataStart = nameStart + namesBytes;

            // 4) Compute field headers (DataOffset relative to DataStart)
            int running = 0;
            //var fhs = new FieldHeader[n];
            for (int i = 0; i < n; i++)
            {
                var e = _map.Values[i];
                running += (int)e.Data.Length;
            }

            // 5) Allocate final buffer
            int total = dataStart + (int)running;
            ContainerView view;
            Container container = null;
            if (target == null)
            {
                container = Container.Registry.Shared.CreateWild(total);
                view = container.View;
            }
            else
            {
                view = new ContainerView(target);
            }

            // 6) Write ContainerHeader 
            ref var h2 = ref view.Header;
            h2.Version = Version;
            h2.FieldCount = _map.Count;
            h2.NameOffset = nameStart;
            h2.DataOffset = dataStart;

            // 7) Write FieldHeaders
            var fields = view.Fields;
            int nameOffset = 0;
            for (int i = 0; i < _map.Count; i++)
            {
                var name = _map.Keys[i];
                var e = _map.Values[i];
                fields[i] = new FieldHeader
                {
                    NameHash = ReadOnlyMemoryComparer.GetHashCode(name),
                    NameOffset = (nameStart + nameOffset),
                    NameLength = (short)name.Length,
                    FieldType = e.Type,
                    Reserved = 0,
                    DataOffset = running,
                    ElemSize = e.ElemSize,
                    Length = (int)e.Data.Length,
                };
                nameOffset += name.Length * sizeof(char);
            }

            // 8) Write Names blob (UTF-16)
            int nameCursor = 0;
            var dst = view.NameSegment;
            for (int i = 0; i < _map.Count; i++)
            {
                var s = _map.Keys[i];
                var byteCount = s.Length * sizeof(char);
                MemoryMarshal.AsBytes(s.Span).CopyTo(dst.Slice(nameCursor, byteCount));
                nameCursor += byteCount;
            }

            // 9) Write Data payloads
            int dataCursor = dataStart;
            for (int i = 0; i < _map.Count; i++)
            {
                var data = _map.Values[i].Data;
                data.CopyTo(dst.Slice(dataCursor, data.Length));
                dataCursor += data.Length;
            }

            return container;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint Hash32_FNV1a(string s)
        {
            const uint offset = 2166136261u;
            const uint prime = 16777619u;
            uint h = offset;
            foreach (char c in s)
            {
                h ^= (byte)c; h *= prime;
                h ^= (byte)(c >> 8); h *= prime;
            }
            return h;
        }
    }
}
