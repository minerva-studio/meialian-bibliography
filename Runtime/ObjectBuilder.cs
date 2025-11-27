using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
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
            public FieldType Type;  // FieldType.b
            public short ElemSize;  // element size
            public Data Data;       // payload bytes (Length = total bytes). For refs: 8 bytes. 
        }

        private struct Data
        {
            byte[] buffer;
            int length;

            public int Length => length;
            public byte[] Buffer => buffer;

            public Span<byte> GetBuffer()
            {
                return this.buffer ??= (length == 0 ? Array.Empty<byte>() : new byte[length]);
            }

            public static implicit operator Data(byte[] bytes)
            {
                return new Data { buffer = bytes, length = bytes.Length };
            }

            public static implicit operator Data(int length)
            {
                return new Data { buffer = null, length = length };
            }
        }



        private ReadOnlyMemory<char> _name;
        private readonly SortedList<ReadOnlyMemory<char>, Entry> _map = new(ReadOnlyMemoryComparer.Default);

        int Version { get; set; } = 0;


        // --------------------------
        // Mutating API
        // --------------------------


        public void SetName(string name) => _name = name.AsMemory();

        internal void SetName(ReadOnlySpan<char> name)
        {
            _name = name.ToString().AsMemory();
        }




        /// <summary>Add or replace a raw byte payload.</summary>
        public ObjectBuilder SetRaw(string name, FieldType type, int elemSize, ReadOnlySpan<byte> data) => SetRaw(name.AsMemory(), type, elemSize, data);
        public ObjectBuilder SetRaw(ReadOnlyMemory<char> name, FieldType type, int elemSize, ReadOnlySpan<byte> data)
        {
            if (name.Length == 0) throw new ArgumentNullException(nameof(name));
            if (elemSize < 0 || elemSize > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(elemSize));
            var arr = data.ToArray(); // copy into cache
            _map[name] = new Entry { Type = type, ElemSize = (short)elemSize, Data = arr };
            return this;
        }


        /// <summary>Add or replace a raw byte payload.</summary>
        public void SetRaw(string name, in FieldHeader header) => SetRaw(name.AsMemory(), header);
        public void SetRaw(ReadOnlyMemory<char> name, in FieldHeader header)
        {
            if (name.Length == 0) throw new ArgumentNullException(nameof(name));
            var elemSize = header.ElemSize;
            if (elemSize < 0) throw new ArgumentOutOfRangeException(nameof(elemSize));
            _map[name] = new Entry { Type = header.FieldType, ElemSize = (short)elemSize, Data = header.Length };
        }


        /// <summary>Remove a field (no-op if absent).</summary>
        public bool Remove(string name) => _map.Remove(name.AsMemory());
        public bool Remove(ReadOnlyMemory<char> name) => _map.Remove(name);


        public void Clear() => _map.Clear();


        // Convenience writers:




        /// <summary>Set a scalar value of unmanaged T.</summary>
        public ObjectBuilder SetScalar<T>(string name) where T : unmanaged => SetScalar<T>(name.AsMemory());
        internal ObjectBuilder SetScalar<T>(ReadOnlyMemory<char> name) where T : unmanaged
        {
            int size = TypeUtil<T>.Size;
            _map[name] = new Entry { Type = TypeUtil<T>.ScalarFieldType, ElemSize = (short)size, Data = size };
            return this;
        }


        /// <summary>Set a scalar value of unmanaged T.</summary>
        public ObjectBuilder SetScalar(string name, ValueType type) => SetScalar(name.AsMemory(), type);
        internal ObjectBuilder SetScalar(ReadOnlyMemory<char> name, ValueType type)
        {
            var size = TypeUtil.SizeOf(type);
            if (size == 0)
                throw new ArgumentOutOfRangeException(nameof(type));

            var buf = new byte[size];
            _map[name] = new Entry { Type = type, ElemSize = (short)size, Data = buf };
            return this;
        }


        /// <summary>Set a scalar value of unmanaged T with value.</summary>
        public ObjectBuilder SetScalar<T>(string name, in T value) where T : unmanaged => SetScalar(name.AsMemory(), value);
        internal ObjectBuilder SetScalar<T>(ReadOnlyMemory<char> name, in T value) where T : unmanaged
        {
            int size = TypeUtil<T>.Size;
            var bytes = CreateDefaultValueBytes(value);
            _map[name] = new Entry { Type = TypeUtil<T>.ScalarFieldType, ElemSize = (short)size, Data = bytes };
            return this;
        }


        public ObjectBuilder SetScalar(string name, FieldType type, Span<byte> value) => SetScalar(name.AsMemory(), type, value);
        internal ObjectBuilder SetScalar(ReadOnlyMemory<char> name, FieldType type, Span<byte> value)
        {
            int size = value.Length;
            var buf = new byte[size];
            value.CopyTo(buf);
            _map[name] = new Entry { Type = type, ElemSize = (short)size, Data = buf };
            return this;
        }





        /// <summary>Set a reference id (8 bytes, little-endian).</summary>
        public ObjectBuilder SetRef(string name) => SetRef(name.AsMemory());
        public ObjectBuilder SetRef(ReadOnlyMemory<char> name)
        {
            _map[name] = new Entry { Type = FieldType.Ref, ElemSize = 8, Data = ContainerReference.Size };
            return this;
        }

        /// <summary>Set a reference id (8 bytes, little-endian).</summary>
        internal ObjectBuilder SetRef(string name, ulong id) => SetRef(name.AsMemory(), id);
        internal ObjectBuilder SetRef(ReadOnlyMemory<char> name, ulong id)
        {
            var buf = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(buf, id);
            _map[name] = new Entry { Type = FieldType.Ref, ElemSize = 8, Data = buf };
            return this;
        }

        /// <summary>Set a reference array (8 bytes, little-endian).</summary>
        public ObjectBuilder SetRefArray(string name, int arraySize) => SetRefArray(name.AsMemory(), arraySize);
        public ObjectBuilder SetRefArray(ReadOnlyMemory<char> name, int count)
        {
            var buf = new byte[checked(count * 8)];
            _map[name] = new Entry { Type = FieldType.RefArray, ElemSize = 8, Data = buf };
            return this;
        }





        /// <summary>Set an array payload of unmanaged T.</summary>
        public ObjectBuilder SetArray(string name, ValueType type, int arraySize) => SetArray(name.AsMemory(), type, arraySize);
        internal ObjectBuilder SetArray(ReadOnlyMemory<char> name, ValueType type, int arraySize)
        {
            // cannot create blobs because size is unknown
            if (type == ValueType.Blob)
                ThrowHelper.ArgumentException(nameof(type));

            var fieldType = new FieldType(type, true);
            int elem = fieldType.Size;
            int length = elem * arraySize;
            _map[name] = new Entry { Type = fieldType, ElemSize = (short)elem, Data = length };
            return this;
        }
        /// <summary>Set an array payload of unmanaged T.</summary>
        public ObjectBuilder SetArray(string name, TypeData type, int arraySize) => SetArray(name.AsMemory(), type, arraySize);
        internal ObjectBuilder SetArray(ReadOnlyMemory<char> name, TypeData type, int arraySize)
        {
            var fieldType = new FieldType(type.ValueType, true);
            int elem = type.Size;
            int length = elem * arraySize;
            _map[name] = new Entry { Type = fieldType, ElemSize = (short)elem, Data = length };
            return this;
        }

        /// <summary>Set an array payload of unmanaged T.</summary>
        public ObjectBuilder SetArray<T>(string name, int arraySize) where T : unmanaged => SetArray<T>(name.AsMemory(), arraySize);
        internal ObjectBuilder SetArray<T>(ReadOnlyMemory<char> name, int arraySize) where T : unmanaged
        {
            int elem = TypeUtil<T>.Size;
            int length = elem * arraySize;
            _map[name] = new Entry { Type = TypeUtil<T>.ArrayFieldType, ElemSize = (short)elem, Data = length };
            return this;
        }

        /// <summary>Set an array payload of unmanaged T.</summary>
        public ObjectBuilder SetArray<T>(string name, ReadOnlySpan<T> items) where T : unmanaged => SetArray<T>(name.AsMemory(), items);
        internal ObjectBuilder SetArray<T>(ReadOnlyMemory<char> name, ReadOnlySpan<T> items) where T : unmanaged
        {
            int elem = TypeUtil<T>.Size;
            int len = elem * items.Length;
            var buf = new byte[len];
            MemoryMarshal.AsBytes(items).CopyTo(buf);
            _map[name] = new Entry { Type = TypeUtil<T>.ArrayFieldType, ElemSize = (short)elem, Data = buf };
            return this;
        }

        /// <summary>Set an array payload of unmanaged T.</summary>
        public ObjectBuilder SetBlobArray(string name, int elemSize, int arraySize) => SetBlobArray(name.AsMemory(), elemSize, arraySize);
        internal ObjectBuilder SetBlobArray(ReadOnlyMemory<char> name, int elemSize, int arraySize)
        {
            var fieldType = new FieldType(ValueType.Blob, true);
            int length = elemSize * arraySize;
            _map[name] = new Entry { Type = fieldType, ElemSize = (short)elemSize, Data = length };
            return this;
        }




        /// <summary>Set a raw byte array (ElemSize = 1).</summary>
        public ObjectBuilder SetBytes(string name, byte type, ReadOnlySpan<byte> bytes) => SetBytes(name.AsMemory(), type, bytes);
        internal ObjectBuilder SetBytes(ReadOnlyMemory<char> name, byte type, ReadOnlySpan<byte> bytes) => SetRaw(name, type, 1, bytes);



        /// <summary>
        /// Get field buffer
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        internal Span<byte> GetBuffer(string name)
        {
            ReadOnlyMemory<char> readOnlyMemory = name.AsMemory();
            return GetBuffer(readOnlyMemory);
        }

        /// <summary>
        /// Get field buffer
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        internal Span<byte> GetBuffer(ReadOnlyMemory<char> readOnlyMemory)
        {
            Entry entry = _map[readOnlyMemory];
            var buffer = entry.Data.GetBuffer();
            _map[readOnlyMemory] = entry;
            return buffer;
        }



        /// <summary>
        /// Get field buffer
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name"></param>
        /// <returns></returns>
        internal Span<T> GetBuffer<T>(string name) where T : unmanaged => MemoryMarshal.Cast<byte, T>(GetBuffer(name));
        /// <summary>
        /// Get field buffer
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name"></param>
        /// <returns></returns>
        internal Span<T> GetBuffer<T>(ReadOnlyMemory<char> name) where T : unmanaged => MemoryMarshal.Cast<byte, T>(GetBuffer(name));




        public ObjectBuilder Variate(Action<ObjectBuilder> action)
        {
            action(this);
            return this;
        }





        /// <summary>
        /// Count number of bytes required to serialize the container.
        /// </summary>
        /// <returns></returns>
        internal int CountByte()
        {
            int headerSize = CountHeader();

            // Sum data length for total size
            int totalDataBytes = 0;
            for (int i = 0; i < _map.Count; i++)
                totalDataBytes += _map.Values[i].Data.Length;

            return headerSize + totalDataBytes;
        }

        internal int CountHeader()
        {
            var n = _map.Count;
            if (n == 0)
                return ContainerHeader.Size;

            // Names blob size (UTF-16)
            int namesBytes = 0;
            for (int i = 0; i < n; i++)
                namesBytes += _map.Keys[i].Length * sizeof(char);

            // Fixed offsets
            int fhSize = n * FieldHeader.Size;
            int nameStart = ContainerHeader.Size + fhSize;
            return nameStart + namesBytes;
        }



        internal void WriteTo(ref AllocatedMemory target)
        {
            if (_map.Count == 0)
            {
                ContainerHeader.WriteEmptyHeader(target.Buffer.Span, Version);
                return;
            }

            BuildLayout(ref target);

            // Write Data payloads using ABSOLUTE offsets into full buffer
            var buf = target.Buffer.Span;
            int dataCursor = ContainerHeader.FromSpan(buf).DataOffset; //view.Header.DataOffset;
            for (int i = 0; i < _map.Count; i++)
            {
                var data = _map.Values[i].Data;
                // absolute write
                data.Buffer?.CopyTo(buf.Slice(dataCursor, data.Length));
                dataCursor += data.Length;
            }
        }

        internal void BuildLayout(ref AllocatedMemory target, bool includeData = true, bool includeContainerName = true)
        {
            int n = _map.Count;

            // Names blob size (UTF-16)
            int namesBytes = 0;
            for (int i = 0; i < n; i++)
                namesBytes += _map.Keys[i].Length * sizeof(char);

            // Fixed offsets
            int fhSize = n * FieldHeader.Size;
            int baseNameStart = ContainerHeader.Size + fhSize;
            int nameStart = baseNameStart;
            if (includeContainerName) nameStart += _name.Length * sizeof(byte);
            int dataStart = nameStart + namesBytes;

            // Sum data length for total size
            int totalDataBytes = 0;
            for (int i = 0; i < n; i++)
                totalDataBytes += _map.Values[i].Data.Length;

            int allocSize = dataStart;
            if (includeData) allocSize += totalDataBytes;

            // Create buffer/view
            target.Expand(allocSize);
            target.Clear();
            Span<byte> span = target.Buffer.Span;
            ContainerHeader.WriteLength(span, dataStart + totalDataBytes);

            // Header
            ref var containerHeader = ref ContainerHeader.FromSpan(span);
            containerHeader.Version = Version;
            containerHeader.FieldCount = n;
            //h2.NameOffset = nameStart;  // absolute
            containerHeader.DataOffset = dataStart;  // absolute
            containerHeader.ContainerNameLength = (short)(includeContainerName ? _name.Length * sizeof(char) : 0);

            // Field headers (absolute DataOffset)
            int nameOffset = 0;      // relative within Names blob
            int running = dataStart; // absolute cursor in whole buffer
            for (int i = 0; i < n; i++)
            {
                var name = _map.Keys[i];
                var e = _map.Values[i];
                ref var field = ref FieldHeader.FromSpanAndFieldIndex(span, i);

                field = new FieldHeader
                {
                    NameHash = ReadOnlyMemoryComparer.GetHashCode(name),
                    NameOffset = nameStart + nameOffset,   // absolute
                    NameLength = (short)name.Length,
                    FieldType = e.Type,
                    Reserved = 0,
                    DataOffset = running,                  // absolute (== start of this field payload)
                    ElemSize = e.ElemSize,
                    Length = e.Data.Length,
                };

                nameOffset += name.Length * sizeof(char);
                running += e.Data.Length;              // advance absolute cursor
            }

            // try copy container name
            if (includeContainerName)
            {
                var nameBytes = MemoryMarshal.AsBytes(_name.Span);
                nameBytes.CopyTo(target.AsSpan(baseNameStart, nameBytes.Length));
            }

            // Write Names blob (UTF-16) into NameSegment (relative slicing OK)
            int nameCursor = 0; // relative within NameSegment
            var nameDst = target.AsSpan(containerHeader.NameOffset);
            for (int i = 0; i < n; i++)
            {
                var s = _map.Keys[i];
                int byteCount = s.Length * sizeof(char);
                MemoryMarshal.AsBytes(s.Span).CopyTo(nameDst.Slice(nameCursor, byteCount));
                nameCursor += byteCount;
            }
        }






        /// <summary>
        /// Materialize the container as a compact byte[] with fresh layout.
        /// Order: [ContainerHeader][FieldHeader...][Names][Data].
        /// </summary>
        internal Container BuildContainer()
        {
            //byte[] target = null;
            AllocatedMemory m = default;
            WriteTo(ref m);
            return Container.Registry.Shared.CreateWildWith(in m);
        }

        /// <summary>
        /// Build a container layout that can be use to create multiple new objects with same layout
        /// </summary>
        /// <returns></returns>
        public ContainerLayout BuildLayout()
        {
            AllocatedMemory header = default;
            BuildLayout(ref header, false, false);
            return new ContainerLayout(header.Array);
        }

        /// <summary>
        /// Create a new storage
        /// </summary>
        /// <returns></returns>
        public Storage BuildStorage() => new Storage(BuildContainer());




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte[] CreateDefaultValueBytes<T>(T value) where T : unmanaged
        {
            var buf = new byte[TypeUtil<T>.Size];
            MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref value, 1)).CopyTo(buf);
            return buf;
        }




        internal static ObjectBuilder FromContainer(Container container)
        {
            var builder = new ObjectBuilder();
            builder.SetName(container.NameSpan);
            builder.Version = container.Version;
            for (int i = 0; i < container.FieldCount; i++)
            {
                ref var header = ref container.GetFieldHeader(i);
                builder.SetRaw(container.GetFieldName(in header).ToString(), in header);
            }
            return builder;
        }
    }
}
