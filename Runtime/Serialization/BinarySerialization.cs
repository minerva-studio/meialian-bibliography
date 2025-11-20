using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage.Serialization
{
    /// <summary>
    /// Binary serialization helpers for <see cref="Storage"/> and <see cref="StorageObject"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The binary format is a depth-first, pre-order dump of the <see cref="Storage"/> tree
    /// starting at <see cref="Storage.Root"/>. It is designed for fast round-trips inside the
    /// same build rather than long-term, schema-compatible persistence.
    /// </para>
    ///
    /// <para>
    /// For each container in the tree, the following bytes are written:
    /// <code>
    /// [ id : sizeof(ContainerReference) bytes, little-endian ]
    /// [ payload : Length bytes of the container's raw memory ]
    /// </code>
    /// The <c>payload</c> is exactly the backing memory of the container (including header and
    /// field data). Reference fields (<c>IsRef == true</c>) contain an array of
    /// <see cref="ContainerReference"/> values. During serialization, the code follows those
    /// IDs and recursively emits the referenced containers. During deserialization, the IDs
    /// inside reference fields are rewritten to the IDs of the newly created containers.
    /// </para>
    ///
    /// <para>
    /// This implementation assumes that the storage graph is a tree: each child container is
    /// reached from exactly one parent through a reference field, and there are no cycles or
    /// shared subtrees. If the same <see cref="Container"/> is referenced from multiple places,
    /// it will be serialized multiple times and deserialization semantics are undefined.
    /// </para>
    ///
    /// <para>
    /// All containers created by <see cref="Parse(ReadOnlySpan{byte})"/> and
    /// <see cref="Parse(Memory{byte}, bool)"/> are registered in
    /// <see cref="Container.Registry"/>, and a fresh ID is assigned to each of them.
    /// </para>
    /// </remarks>
    public static class BinarySerialization
    {
        /// <summary>
        /// Serializes the entire <see cref="Storage"/> tree into a Base64-encoded string.
        /// </summary>
        /// <remarks>
        /// This is a thin wrapper over <see cref="BinarySerialization.WriteBinaryTo(StorageObject, Buffers.IBufferWriter{byte})"/>.
        /// It first writes the binary payload into an <see cref="ArrayBufferWriter{T}"/> and then
        /// converts the written bytes to a Base64 string.
        /// </remarks>
        /// <param name="storage">The storage instance to serialize.</param>
        /// <returns>
        /// A Base64-encoded string representing the full container tree rooted at
        /// <see cref="Storage.Root"/>.
        /// </returns>
        public static string ToBase64(this Storage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            var writer = new ArrayBufferWriter<byte>();
            storage.Root.WriteBinaryTo(writer);
            return Convert.ToBase64String(writer.WrittenMemory.Span);
        }

        /// <summary>
        /// Serializes the entire <see cref="Storage"/> tree rooted at <see cref="Storage.Root"/>
        /// into a contiguous binary blob.
        /// </summary>
        /// <remarks>
        /// The returned span points into the internal buffer of an <see cref="ArrayBufferWriter{T}"/>.
        /// It is only valid until the writer gets resized or discarded; callers are expected to copy
        /// the data (e.g., via <see cref="ReadOnlySpan{T}.ToArray"/>) if they need to keep it.
        /// </remarks>
        /// <param name="storage">The storage whose root container will be serialized.</param>
        /// <returns>
        /// A <see cref="ReadOnlySpan{T}"/> exposing the written bytes for the full tree.
        /// </returns>
        public static ReadOnlySpan<byte> ToBinary(this Storage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            var writer = new ArrayBufferWriter<byte>();
            storage.Root.WriteBinaryTo(writer);
            return writer.WrittenSpan;
        }

        /// <summary>
        /// Writes the current <see cref="StorageObject"/> and all of its child containers
        /// into the specified <see cref="IBufferWriter{T}"/> in depth-first order.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method first writes the container backing the current <see cref="StorageObject"/>
        /// and then walks all fields of that container. For each reference field
        /// (<c>IsRef == true</c>), it reads the array of <see cref="ContainerReference"/> values
        /// and recursively serializes each non-empty referenced container.
        /// </para>
        ///
        /// <para>
        /// The traversal is purely structural and does not perform any cycle detection or sharing
        /// checks; the caller is responsible for ensuring that the logical structure is a tree.
        /// </para>
        /// </remarks>
        /// <param name="storage">The logical object whose container tree will be written.</param>
        /// <param name="writer">Destination buffer writer that receives the serialized bytes.</param>
        private static void WriteBinaryTo(this StorageObject storage, IBufferWriter<byte> writer)
        {
            storage.EnsureNotDisposed(); // ensure not disposed
            WriteBinary_Internal(storage, writer);
            for (int i = 0; i < storage.FieldCount; i++)
            {
                ref var field = ref storage._container.GetFieldHeader(i);
                if (!field.IsRef)
                    continue;

                var ids = storage._container.GetRefSpan(in field);
                for (int i1 = 0; i1 < ids.Length; i1++)
                {
                    var cid = ids[i1];
                    if (cid == Container.Registry.ID.Empty)
                        continue;

                    if (cid.TryGet(out var obj))
                        obj.WriteBinaryTo(writer);
                }
            }
        }

        /// <summary>
        /// Writes a single container (without walking its children) to the buffer writer.
        /// </summary>
        /// <remarks>
        /// The format is:
        /// <code>
        /// [ id : sizeof(ContainerReference) bytes, little-endian ]
        /// [ payload : container.Memory.Length bytes ]
        /// </code>
        /// Containers with an empty ID are skipped and do not emit any bytes.
        /// </remarks>
        /// <param name="storage">The object whose backing container will be written.</param>
        /// <param name="writer">Destination buffer writer.</param>
        private static void WriteBinary_Internal(StorageObject storage, IBufferWriter<byte> writer)
        {
            if (storage.ID == Container.Registry.ID.Empty)
                return;

            Span<byte> idSpan = writer.GetSpan(8);
            BinaryPrimitives.WriteUInt64LittleEndian(idSpan, storage.ID);
            writer.Advance(8);

            var memory = storage._container.Memory;
            var dst = writer.GetSpan(memory.Length);
            memory.Span.CopyTo(dst);
            writer.Advance(memory.Length);
        }




        /// <summary>
        /// Parses a binary blob produced by <see cref="ToBinary(Storage)"/> (or
        /// <see cref="WriteBinaryTo(StorageObject, IBufferWriter{byte})"/>) and returns
        /// a new <see cref="Storage"/> whose root is the first container in the stream.
        /// </summary>
        /// <remarks>
        /// This overload always allocates a fresh buffer for each <see cref="Container"/>,
        /// because a <see cref="ReadOnlySpan{T}"/> does not provide ownership semantics.
        /// The containers are registered in <see cref="Container.Registry"/> and assigned
        /// new IDs.
        /// </remarks>
        /// <param name="bytes">Binary data starting at the root container.</param>
        /// <returns>
        /// A new <see cref="Storage"/> instance owning the reconstructed container tree.
        /// </returns>
        public static Storage Parse(ReadOnlySpan<byte> bytes)
        {
            var root = ReadContainer(bytes);
            return new Storage(root.Item1);
        }

        /// <summary>
        /// Parses a binary blob produced by <see cref="ToBinary(Storage)"/> (or
        /// <see cref="WriteBinaryTo(StorageObject, IBufferWriter{byte})"/>) and returns
        /// a new <see cref="Storage"/> whose root is the first container in the stream.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When <paramref name="allocate"/> is <c>true</c>, the payload for each container is
        /// copied into a fresh <see cref="AllocatedMemory"/> owned by that container.
        /// </para>
        /// <para>
        /// When <paramref name="allocate"/> is <c>false</c>, each container's
        /// <see cref="AllocatedMemory"/> aliases the provided <see cref="Memory{T}"/> slice.
        /// The caller must ensure that the original buffer outlives the <see cref="Storage"/>
        /// and any derived <see cref="StorageObject"/> instances.
        /// </para>
        /// </remarks>
        /// <param name="bytes">Binary data buffer that contains the root followed by its children.</param>
        /// <param name="allocate">
        /// If <c>true</c>, clone container payloads into new memory blocks; if <c>false</c>,
        /// keep them as views into the provided <paramref name="bytes"/>.
        /// </param>
        /// <returns>
        /// A new <see cref="Storage"/> instance owning the reconstructed container tree.
        /// </returns>
        public static Storage Parse(Memory<byte> bytes, bool allocate = true)
        {
            var root = ReadContainer(bytes, allocate);
            return new Storage(root.Item1);
        }

        /// <summary>
        /// Parses a Base64-encoded string produced by <see cref="ToBase64(Storage)"/>
        /// and reconstructs a new <see cref="Storage"/> instance.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Internally this method decodes the Base64 string into a byte array and then forwards
        /// to the existing <see cref="Parse(ReadOnlySpan{byte})"/> or
        /// <see cref="Parse(Memory{byte}, bool)"/> overloads.
        /// </para>
        /// <para>
        /// When <paramref name="allocate"/> is <c>true</c>, each container's payload is cloned into
        /// its own <see cref="AllocatedMemory"/> block, so the resulting storage does not depend
        /// on the decoded byte array.
        /// When <paramref name="allocate"/> is <c>false</c>, containers will alias slices of the
        /// decoded buffer; the buffer must outlive the resulting <see cref="Storage"/>.
        /// </para>
        /// </remarks>
        /// <param name="base64">Base64 string that encodes a serialized storage tree.</param>
        /// <param name="allocate">
        /// If <c>true</c>, clone container payloads into dedicated memory. If <c>false</c>,
        /// keep them as views over the decoded buffer.
        /// </param>
        /// <returns>A new <see cref="Storage"/> instance reconstructed from the Base64 payload.</returns>
        public static Storage ParseBase64(string base64, bool allocate = true)
        {
            if (base64 == null)
                throw new ArgumentNullException(nameof(base64));

            var bytes = Convert.FromBase64String(base64);

            if (allocate)
            {
                // This overload always allocates new backing memory for each container.
                return Parse((ReadOnlySpan<byte>)bytes);
            }

            // This overload can alias the decoded buffer to avoid extra copies.
            return Parse(new Memory<byte>(bytes), allocate: false);
        }

        /// <summary>
        /// Reads a single container and all of its descendants from a mutable buffer.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The input is assumed to start with a container record encoded as:
        /// <code>
        /// [ id : sizeof(ContainerReference) bytes ]
        /// [ payload : Length bytes ]
        /// [ children... ]
        /// </code>
        /// where <c>Length</c> is read from the container header at
        /// <c>ContainerHeader.LengthOffset</c>. After the root payload, the method walks all
        /// reference fields in the container and recursively calls itself to load each child.
        /// </para>
        ///
        /// <para>
        /// The second item in the returned tuple is the total number of bytes consumed from
        /// <paramref name="src"/> to reconstruct the full subtree rooted at this container.
        /// This allows callers to chain multiple calls over a larger buffer.
        /// </para>
        ///
        /// <para>
        /// If <paramref name="allocate"/> is <c>true</c>, the container payload is copied into
        /// a new <see cref="AllocatedMemory"/>. Otherwise, the container will directly wrap the
        /// corresponding slice of <paramref name="src"/>.
        /// </para>
        /// </remarks>
        /// <param name="src">Source buffer starting at the container ID.</param>
        /// <param name="allocate">
        /// Whether to allocate a new backing buffer for the container payload.
        /// </param>
        /// <returns>
        /// A tuple containing the reconstructed <see cref="Container"/> and the number of bytes
        /// consumed from <paramref name="src"/>.
        /// </returns>
        private static (Container, int) ReadContainer(Memory<byte> src, bool allocate = false)
        {
            var span = src.Span;

            var idSize = Unsafe.SizeOf<ContainerReference>();
            var offset = idSize + ContainerHeader.LengthOffset;
            var length = BitConverter.ToInt32(span[offset..(offset + ContainerHeader.LengthSize)]);

            Memory<byte> buffer = src.Slice(idSize, length);
            AllocatedMemory allocated = allocate ? AllocatedMemory.Create(buffer.Span) : new AllocatedMemory(buffer);

            var view = new ContainerView(allocated.Span);
            int totalConsumption = idSize + length;
            for (int i = 0; i < view.Header.FieldCount; i++)
            {
                ref var field = ref view.GetFieldHeader(i);
                if (!field.IsRef)
                    continue;

                var ids = MemoryMarshal.Cast<byte, ContainerReference>(allocated.AsSpan(field.DataOffset, field.Length));
                for (int i1 = 0; i1 < ids.Length; i1++)
                {
                    var cid = ids[i1];
                    if (cid == Container.Registry.ID.Empty)
                        continue;

                    // Deserialize child subtree starting right after the current container's bytes
                    var (childContainer, consumption) = ReadContainer(src[totalConsumption..]);
                    // Rewrite reference to point at the newly created container
                    ids[i1] = childContainer.ID;
                    // Advance past the bytes consumed by the child subtree
                    totalConsumption += consumption;
                }
            }

            Container container = Container.Registry.Shared.CreateWildWith(allocated);
            // Assign a fresh ID and register the container
            Container.Registry.Shared.AssignNewID(container);
            return (container, totalConsumption);
        }

        /// <summary>
        /// Reads a single container and all of its descendants from a read-only span.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This overload always allocates a new buffer for the container payload, because the
        /// source data is exposed as <see cref="ReadOnlySpan{T}"/>. The layout and traversal
        /// are identical to <see cref="ReadContainer(Memory{byte}, bool)"/>.
        /// </para>
        /// </remarks>
        /// <param name="src">Source span starting at the container ID.</param>
        /// <returns>
        /// A tuple containing the reconstructed <see cref="Container"/> and the number of bytes
        /// consumed from <paramref name="src"/>.
        /// </returns>
        private static (Container, int) ReadContainer(ReadOnlySpan<byte> src)
        {
            var idSize = Unsafe.SizeOf<ContainerReference>();
            var offset = idSize + ContainerHeader.LengthOffset;
            var length = BitConverter.ToInt32(src[offset..(offset + ContainerHeader.LengthSize)]);

            AllocatedMemory allocated = AllocatedMemory.Create(src.Slice(idSize, length));
            Span<byte> span = allocated.Span;

            ref var containerHeader = ref ContainerHeader.FromSpan(span);
            int totalConsumption = idSize + length;
            for (int i = 0; i < containerHeader.FieldCount; i++)
            {
                ref var field = ref FieldHeader.FromSpanAndFieldIndex(span, i);
                if (!field.IsRef)
                    continue;

                var ids = MemoryMarshal.Cast<byte, ContainerReference>(allocated.AsSpan(field.DataOffset, field.Length));
                for (int i1 = 0; i1 < ids.Length; i1++)
                {
                    var cid = ids[i1];
                    if (cid == Container.Registry.ID.Empty)
                        continue;

                    // Deserialize child subtree starting right after the current container's bytes
                    var (childContainer, consumption) = ReadContainer(src[totalConsumption..]);
                    // Rewrite reference to point at the newly created container
                    ids[i1] = childContainer.ID;
                    // Advance past the bytes consumed by the child subtree
                    totalConsumption += consumption;
                }
            }

            Container container = Container.Registry.Shared.CreateWildWith(allocated);
            // Assign a fresh ID and register the container
            Container.Registry.Shared.AssignNewID(container);
            return (container, totalConsumption);
        }
    }
}
