using System;

namespace Amlos.Container
{
    internal sealed partial class Container
    {
        /// <summary>
        /// Migrate this container to a new schema by ONLY adding/removing fields.
        /// Rules:
        ///  - For fields present in BOTH old and new schemas (matched by Name):
        ///      * They must be the same kind (ref vs value) AND the same AbsLength.
        ///      * If not, throws InvalidOperationException.
        ///  - New fields (present only in new schema) are zero-initialized.
        ///  - Removed fields (present only in old schema) are discarded.
        ///  - Container identity (ID) is preserved.
        ///  - New buffer is always zero-initialized by default.
        /// </summary>
        public void RebuildSchema(Schema newSchema)
        {
            RebuildSchema(newSchema, zeroInitNewBuffer: true);
        }

        /// <summary>
        /// Internal overload allowing to skip zero-initialization when the caller
        /// will fully overwrite the new buffer manually.
        /// </summary>
        internal void RebuildSchema(Schema newSchema, bool zeroInitNewBuffer)
        {
            if (newSchema is null) throw new ArgumentNullException(nameof(newSchema));
            EnsureNotDisposed();

            if (ReferenceEquals(_schema, newSchema) || _schema.Equals(newSchema))
                return;

            // Prepare destination buffer (zero-initialized by default).
            byte[] dstBuf = newSchema.Stride == 0
                ? Array.Empty<byte>()
                : newSchema.Pool.Rent(zeroInit: zeroInitNewBuffer);
            var dst = dstBuf.AsSpan();

            // Build a fast name set for the new schema to detect removals quickly.
            // Copy overlapping fields (same name/kind/length).
            foreach (var oldField in _schema.Fields)
            {
                // deal with removed field
                if (!newSchema.TryGetField(oldField.Name, out var newField))
                {
                    // IMPORTANT: before swapping buffers, unregister removed ref subtrees
                    // (fields present in old schema but missing in new schema). 
                    if (!oldField.IsRef) continue;

                    // Unregister every non-zero child id in this ref field (single or array).
                    var ids = GetRefSpan(oldField);
                    for (int j = 0; j < ids.Length; j++)
                    {
                        Registry.Shared.Unregister(ref ids[j]);
                    }
                }
                // migrate overlapping field
                else
                {
                    if (oldField.IsRef != newField.IsRef)
                        throw new InvalidOperationException(
                            $"Field '{oldField.Name}' changed kind (ref <-> value); only add/remove allowed.");

                    if (oldField.AbsLength != newField.AbsLength)
                        throw new InvalidOperationException(
                            $"Field '{oldField.Name}' changed length ({oldField.AbsLength} -> {newField.AbsLength}); only add/remove allowed.");

                    var src = GetReadOnlySpan(oldField);
                    var dstSpan = dst.Slice(newField.Offset, newField.AbsLength);
                    src.CopyTo(dstSpan);
                }

            }


            // Swap schema+buffer, return old buffer
            var oldSchema = _schema;
            var oldBuf = _buffer;

            _schema = newSchema;
            _buffer = dstBuf;

            if (oldSchema.Stride > 0 && oldBuf.Length > 0 && !ReferenceEquals(oldBuf, Array.Empty<byte>()))
                oldSchema.Pool.Return(oldBuf);
        }

        /// <summary>
        /// Convenience: derive a builder from the current schema, let the caller add/remove fields,
        /// then rebuild with the produced schema. Only add/remove is allowed; changing an existing
        /// field's kind/length will throw during rebuild.
        /// </summary>
        public void Rescheme(Action<SchemaBuilder> edit)
        {
            if (edit is null) throw new ArgumentNullException(nameof(edit));
            EnsureNotDisposed();
            RebuildSchema(Schema.Variate(edit));  // zero-init by default
        }
    }
}
