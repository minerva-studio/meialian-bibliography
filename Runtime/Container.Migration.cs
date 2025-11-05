using System;
using System.Buffers;
using static Amlos.Container.TypeUtil;

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
        public void Rescheme(Schema newSchema)
        {
            Rescheme(newSchema, zeroInitNewBuffer: true);
        }

        /// <summary>
        /// Convenience: derive a builder from the current schema, let the caller add/remove fields,
        /// then rebuild with the produced schema. Only add/remove is allowed; changing an existing
        /// field's kind/length will throw during rebuild.
        /// </summary>
        public void Rescheme(Action<SchemaBuilder> edit)
        {
            EnsureNotDisposed();
            Rescheme(Schema.Variate(edit));  // zero-init by default
        }

        /// <summary>
        /// Internal overload allowing to skip zero-initialization when the caller
        /// will fully overwrite the new buffer manually.
        /// </summary>
        internal void Rescheme(Schema newSchema, bool zeroInitNewBuffer)
        {
            if (newSchema is null) throw new ArgumentNullException(nameof(newSchema));
            EnsureNotDisposed();

            if (ReferenceEquals(_schema, newSchema) || _schema.Equals(newSchema))
                return;

            // Prepare destination buffer
            byte[] dstBuf = newSchema.Stride == 0
                ? Array.Empty<byte>()
                : newSchema.Pool.Rent(zeroInit: zeroInitNewBuffer);
            var dst = dstBuf.AsSpan();

            // Prepare new header hints (migrate by name)
            var newHints = dstBuf.AsSpan(0, newSchema.HeaderSize);

            // ---- Migrate each old field into new layout ----
            for (int oldIdx = 0; oldIdx < _schema.Fields.Count; oldIdx++)
            {
                var oldField = _schema.Fields[oldIdx];
                byte oldHint = (oldIdx < HeaderSegment.Length) ? HeaderSegment[oldIdx] : (byte)0;

                if (!newSchema.TryGetField(oldField.Name, out var newField))
                {
                    // Removed: if ref, unregister all non-zero children
                    if (oldField.IsRef)
                    {
                        var oldIds = GetRefSpan(oldField);
                        for (int j = 0; j < oldIds.Length; j++)
                            Registry.Shared.Unregister(ref oldIds[j]);
                    }
                    continue;
                }

                int newIdx = newSchema.IndexOf(newField.Name);


                if (oldField.IsRef != newField.IsRef)
                {
                    throw new InvalidOperationException(
                        $"Field '{oldField.Name}' changed kind (ref <-> value) which is not supported.");
                }

                if (!oldField.IsRef)
                {
                    // we dont know what it could be, use unknown for now
                    // Value-to-value migration: convert from oldHint -> targetHint
                    var srcBytes = GetReadOnlySpan(oldField);                 // OLD buffer read-only
                    var dstBytes = dst.Slice(newField.Offset, newField.AbsLength); // NEW buffer slice
                    if (srcBytes.Length == dstBytes.Length)
                    {
                        newHints[newIdx] = oldHint;
                        srcBytes.CopyTo(dstBytes);
                    }
                    else
                    {
                        newHints[newIdx] = Pack(ValueType.Unknown, IsArray(oldHint));
                    }
                }
                else
                {
                    newHints[newIdx] = oldHint;
                    // Ref-to-Ref migration unchanged (copy ids, unregister tails)
                    var oldIds = GetRefSpan(oldField);
                    var dstBytes = dst.Slice(newField.Offset, newField.AbsLength);
                    var newIds = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ulong>(dstBytes);

                    int keep = Math.Min(oldIds.Length, newIds.Length);
                    for (int i = 0; i < keep; i++) newIds[i] = oldIds[i];
                    for (int i = keep; i < newIds.Length; i++) newIds[i] = 0UL;
                    for (int i = keep; i < oldIds.Length; i++) Registry.Shared.Unregister(ref oldIds[i]);
                }
            }

            // ---- Swap schema & buffers ----
            var oldSchema = _schema;
            var oldBuf = _buffer;

            _schema = newSchema;
            _buffer = dstBuf;

            if (oldSchema.Stride > 0 && oldBuf.Length > 0 && !ReferenceEquals(oldBuf, Array.Empty<byte>()))
                oldSchema.Pool.Return(oldBuf);
        }






        public FieldDescriptor GetFieldDescriptorOrRescheme<T>(string fieldName) where T : unmanaged
        {
            if (!_schema.TryGetField(fieldName, out var f))
            {
                Rescheme(b => b.AddFieldOf<T>(fieldName));
                f = _schema.GetField(fieldName);
            }

            return f;
        }

        /// <summary>
        /// Ensure that 'fieldName' exists as a non-ref value field sized to sizeof(T).
        /// If it exists with same size but different type hint, convert the in-place bytes and update the hint,
        /// without rescheming. If it doesn't exist or size mismatches, rebuild schema to replace the field.
        /// </summary>
        public void EnsureFieldForRead<T>(string fieldName) where T : unmanaged
        {
            //1) Field missing ¡ú add as value field of T
            if (!_schema.TryGetField(fieldName, out var f))
            {
                return;
            }
            Migrate<T>(f);
        }

        internal void Migrate<T>(FieldDescriptor field) where T : unmanaged
        {
            EnsureNotDisposed();

            // 2) Existing but ref ¡ú not allowed
            if (field.IsRef)
                throw new InvalidOperationException($"Field '{field}' is a reference; cannot read value T={typeof(T).Name}.");

            int fi = _schema.IndexOf(field.Name);
            byte fieldType = HeaderSegment[fi];
            bool isArray = IsArray(fieldType);
            ValueType valueType = PrimOf(fieldType);
            ValueType target = PrimOf<T>();

            // is same type
            if (valueType == target)
                return;

            // if we don't know current type, assume new type is valid
            if (valueType == ValueType.Unknown)
            {
                HeaderSegment[fi] = Pack(target, isArray);
                return;
            }

            int oldElementSize = TypeUtil.SizeOf(valueType);
            int newElementSize = TypeUtil.SizeOf(PrimOf<T>());
            int arrayLength = isArray ? field.Length / oldElementSize : 1;

            // inplace conversion, given element same size
            if (oldElementSize == newElementSize)
            {
                Span<byte> data = GetSpan(field);
                if (isArray) MigrationConverter.ConvertArrayInPlaceSameSize(data, arrayLength, valueType, target);
                else MigrationConverter.ConvertScalarInPlace(data, valueType, target);
                HeaderSegment[fi] = Pack(target, isArray);
            }
            // different size
            else
            {
                // copy old data
                byte[] array = ArrayPool<byte>.Shared.Rent(field.Length);
                try
                {
                    Span<byte> buffer = array.AsSpan(0, field.Length);
                    GetSpan(field).CopyTo(buffer);
                    // rescheme
                    Rescheme(b =>
                    {
                        b.RemoveField(field.Name);
                        if (isArray) b.AddArrayOf<T>(field.Name, arrayLength);
                        else b.AddFieldOf<T>(field.Name);
                    });
                    var newIndex = Schema.IndexOf(field.Name);
                    var newSpan = GetSpan(Schema.Fields[newIndex]);
                    // fill back
                    MigrationConverter.MigrateValueFieldBytes(buffer, newSpan, valueType, target);
                    HeaderSegment[newIndex] = Pack(target, isArray);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(array);
                }
            }
        }



        private bool TryReadScalarWithoutRescheme<T>(FieldDescriptor f, out T value) where T : unmanaged
        {
            value = default;
            int idx = _schema.IndexOf(f.Name);
            if (idx < 0) return false;

            byte hint = (idx < HeaderSegment.Length) ? HeaderSegment[idx] : (byte)0;
            bool isArray = IsArray(hint);
            var storedPrim = TypeUtil.PrimOf(hint);

            if (storedPrim == ValueType.Unknown || isArray) return false;

            var requestedPrim = TypeUtil.PrimOf<T>();

            // If stored -> requested is allowed by implicit conversion table, do the conversion and return
            if (!MigrationConverter.CanImplicitlyConvert(storedPrim, requestedPrim))
                return false;

            int storedElem = SizeOf(storedPrim);
            if (storedElem <= 0) return false;

            var srcSlice = GetReadOnlySpan(f)[..storedElem];
            MigrationConverter.ReadElementAs(srcSlice, storedPrim, out double asDouble, out bool asBool, out char asChar);

            // convert canonical -> T
            if (!TryConvertCanonicalTo<T>(asDouble, asBool, asChar, out value))
                return false;

            return true;
        }

        private bool TryWriteScalarWithoutRescheme<T>(FieldDescriptor f, T value) where T : unmanaged
        {
            int idx = _schema.IndexOf(f.Name);
            if (idx < 0) return false;

            byte hint = (HeaderSegment != null && idx < HeaderSegment.Length) ? HeaderSegment[idx] : (byte)0;
            bool isArray = TypeUtil.IsArray(hint);
            var storedPrim = TypeUtil.PrimOf(hint);

            if (storedPrim == ValueType.Unknown || isArray) return false;

            var valuePrim = TypeUtil.PrimOf<T>();

            // If value type can implicitly convert to storedPrim (write-side)
            if (!MigrationConverter.CanImplicitlyConvert(valuePrim, storedPrim))
                return false;

            // prepare canonical triple and write into stored slot
            if (!TryGetCanonicalFromValue(value, out double asDouble, out bool asBool, out char asChar))
                return false;

            var span = GetSpan(f)[..SizeOf(storedPrim)];
            MigrationConverter.WriteElementAs(span, storedPrim, asDouble, asBool, asChar);

            // update hint when previously Unknown
            if (TypeUtil.PrimOf(hint) == ValueType.Unknown)
                HeaderSegment[idx] = TypeUtil.Pack(storedPrim, isArray: false);

            return true;
        }


    }
}
