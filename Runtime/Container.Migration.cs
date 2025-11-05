using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Amlos.Container.TypeUtil;

namespace Amlos.Container
{
    internal sealed partial class Container
    {
        /// <summary>
        /// Migrate this container to a new schema by adding/removing fields.
        /// Rules:
        ///  - New fields (present only in new schema) are zero-initialized.
        ///  - Removed fields (present only in old schema) are discarded.
        ///  - Container identity (ID) is preserved.
        ///  - New buffer is always zero-initialized by default.
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
        public void Rescheme(Schema newSchema, bool zeroInitNewBuffer = true)
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
                var dstBytes = dst.Slice(newField.Offset, newField.AbsLength); // NEW buffer slice


                if (oldField.IsRef != newField.IsRef)
                {
                    // Removed: if ref, unregister all non-zero children
                    if (oldField.IsRef)
                    {
                        var oldIds = GetRefSpan(oldField);
                        for (int j = 0; j < oldIds.Length; j++)
                            Registry.Shared.Unregister(ref oldIds[j]);
                    }
                    dstBytes.Clear();
                    continue;
                }
                if (!oldField.IsRef)
                {
                    // we dont know what it could be, use unknown for now
                    // Value-to-value migration: convert from oldHint -> targetHint
                    var srcBytes = GetReadOnlySpan(oldField);                 // OLD buffer read-only
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






        public int GetFieldIndexOrRescheme<T>(string fieldName) where T : unmanaged
        {
            int index = _schema.IndexOf(fieldName);
            if (index < 0)
            {
                index = ReschemeForNew<T>(fieldName);
            }

            return index;
        }

        public int GetFieldIndexOrReschemeObject(string fieldName)
        {
            int index = _schema.IndexOf(fieldName);
            if (index < 0) index = ReschemeForNewObject(fieldName);
            return index;
        }

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public int ReschemeForNew<T>(string fieldName, int? inlineArrayLength = null) where T : unmanaged => ReschemeForField_Internal(fieldName, TypeUtil.Pack(TypeUtil.PrimOf<T>(), inlineArrayLength.HasValue),
            b =>
            {
                if (inlineArrayLength.HasValue) b.AddArrayOf<T>(fieldName, inlineArrayLength.Value);
                else b.AddFieldOf<T>(fieldName);
            });

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public int ReschemeForNewObject(string fieldName, int? inlineArrayLength = null) => ReschemeForField_Internal(fieldName, TypeUtil.Pack(ValueType.Ref, inlineArrayLength.HasValue),
            b =>
            {
                if (inlineArrayLength.HasValue) b.AddRefArray(fieldName, inlineArrayLength.Value);
                else b.AddRef(fieldName);
            });

        /// <summary>
        /// Rescheme to add a new field of type T with given fieldName.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public int ReschemeFor<T>(string fieldName, int? inlineArrayLength = null) where T : unmanaged => ReschemeForField_Internal(fieldName, TypeUtil.Pack(TypeUtil.PrimOf<T>(), inlineArrayLength.HasValue),
            b =>
            {
                b.RemoveField(fieldName);
                if (inlineArrayLength.HasValue) b.AddArrayOf<T>(fieldName, inlineArrayLength.Value);
                else b.AddFieldOf<T>(fieldName);
            });

        private int ReschemeForField_Internal(string fieldName, byte type, Action<SchemaBuilder> edit)
        {
            Rescheme(edit);
            var index = _schema.IndexOf(fieldName);
            HeaderSegment[index] = type;
            return index;
        }







        /// <summary>
        /// Ensure that 'fieldName' exists as a non-ref value field sized to sizeof(T).
        /// If it exists with same size but different type hint, convert the in-place bytes and update the hint,
        /// without rescheming. If it doesn't exist or size mismatches, rebuild schema to replace the field.
        /// </summary>
        public void EnsureFieldForRead<T>(int index, bool isExplicit = false) where T : unmanaged
        {
            FieldType fieldType = HeaderSegment[index];
            FieldType targetType = FieldType.Of<T>(fieldType.IsArray);
            ValueType valueType = fieldType.Type;
            ValueType target = targetType.Type;

            // is same type
            if (valueType == target)
                return;

            // if we don't know current type, assume new type is valid
            if (valueType == ValueType.Unknown)
            {
                HeaderSegment[index] = Pack(target, fieldType.IsArray);
                return;
            }
            Migrate<T>(index, target);
        }

        public bool Migrate<T>(int index, ValueType target) where T : unmanaged
        {
            byte fieldType = HeaderSegment[index];
            bool isArray = IsArray(fieldType);
            ValueType valueType = PrimOf(fieldType);


            FieldDescriptor field = _schema.Fields[index];
            // 2) Existing but ref ¡ú not allowed
            if (field.IsRef)
                throw new InvalidOperationException($"Field '{field}' is a reference; cannot read value T={typeof(T).Name}.");

            int oldElementSize = TypeUtil.SizeOf(valueType);
            int newElementSize = TypeUtil.SizeOf(PrimOf<T>());
            int arrayLength = isArray ? field.Length / oldElementSize : 1;

            // inplace conversion, given element same size
            if (oldElementSize == newElementSize)
            {
                Span<byte> data = GetSpan(field);
                if (isArray) MigrationConverter.ConvertArrayInPlaceSameSize(data, arrayLength, valueType, target);
                else MigrationConverter.ConvertScalarInPlace(data, valueType, target);
                HeaderSegment[index] = Pack(target, isArray);
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
                    MigrationConverter.MigrateValueFieldBytes(buffer, newSpan, valueType, target, true);
                    HeaderSegment[newIndex] = Pack(target, isArray);
                }
                catch
                {
                    return false;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(array);
                }
            }

            return true;
        }




        /// <summary>
        /// Try read scalar with implicit conversion if needed. will not change type hint if type known.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool TryReadScalarImplicit<T>(int index, out T value) where T : unmanaged
        {
            value = default;
            FieldType ft = new(HeaderSegment[index]);
            if (ft.Type == ValueType.Unknown) return false;

            var view = GetValueView(index);
            return view.TryRead(out value);
        }

        /// <summary>
        /// Try write scalar with implicit conversion if needed. will not change type hint if type known.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool TryWriteScalarImplicit<T>(int index, T value) where T : unmanaged
        {
            FieldType ft = new(HeaderSegment[index]);

            var dstSpan = GetSpan(index);
            var dstType = ft.Type;
            var srcType = TypeUtil.PrimOf<T>();
            // current type unknown, update hint
            if (dstType == ValueType.Unknown)
            {
                // too large to fit
                if (Unsafe.SizeOf<T>() > dstSpan.Length)
                    return false;

                HeaderSegment[index] = TypeUtil.Pack(srcType, ft.IsArray);
                MemoryMarshal.Write(dstSpan, ref Unsafe.AsRef(value));
                return true;
            }
            // type match, direct write
            if (dstType == srcType)
            {
                MemoryMarshal.Write(dstSpan, ref Unsafe.AsRef(value));
                return true;
            }

            // implicit conversion
            var srcSpan = MemoryMarshal.Cast<T, byte>(MemoryMarshal.CreateSpan(ref value, 1));
            var view = new ValueView(srcSpan, srcType);
            return view.TryWrite(dstSpan, dstType);
        }

        /// <summary>
        /// Try read scalar with explicit conversion if needed. will not change type hint if type known.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool TryReadScalarExplicit<T>(int index, out T value) where T : unmanaged
        {
            var view = GetValueView(index);
            return view.TryRead(out value, true);
        }

        private bool TryWriteScalarExplicit<T>(int index, T value) where T : unmanaged
        {
            FieldType ft = new(HeaderSegment[index]);

            var dstSpan = GetSpan(index);
            var dstType = ft.Type;
            var srcType = TypeUtil.PrimOf<T>();

            // conversion
            var srcSpan = MemoryMarshal.Cast<T, byte>(MemoryMarshal.CreateSpan(ref value, 1));
            var view = new ValueView(srcSpan, srcType);
            return view.TryWrite(dstSpan, dstType, true);
        }
    }
}
