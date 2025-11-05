using System;
using System.Buffers;
using System.Buffers.Binary;
using static Amlos.Container.TypeHintUtil;

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
                byte oldHint = (HeaderHints != null && oldIdx < HeaderHints.Length) ? HeaderHints[oldIdx] : (byte)0;

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
            //1) Field missing → add as value field of T
            if (!_schema.TryGetField(fieldName, out var f))
            {
                return;
            }
            Migrate<T>(f);
        }

        internal void Migrate<T>(FieldDescriptor field) where T : unmanaged
        {
            EnsureNotDisposed();

            // 2) Existing but ref → not allowed
            if (field.IsRef)
                throw new InvalidOperationException($"Field '{field}' is a reference; cannot read value T={typeof(T).Name}.");

            int fi = _schema.IndexOf(field.Name);
            byte oldHint = HeaderHints[fi];
            bool isArray = IsArray(oldHint);
            ValueType valueType = Prim(oldHint);
            ValueType target = PrimOf<T>();

            // is same type
            if (valueType == target)
                return;

            // if we don't know current type, assume new type is valid
            if (valueType == ValueType.Unknown)
            {
                HeaderHints[fi] = Pack(target, isArray);
                return;
            }

            int oldElementSize = TypeHintUtil.ElemSize(valueType);
            int newElementSize = TypeHintUtil.ElemSize(PrimOf<T>());
            int arrayLength = isArray ? field.Length / oldElementSize : 1;

            // inplace conversion, given element same size
            if (oldElementSize == newElementSize)
            {
                Span<byte> data = GetSpan(field);
                if (isArray) MigrationConverter.ConvertArrayInPlaceSameSize(data, arrayLength, valueType, target);
                else MigrationConverter.ConvertScalarInPlace(data, valueType, target);
                HeaderHints[fi] = Pack(target, isArray);
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
                    HeaderHints[newIndex] = Pack(target, isArray);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(array);
                }
            }
        }



    }

    public static class MigrationConverter
    {
        // Place into MigrationConverter (keep your ReadElementAs/WriteElementAs/ElemSize helpers)
        public static bool MigrateValueFieldBytes(ReadOnlySpan<byte> src, Span<byte> dst, byte oldHint, byte newHint)
        {
            var oldVt = TypeHintUtil.Prim(oldHint);
            var newVt = TypeHintUtil.Prim(newHint);

            return MigrateValueFieldBytes(src, dst, oldVt, newVt);
        }

        public static bool MigrateValueFieldBytes(ReadOnlySpan<byte> src, Span<byte> dst, ValueType oldVt, ValueType newVt)
        {
            // Unknown fallback: raw copy + zero-fill/truncate
            if (oldVt == ValueType.Unknown || newVt == ValueType.Unknown)
            {
                int copy = Math.Min(src.Length, dst.Length);
                if (copy > 0) src[..copy].CopyTo(dst);
                if (dst.Length > copy) dst[copy..].Clear();
                return false;
            }

            int oldElem = ElemSize(oldVt);
            int newElem = ElemSize(newVt);
            if (oldElem <= 0 || newElem <= 0)
            {
                dst.Clear();
                return false;
            }

            // SAFETY GUARD: only operate in element-aligned arrays. If sizes are not divisible,
            // fallback to raw-copy to avoid misinterpreting incomplete trailing bytes.
            if (src.Length % oldElem != 0 || dst.Length % newElem != 0)
            {
                // conservative fallback
                int copy = Math.Min(src.Length, dst.Length);
                if (copy > 0) src[..copy].CopyTo(dst);
                if (dst.Length > copy) dst[copy..].Clear();
                return false;
            }

            int oldCnt = src.Length / oldElem;
            int newCnt = dst.Length / newElem;
            int cnt = Math.Min(oldCnt, newCnt);

            // Fast path: identical element type and element size -> memcpy common part
            if (oldVt == newVt && oldElem == newElem)
            {
                int bytesToCopy = cnt * oldElem;
                if (bytesToCopy > 0) src[..bytesToCopy].CopyTo(dst);
                if (dst.Length > bytesToCopy) dst[bytesToCopy..].Clear();
                return false;
            }

            // Element-wise conversion using explicit, offsetted slices (no global buffer assumptions)
            int sOff = 0, dOff = 0;
            for (int i = 0; i < cnt; i++, sOff += oldElem, dOff += newElem)
            {
                var sSlice = src.Slice(sOff, oldElem);
                var dSlice = dst.Slice(dOff, newElem);

                // read element into canonical numeric view (double), bool and char views
                ReadElementAs(sSlice, oldVt, out double asDouble, out bool asBool, out char asChar);

                // write into destination element according to target type
                WriteElementAs(dSlice, newVt, asDouble, asBool, asChar);
            }

            // zero-fill remainder of destination if any
            int used = cnt * newElem;
            if (dst.Length > used) dst[used..].Clear();
            return true;
        }


        /// <summary>
        /// Convert a single scalar value in-place from oldHint to newHint.
        /// The span length is the field's total length; only the first element is read/written.
        /// </summary> 
        public static bool ConvertScalarInPlace(Span<byte> bytes, ValueType oldVt, ValueType newVt)
        {
            // Unknown on either side → keep bytes unchanged (but caller may still update hint).
            if (oldVt == ValueType.Unknown || newVt == ValueType.Unknown)
                return false;

            // Read exactly ONE element from oldVt
            ReadElementAs(bytes, oldVt, out double asDouble, out bool asBool, out char asChar);

            // Write ONE element as newVt (overwriting the same span)
            WriteElementAs(bytes, newVt, asDouble, asBool, asChar);

            // Zero out any tail beyond the new element size (if span larger than element)
            int newElem = ElemSize(newVt);
            if (bytes.Length > newElem)
                bytes.Slice(newElem).Clear();
            return true;
        }

        /// <summary>
        /// In-place convert an array when old and new element sizes are the same.
        /// Reads each element as 'oldHint' and writes as 'newHint'.
        /// </summary> 
        public static void ConvertArrayInPlaceSameSize(Span<byte> bytes, int count, ValueType oldVt, ValueType newVt)
        {
            int elem = ElemSize(oldVt); // == ElemSize(newVt) by contract

            for (int i = 0, off = 0; i < count; i++, off += elem)
            {
                var cell = bytes.Slice(off, elem);

                // 读一个旧元素
                ReadElementAs(cell, oldVt, out double asDouble, out bool asBool, out char asChar);

                // 写一个新元素
                WriteElementAs(cell, newVt, asDouble, asBool, asChar);
            }
        }





        // Read one element (given its ValueType) and return canonical views:
        // numeric as double, boolean asBool, char asChar.
        public static void ReadElementAs(ReadOnlySpan<byte> src, ValueType vt, out double asDouble, out bool asBool, out char asChar)
        {
            asDouble = 0.0;
            asBool = false;
            asChar = '\0';

            switch (vt)
            {
                case ValueType.Bool:
                    asBool = src.Length > 0 && src[0] != 0;
                    asDouble = asBool ? 1.0 : 0.0;
                    return;

                case ValueType.Char16:
                    {
                        ushort u = BinaryPrimitives.ReadUInt16LittleEndian(src);
                        asChar = (char)u;
                        asBool = u != 0;
                        asDouble = u;
                        return;
                    }

                case ValueType.Int8:
                    {
                        sbyte v = unchecked((sbyte)src[0]);
                        asDouble = v; asBool = v != 0; asChar = (char)(ushort)(byte)v;
                        return;
                    }
                case ValueType.UInt8:
                    {
                        byte v = src[0];
                        asDouble = v; asBool = v != 0; asChar = (char)(ushort)v;
                        return;
                    }
                case ValueType.Int16:
                    {
                        short v = BinaryPrimitives.ReadInt16LittleEndian(src);
                        asDouble = v; asBool = v != 0; asChar = (char)(ushort)v;
                        return;
                    }
                case ValueType.UInt16:
                    {
                        ushort v = BinaryPrimitives.ReadUInt16LittleEndian(src);
                        asDouble = v; asBool = v != 0; asChar = (char)v;
                        return;
                    }
                case ValueType.Int32:
                    {
                        int v = BinaryPrimitives.ReadInt32LittleEndian(src);
                        asDouble = v; asBool = v != 0; asChar = (char)(ushort)v;
                        return;
                    }
                case ValueType.UInt32:
                    {
                        uint v = BinaryPrimitives.ReadUInt32LittleEndian(src);
                        asDouble = v; asBool = v != 0; asChar = (char)(ushort)(v & 0xFFFF);
                        return;
                    }
                case ValueType.Int64:
                    {
                        long v = BinaryPrimitives.ReadInt64LittleEndian(src);
                        asDouble = v; asBool = v != 0; asChar = (char)(ushort)(v & 0xFFFF);
                        return;
                    }
                case ValueType.UInt64:
                    {
                        ulong v = BinaryPrimitives.ReadUInt64LittleEndian(src);
                        asDouble = (double)v; asBool = v != 0; asChar = (char)(ushort)(v & 0xFFFF);
                        return;
                    }
                case ValueType.Float32:
                    {
                        int bits = BinaryPrimitives.ReadInt32LittleEndian(src);
                        float f = BitConverter.Int32BitsToSingle(bits);
                        asDouble = f; asBool = f != 0f; asChar = (char)(ushort)(int)f;
                        return;
                    }
                case ValueType.Float64:
                    {
                        long bits = BinaryPrimitives.ReadInt64LittleEndian(src);
                        double d = BitConverter.Int64BitsToDouble(bits);
                        asDouble = d; asBool = d != 0.0; asChar = (char)(ushort)(int)d;
                        return;
                    }
                default:
                    asDouble = 0.0; asBool = false; asChar = '\0';
                    return;
            }
        }

        // Write one element into destination span according to target ValueType.
        // For float targets, perform IEEE cast (double -> float) before writing bits.
        public static void WriteElementAs(Span<byte> dst, ValueType to, double asDouble, bool asBool, char asChar)
        {
            switch (to)
            {
                case ValueType.Bool:
                    dst[0] = (byte)(asBool ? 1 : 0);
                    return;

                case ValueType.Char16:
                    BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)asChar);
                    return;

                case ValueType.Int8:
                    {
                        sbyte vv = (sbyte)System.Math.Truncate(asDouble);
                        dst[0] = unchecked((byte)vv);
                        return;
                    }
                case ValueType.UInt8:
                    {
                        byte vv = (byte)System.Math.Truncate(asDouble);
                        dst[0] = vv;
                        return;
                    }
                case ValueType.Int16:
                    {
                        short vv = (short)System.Math.Truncate(asDouble);
                        BinaryPrimitives.WriteInt16LittleEndian(dst, vv);
                        return;
                    }
                case ValueType.UInt16:
                    {
                        ushort vv = (ushort)System.Math.Truncate(asDouble);
                        BinaryPrimitives.WriteUInt16LittleEndian(dst, vv);
                        return;
                    }
                case ValueType.Int32:
                    {
                        int vv = (int)System.Math.Truncate(asDouble);
                        BinaryPrimitives.WriteInt32LittleEndian(dst, vv);
                        return;
                    }
                case ValueType.UInt32:
                    {
                        uint vv = (uint)System.Math.Truncate(asDouble);
                        BinaryPrimitives.WriteUInt32LittleEndian(dst, vv);
                        return;
                    }
                case ValueType.Int64:
                    {
                        long vv = (long)System.Math.Truncate(asDouble);
                        BinaryPrimitives.WriteInt64LittleEndian(dst, vv);
                        return;
                    }
                case ValueType.UInt64:
                    {
                        ulong vv = (ulong)System.Math.Truncate(asDouble);
                        BinaryPrimitives.WriteUInt64LittleEndian(dst, vv);
                        return;
                    }
                case ValueType.Float32:
                    {
                        // IEEE cast double->float, then write float bits as little-endian
                        float f = (float)asDouble;
                        int bits = BitConverter.SingleToInt32Bits(f);
                        BinaryPrimitives.WriteInt32LittleEndian(dst, bits);
                        return;
                    }
                case ValueType.Float64:
                    {
                        long bits = BitConverter.DoubleToInt64Bits(asDouble);
                        BinaryPrimitives.WriteInt64LittleEndian(dst, bits);
                        return;
                    }
                default:
                    dst.Clear();
                    return;
            }
        }
    }
}
