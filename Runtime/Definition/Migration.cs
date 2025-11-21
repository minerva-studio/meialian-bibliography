using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Minerva.DataStorage.TypeUtil;

namespace Minerva.DataStorage
{
    public static class Migration
    {
        public static bool TryWriteTo(ReadOnlySpan<byte> src, ValueType srcType, Span<byte> dst, ValueType dstType, bool isExplicit)
        {
            if (srcType == dstType)
            {
                src.CopyTo(dst);
                return true;
            }

            // If implicit-only and conversion not allowed by implicit table -> fail
            if (!isExplicit && !TypeUtil.IsImplicitlyConvertible(srcType, dstType))
                return false;

            // classify types
            bool srcIsInt = srcType.IsIntegral();
            bool dstIsInt = dstType.IsIntegral();
            bool srcIsFloat = srcType.IsFloatingPoint();
            bool dstIsFloat = dstType.IsFloatingPoint();
            bool srcIsBool = srcType == ValueType.Bool;
            bool dstIsBool = dstType == ValueType.Bool;
            bool srcIsChar = srcType == ValueType.Char16;
            bool dstIsChar = dstType == ValueType.Char16;

            try
            {
                // ---- Boolean conversions ----
                if (srcIsBool && dstIsBool)
                {
                    dst[0] = (byte)(src.Length > 0 && src[0] != 0 ? 1 : 0);
                    if (dst.Length > 1) dst.Slice(1).Clear();
                    return true;
                }

                if (srcIsBool && !dstIsBool)
                {
                    bool b = src.Length > 0 && src[0] != 0;
                    double d = b ? 1.0 : 0.0;
                    WriteFromDouble(dst, dstType, d);
                    return true;
                }

                if (!srcIsBool && dstIsBool)
                {
                    bool nonzero = !IsZero(src, srcType);
                    dst[0] = (byte)(nonzero ? 1 : 0);
                    if (dst.Length > 1) dst.Slice(1).Clear();
                    return true;
                }

                // ---- Char16 handling (treat as unsigned 16-bit) ----
                if (srcIsChar && dstIsChar)
                {
                    int copy = Math.Min(2, Math.Min(src.Length, dst.Length));
                    if (copy > 0) src.Slice(0, copy).CopyTo(dst.Slice(0, copy));
                    if (dst.Length > copy) dst.Slice(copy).Clear();
                    return true;
                }

                if (srcIsChar && !dstIsChar)
                {
                    ushort u = BinaryPrimitives.ReadUInt16LittleEndian(src);
                    WriteFromULong(dst, dstType, u);
                    return true;
                }

                if (!srcIsChar && dstIsChar)
                {
                    if (srcIsFloat)
                    {
                        double d = ReadDouble(src, srcType);
                        ushort u = (ushort)(int)Math.Truncate(d);
                        BinaryPrimitives.WriteUInt16LittleEndian(dst, u);
                    }
                    else
                    {
                        ulong u = ReadULong(src, srcType);
                        BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)u);
                    }
                    return true;
                }

                // ---- Integer <-> Integer conversions ----
                if (srcIsInt && dstIsInt)
                {
                    if (srcType.IsUnsignedInteger())
                    {
                        ulong u = ReadULong(src, srcType);
                        WriteFromULong(dst, dstType, u);
                    }
                    else
                    {
                        long s = ReadLong(src, srcType);
                        WriteFromLong(dst, dstType, s);
                    }
                    return true;
                }

                // ---- Integer -> Float conversions ----
                if (srcIsInt && dstIsFloat)
                {
                    double d = srcType.IsUnsignedInteger() ? (double)ReadULong(src, srcType) : (double)ReadLong(src, srcType);
                    WriteFromDouble(dst, dstType, d);
                    return true;
                }

                // ---- Float -> Integer conversions ----
                if (srcIsFloat && dstIsInt)
                {
                    double d = ReadDouble(src, srcType);
                    long s = (long)Math.Truncate(d); // truncate toward zero
                    WriteFromLong(dst, dstType, s);
                    return true;
                }

                // ---- Float <-> Float conversions ----
                if (srcIsFloat && dstIsFloat)
                {
                    if (dstType == ValueType.Float32)
                    {
                        float f = (float)ReadDouble(src, srcType);
                        int bits = BitConverter.SingleToInt32Bits(f);
                        BinaryPrimitives.WriteInt32LittleEndian(dst, bits);
                    }
                    else
                    {
                        double d = ReadDouble(src, srcType);
                        long bits = BitConverter.DoubleToInt64Bits(d);
                        BinaryPrimitives.WriteInt64LittleEndian(dst, bits);
                    }
                    return true;
                }

                // If we fell through, conversion is unsupported even with explicit permission
                return false;
            }
            catch
            {
                // treat any unexpected error as conversion failure (caller should drop/clear)
                return false;
            }
        }




        public static bool MigrateValueFieldBytes(ReadOnlySpan<byte> src, Span<byte> dst, byte oldHint, byte newHint)
        {
            var oldVt = TypeUtil.PrimOf(oldHint);
            var newVt = TypeUtil.PrimOf(newHint);

            return MigrateValueFieldBytes(src, dst, oldVt, newVt);
        }

        public static bool MigrateValueFieldBytes(ReadOnlySpan<byte> src, Span<byte> dst, ValueType oldVt, ValueType newVt, bool isExplicit = false)
        {
            // Unknown fallback: raw copy + zero-fill/truncate
            if (oldVt == ValueType.Unknown || newVt == ValueType.Unknown)
            {
                int copy = Math.Min(src.Length, dst.Length);
                if (copy > 0) src[..copy].CopyTo(dst);
                if (dst.Length > copy) dst[copy..].Clear();
                return false;
            }

            int oldElem = SizeOf(oldVt);
            int newElem = SizeOf(newVt);
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

                var valueView = new ReadOnlyValueView(sSlice, oldVt);
                if (!valueView.TryWriteTo(dSlice, newVt, isExplicit))
                {
                    dSlice.Clear();
                }
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
            // Unknown on either side �� keep bytes unchanged (but caller may still update hint).
            if (oldVt == ValueType.Unknown || newVt == ValueType.Unknown)
                return false;

            ConvertInPlace(bytes, bytes, oldVt, newVt, true);

            // Zero out any tail beyond the new element size (if span larger than element)
            int newElem = SizeOf(newVt);
            if (bytes.Length > newElem)
                bytes[newElem..].Clear();
            return true;
        }

        /// <summary>
        /// In-place convert an array when old and new element sizes are the same.
        /// Reads each element as 'oldHint' and writes as 'newHint'.
        /// </summary> 
        public static void ConvertArrayInPlaceSameSize(Span<byte> bytes, int count, ValueType oldVt, ValueType newVt)
        {
            int elem = SizeOf(oldVt); // == ElemSize(newVt) by contract

            for (int i = 0, off = 0; i < count; i++, off += elem)
            {
                var cell = bytes.Slice(off, elem);
                ConvertInPlace(cell, cell, oldVt, newVt, true);
            }
        }





        public static void ConvertInPlace(Span<byte> src, Span<byte> dst, ValueType from, ValueType to, bool isExplicit = false)
        {
            Span<byte> buffer = stackalloc byte[src.Length];
            src.CopyTo(buffer);
            var view = new ReadOnlyValueView(buffer, from);
            view.TryWriteTo(dst, to, isExplicit);
        }





        public static bool IsZero(ReadOnlySpan<byte> src, ValueType vt)
        {
            switch (vt)
            {
                case ValueType.Bool: return src.Length == 0 || src[0] == 0;
                case ValueType.Int8: return (sbyte)src[0] == 0;
                case ValueType.UInt8: return src[0] == 0;
                case ValueType.Int16: return BinaryPrimitives.ReadInt16LittleEndian(src) == 0;
                case ValueType.UInt16: return BinaryPrimitives.ReadUInt16LittleEndian(src) == 0;
                case ValueType.Int32: return BinaryPrimitives.ReadInt32LittleEndian(src) == 0;
                case ValueType.UInt32: return BinaryPrimitives.ReadUInt32LittleEndian(src) == 0;
                case ValueType.Int64: return BinaryPrimitives.ReadInt64LittleEndian(src) == 0;
                case ValueType.UInt64: return BinaryPrimitives.ReadUInt64LittleEndian(src) == 0;
                case ValueType.Float32: return BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(src)) == 0f;
                case ValueType.Float64: return BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(src)) == 0.0;
                case ValueType.Char16: return BinaryPrimitives.ReadUInt16LittleEndian(src) == 0;
                default: return true;
            }
        }


        #region Integral

        public static ulong ReadULong(ReadOnlySpan<byte> src, ValueType vt)
        {
            switch (vt)
            {
                case ValueType.UInt8: return src[0];
                case ValueType.UInt16: return BinaryPrimitives.ReadUInt16LittleEndian(src);
                case ValueType.UInt32: return BinaryPrimitives.ReadUInt32LittleEndian(src);
                case ValueType.UInt64: return BinaryPrimitives.ReadUInt64LittleEndian(src);
                case ValueType.Int8: return (ulong)(sbyte)src[0];
                case ValueType.Int16: return (ulong)BinaryPrimitives.ReadInt16LittleEndian(src);
                case ValueType.Int32: return (ulong)BinaryPrimitives.ReadInt32LittleEndian(src);
                case ValueType.Int64: return (ulong)BinaryPrimitives.ReadInt64LittleEndian(src);
                case ValueType.Char16: return BinaryPrimitives.ReadUInt16LittleEndian(src);
                default: throw new ArgumentOutOfRangeException(nameof(vt));
            }
        }

        public static long ReadLong(ReadOnlySpan<byte> src, ValueType vt)
        {
            switch (vt)
            {
                case ValueType.Int8: return (sbyte)src[0];
                case ValueType.Int16: return BinaryPrimitives.ReadInt16LittleEndian(src);
                case ValueType.Int32: return BinaryPrimitives.ReadInt32LittleEndian(src);
                case ValueType.Int64: return BinaryPrimitives.ReadInt64LittleEndian(src);
                case ValueType.UInt8: return src[0];
                case ValueType.UInt16: return BinaryPrimitives.ReadUInt16LittleEndian(src);
                case ValueType.UInt32: return BinaryPrimitives.ReadUInt32LittleEndian(src);
                case ValueType.UInt64: return (long)BinaryPrimitives.ReadUInt64LittleEndian(src);
                case ValueType.Char16: return BinaryPrimitives.ReadUInt16LittleEndian(src);
                default: throw new ArgumentOutOfRangeException(nameof(vt));
            }
        }


        public static void WriteFromULong(Span<byte> dst, ValueType to, ulong v)
        {
            switch (to)
            {
                case ValueType.UInt8: dst[0] = (byte)v; return;
                case ValueType.UInt16: BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)v); return;
                case ValueType.UInt32: BinaryPrimitives.WriteUInt32LittleEndian(dst, (uint)v); return;
                case ValueType.UInt64: BinaryPrimitives.WriteUInt64LittleEndian(dst, v); return;
                case ValueType.Int8: dst[0] = ((byte)(sbyte)v); return;
                case ValueType.Int16: BinaryPrimitives.WriteInt16LittleEndian(dst, ((short)v)); return;
                case ValueType.Int32: BinaryPrimitives.WriteInt32LittleEndian(dst, ((int)v)); return;
                case ValueType.Int64: BinaryPrimitives.WriteInt64LittleEndian(dst, ((long)v)); return;
                case ValueType.Char16: BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)v); return;
                case ValueType.Float32:
                    {
                        float f = (float)v;
                        BinaryPrimitives.WriteInt32LittleEndian(dst, BitConverter.SingleToInt32Bits(f));
                        return;
                    }
                case ValueType.Float64:
                    {
                        long bits = BitConverter.DoubleToInt64Bits((double)v);
                        BinaryPrimitives.WriteInt64LittleEndian(dst, bits);
                        return;
                    }
                default:
                    throw new ArgumentOutOfRangeException(nameof(to));
            }
        }

        public static void WriteFromLong(Span<byte> dst, ValueType to, long v)
        {
            switch (to)
            {
                case ValueType.Int8: dst[0] = ((byte)(sbyte)v); return;
                case ValueType.Int16: BinaryPrimitives.WriteInt16LittleEndian(dst, ((short)v)); return;
                case ValueType.Int32: BinaryPrimitives.WriteInt32LittleEndian(dst, ((int)v)); return;
                case ValueType.Int64: BinaryPrimitives.WriteInt64LittleEndian(dst, v); return;
                case ValueType.UInt8: dst[0] = ((byte)v); return;
                case ValueType.UInt16: BinaryPrimitives.WriteUInt16LittleEndian(dst, ((ushort)v)); return;
                case ValueType.UInt32: BinaryPrimitives.WriteUInt32LittleEndian(dst, ((uint)v)); return;
                case ValueType.UInt64: BinaryPrimitives.WriteUInt64LittleEndian(dst, ((ulong)v)); return;
                case ValueType.Char16: BinaryPrimitives.WriteUInt16LittleEndian(dst, ((ushort)v)); return;
                case ValueType.Float32:
                    {
                        float f = (float)v;
                        BinaryPrimitives.WriteInt32LittleEndian(dst, BitConverter.SingleToInt32Bits(f));
                        return;
                    }
                case ValueType.Float64:
                    {
                        long bits = BitConverter.DoubleToInt64Bits((double)v);
                        BinaryPrimitives.WriteInt64LittleEndian(dst, bits);
                        return;
                    }
                default:
                    throw new ArgumentOutOfRangeException(nameof(to));
            }
        }

        #endregion



        public static double ReadDouble(ReadOnlySpan<byte> src, ValueType vt)
        {
            switch (vt)
            {
                case ValueType.Float32:
                    return BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(src));
                case ValueType.Float64:
                    return BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(src));
                default:
                    // fallback: try integer -> double
                    if (vt.IsIntegral())
                    {
                        return vt.IsUnsignedInteger() ? (double)ReadULong(src, vt) : (double)ReadLong(src, vt);
                    }
                    if (vt == ValueType.Bool) return (src.Length > 0 && src[0] != 0) ? 1.0 : 0.0;
                    if (vt == ValueType.Char16) return BinaryPrimitives.ReadUInt16LittleEndian(src);
                    throw new ArgumentOutOfRangeException(nameof(vt));
            }
        }

        public static void WriteFromDouble(Span<byte> dst, ValueType to, double d)
        {
            switch (to)
            {
                case ValueType.Float32:
                    {
                        float f = (float)d;
                        BinaryPrimitives.WriteInt32LittleEndian(dst, BitConverter.SingleToInt32Bits(f));
                        return;
                    }
                case ValueType.Float64:
                    {
                        long bits = BitConverter.DoubleToInt64Bits(d);
                        BinaryPrimitives.WriteInt64LittleEndian(dst, bits);
                        return;
                    }
                case ValueType.Int8:
                case ValueType.Int16:
                case ValueType.Int32:
                case ValueType.Int64:
                    {
                        long s = (long)Math.Truncate(d);
                        WriteFromLong(dst, to, s);
                        return;
                    }
                case ValueType.UInt8:
                case ValueType.UInt16:
                case ValueType.UInt32:
                case ValueType.UInt64:
                    {
                        ulong u = (ulong)Math.Truncate(d);
                        WriteFromULong(dst, to, u);
                        return;
                    }
                case ValueType.Bool:
                    dst[0] = (byte)(d != 0.0 ? 1 : 0);
                    return;
                case ValueType.Char16:
                    BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)(int)Math.Truncate(d));
                    return;
                default:
                    throw new ArgumentOutOfRangeException(nameof(to));
            }
        }


        public static string ToString(ReadOnlySpan<byte> bytes, ValueType type)
        {
            switch (type)
            {
                case ValueType.Blob:
                    return $"{{\"$blob\":\"{Convert.ToBase64String(bytes)}\"}}";
                case ValueType.Unknown:
                    return $"{{\"$RawData\":\"{bytes.ToHex()}\"}}";

                case ValueType.Bool:
                    return ToArrayOrSingleString<bool>(bytes);

                case ValueType.Int8:
                    return ToArrayOrSingleString<sbyte>(bytes);
                case ValueType.UInt8:
                    return ToArrayOrSingleString<byte>(bytes);

                case ValueType.Char16:
                    {
                        // Ensure even number of bytes, truncate if odd.
                        int even = bytes.Length & ~1;
                        if (even <= 0) return string.Empty;
                        var chars = MemoryMarshal.Cast<byte, char>(bytes.Slice(0, even));
                        // Build string from span (not Span<T>.ToString()).
                        return new string(chars);
                    }

                case ValueType.Int16:
                    return ToArrayOrSingleString<short>(bytes);
                case ValueType.UInt16:
                    return ToArrayOrSingleString<ushort>(bytes);
                case ValueType.Int32:
                    return ToArrayOrSingleString<int>(bytes);
                case ValueType.UInt32:
                    return ToArrayOrSingleString<uint>(bytes);
                case ValueType.Int64:
                    return ToArrayOrSingleString<long>(bytes);
                case ValueType.UInt64:
                    return ToArrayOrSingleString<ulong>(bytes);
                case ValueType.Float32:
                    return ToArrayOrSingleString<float>(bytes);
                case ValueType.Float64:
                    return ToArrayOrSingleString<double>(bytes);

                case ValueType.Ref:
                    {
                        // Guard length
                        if (bytes.Length < 8) return "null";
                        ulong id = MemoryMarshal.Read<ulong>(bytes);
                        if (id != 0UL && Container.Registry.Shared.GetContainer(id) is Container container)
                            return container.ToString();
                        return "null";
                    }
            }
            return "Unknown";
        }

        private static string ToArrayOrSingleString<T>(ReadOnlySpan<byte> bytes) where T : unmanaged
        {
            int sz = TypeUtil<T>.Size;
            if (bytes.Length > sz)
            {
                var arr = MemoryMarshal.Cast<byte, T>(bytes[..^(bytes.Length % sz)]);
                return "[" + string.Join(", ", arr.ToArray()) + "]";
            }
            if (bytes.Length < sz)
            {
                // Not enough bytes to represent a single T; return hex to avoid throwing.
                return "Raw:" + bytes.ToHex();
            }
            return MemoryMarshal.Read<T>(bytes).ToString();
        }

        public static string ToHex(this ReadOnlyValueView readOnlyValueView) => readOnlyValueView.Bytes.ToHex();
        public static string ToHex(this ValueView valueView) => ((ReadOnlyValueView)valueView).Bytes.ToHex();
    }

    public static class HexExtensions
    {
        private static readonly char[] HexUpper = "0123456789ABCDEF".ToCharArray();
        private static readonly char[] HexLower = "0123456789abcdef".ToCharArray();

        public static string ToHex(this ReadOnlySpan<byte> bytes, bool uppercase = true)
        {
            if (bytes.Length == 0) return string.Empty;
            var table = uppercase ? HexUpper : HexLower;
            char[] chars = new char[bytes.Length * 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                var b = bytes[i];
                chars[i * 2] = table[b >> 4];
                chars[i * 2 + 1] = table[b & 0x0F];
            }
            return new string(chars);
        }
    }
}
