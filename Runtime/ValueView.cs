using System;
using System.Buffers.Binary;
using static Amlos.Container.MigrationConverter;

namespace Amlos.Container
{
    public readonly ref struct ValueView
    {
        public ValueType Type { get; }
        public ReadOnlySpan<byte> Bytes { get; }


        public ValueView(ReadOnlySpan<byte> bytes, ValueType type) : this()
        {
            Bytes = bytes;
            Type = type;
        }


        public void WriteTo(Span<byte> dst, ValueType targetType, bool isExplicit = false)
        {
            // If the source and target types are identical, copy exactly the bytes needed.
            // This avoids copying extra bytes if the backing Bytes is larger than the type size.
            if (Type == targetType)
            {
                // Copy only the number of bytes the target type requires.
                Bytes.CopyTo(dst);
                return;
            }

            // Classify source and destination types for fewer pairwise cases.
            bool srcIsInt = Type.IsIntegral();
            bool dstIsInt = targetType.IsIntegral();
            bool srcIsFloat = Type.IsFloatingPoint();
            bool dstIsFloat = targetType.IsFloatingPoint();
            bool srcIsBool = Type == ValueType.Bool;
            bool dstIsBool = targetType == ValueType.Bool;
            bool srcIsChar = Type == ValueType.Char16;
            bool dstIsChar = targetType == ValueType.Char16;

            // ---- Boolean conversions ----
            // Bool -> Bool (normalized to 0/1)
            if (srcIsBool && dstIsBool)
            {
                dst[0] = (byte)(Bytes.Length > 0 && Bytes[0] != 0 ? 1 : 0);
                return;
            }

            // Bool -> Non-Bool: treat true as 1.0, false as 0.0 and reuse numeric path
            if (srcIsBool && !dstIsBool)
            {
                bool b = Bytes.Length > 0 && Bytes[0] != 0;
                double d = b ? 1.0 : 0.0;
                WriteFromDouble(dst, targetType, d);
                return;
            }

            // Non-Bool -> Bool: non-zero becomes true
            if (!srcIsBool && dstIsBool)
            {
                bool nonzero = !IsZero(Bytes, Type);
                dst[0] = (byte)(nonzero ? 1 : 0);
                return;
            }

            // ---- Char16 handling (treat as unsigned 16-bit) ----
            if (srcIsChar && dstIsChar)
            {
                // both are char16 but Type != targetType, still copy the 2 bytes
                Bytes.CopyTo(dst);
                return;
            }

            if (srcIsChar && !dstIsChar)
            {
                // read char as unsigned 16-bit and convert via integer path
                ushort u = BinaryPrimitives.ReadUInt16LittleEndian(Bytes);
                WriteFromULong(dst, targetType, u);
                return;
            }

            if (!srcIsChar && dstIsChar)
            {
                // convert numeric/float/bool to ushort (truncate floats)
                if (srcIsFloat)
                {
                    double d = ReadDouble(Bytes, Type);
                    ushort u = (ushort)(int)Math.Truncate(d);
                    BinaryPrimitives.WriteUInt16LittleEndian(dst, u);
                }
                else // integer or other
                {
                    ulong u = ReadULong(Bytes, Type);
                    BinaryPrimitives.WriteUInt16LittleEndian(dst, (ushort)u);
                }
                return;
            }

            // ---- Integer <-> Integer conversions ----
            if (srcIsInt && dstIsInt)
            {
                // Use unsigned read if source is unsigned, otherwise signed read.
                if (Type.IsUnsignedInteger())
                {
                    ulong u = ReadULong(Bytes, Type);
                    WriteFromULong(dst, targetType, u);
                }
                else
                {
                    long s = ReadLong(Bytes, Type);
                    WriteFromLong(dst, targetType, s);
                }
                return;
            }

            // ---- Integer -> Float conversions ----
            if (srcIsInt && dstIsFloat)
            {
                double d = Type.IsUnsignedInteger() ? (double)ReadULong(Bytes, Type) : (double)ReadLong(Bytes, Type);
                WriteFromDouble(dst, targetType, d);
                return;
            }

            // ---- Float -> Integer conversions ----
            if (srcIsFloat && dstIsInt)
            {
                double d = ReadDouble(Bytes, Type);
                long s = (long)Math.Truncate(d); // truncate toward zero, consistent with (long)d
                WriteFromLong(dst, targetType, s);
                return;
            }

            // ---- Float <-> Float conversions ----
            if (srcIsFloat && dstIsFloat)
            {
                if (targetType == ValueType.Float32)
                {
                    float f = (float)ReadDouble(Bytes, Type); // may convert double->float
                    int bits = BitConverter.SingleToInt32Bits(f);
                    BinaryPrimitives.WriteInt32LittleEndian(dst, bits);
                }
                else // target float64
                {
                    double d = ReadDouble(Bytes, Type);
                    long bits = BitConverter.DoubleToInt64Bits(d);
                    BinaryPrimitives.WriteInt64LittleEndian(dst, bits);
                }
                return;
            }

            // Fallback for unsupported conversions
            throw new NotSupportedException($"conversion {Type} -> {targetType} not supported");
        }
    }
}
