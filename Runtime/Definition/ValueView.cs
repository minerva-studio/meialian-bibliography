using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    public readonly ref struct ValueView
    {
        public ValueType Type { get; }
        public Span<byte> Bytes { get; }


        public bool IsNaN
        {
            get
            {
                if (Type == ValueType.Float32)
                {
                    // read 4 bytes little-endian as int bits -> float -> IsNaN
                    int bits = BinaryPrimitives.ReadInt32LittleEndian(Bytes);
                    float f = BitConverter.Int32BitsToSingle(bits);
                    return float.IsNaN(f);
                }
                if (Type == ValueType.Float64)
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(Bytes);
                    double d = BitConverter.Int64BitsToDouble(bits);
                    return double.IsNaN(d);
                }
                return false; // not a float type -> cannot be NaN
            }
        }

        public bool IsPositiveInfinity
        {
            get
            {
                if (Type == ValueType.Float32)
                {
                    int bits = BinaryPrimitives.ReadInt32LittleEndian(Bytes);
                    float f = BitConverter.Int32BitsToSingle(bits);
                    return float.IsPositiveInfinity(f);
                }
                if (Type == ValueType.Float64)
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(Bytes);
                    double d = BitConverter.Int64BitsToDouble(bits);
                    return double.IsPositiveInfinity(d);
                }
                return false;
            }
        }

        public bool IsNegativeInfinity
        {
            get
            {
                if (Type == ValueType.Float32)
                {
                    int bits = BinaryPrimitives.ReadInt32LittleEndian(Bytes);
                    float f = BitConverter.Int32BitsToSingle(bits);
                    return float.IsNegativeInfinity(f);
                }
                if (Type == ValueType.Float64)
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(Bytes);
                    double d = BitConverter.Int64BitsToDouble(bits);
                    return double.IsNegativeInfinity(d);
                }
                return false;
            }
        }

        public bool IsInfinity => IsPositiveInfinity || IsNegativeInfinity;

        public bool IsFinite
        {
            get
            {
                if (Type == ValueType.Float32)
                {
                    int bits = BinaryPrimitives.ReadInt32LittleEndian(Bytes);
                    float f = BitConverter.Int32BitsToSingle(bits);
                    return !float.IsNaN(f) && !float.IsInfinity(f);
                }
                if (Type == ValueType.Float64)
                {
                    long bits = BinaryPrimitives.ReadInt64LittleEndian(Bytes);
                    double d = BitConverter.Int64BitsToDouble(bits);
                    return !double.IsNaN(d) && !double.IsInfinity(d);
                }
                // non-float types are always finite from FP perspective
                return true;
            }
        }



        public ValueView(Span<byte> span, ValueType type) : this()
        {
            this.Bytes = span;
            Type = type;
        }




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>(bool isExplicit = true) where T : unmanaged => AsReadOnly().Read<T>(isExplicit);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRead<T>(out T value, bool isExplicit = true) where T : unmanaged => AsReadOnly().TryRead(out value, isExplicit);



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(in T value, bool isExplicit = false) where T : unmanaged
        {
            T copy = value;
            var v = ReadOnlyValueView.Create(ref copy);
            Write(v, isExplicit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(ReadOnlySpan<byte> v, bool isExplicit = false) where T : unmanaged => Write(ReadOnlyValueView.Create<T>(v), isExplicit);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ReadOnlySpan<byte> v, ValueType type, bool isExplicit = false) => Write(new ReadOnlyValueView(v, type), isExplicit);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ReadOnlyValueView v, bool isExplicit = false)
        {
            if (!TryWrite(v, isExplicit))
                throw new InvalidCastException();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWrite<T>(in T value, bool isExplicit = false) where T : unmanaged
        {
            T copy = value;
            var v = ReadOnlyValueView.Create(ref copy);
            return TryWrite(v, isExplicit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWrite<T>(ReadOnlySpan<byte> v, bool isExplicit = false) where T : unmanaged => TryWrite(ReadOnlyValueView.Create<T>(v), isExplicit);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWrite(ReadOnlySpan<byte> v, ValueType type, bool isExplicit = false) => TryWrite(new ReadOnlyValueView(v, type), isExplicit);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWrite(ReadOnlyValueView v, bool isExplicit = false) => v.TryWriteTo(Bytes, Type, isExplicit);




        public bool TryWriteTo(Span<byte> dst, ValueType dstType, bool isExplicit = false)
        {
            // Quick path: identical type -> copy as much as target expects (avoid overrun)
            ValueType srcType = Type;
            ReadOnlySpan<byte> src = Bytes;
            return Migration.TryWriteTo(src, srcType, dst, dstType, isExplicit);
        }





        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlyValueView AsReadOnly() => new ReadOnlyValueView(Bytes, Type);

        public static ValueView Create<T>(ref T value) where T : unmanaged
        {
            var buffer = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref value, 1));
            return new ValueView(buffer, TypeUtil<T>.ValueType);
        }




        public static implicit operator ReadOnlyValueView(ValueView valueView) => valueView.AsReadOnly();
        public static unsafe implicit operator ValueView(int* valueView) => new ValueView(new Span<byte>(valueView, sizeof(int)), ValueType.Int32);
        public static unsafe implicit operator ValueView(long* valueView) => new ValueView(new Span<byte>(valueView, sizeof(long)), ValueType.Int64);
        public static unsafe implicit operator ValueView(float* valueView) => new ValueView(new Span<byte>(valueView, sizeof(float)), ValueType.Float32);
        public static unsafe implicit operator ValueView(double* valueView) => new ValueView(new Span<byte>(valueView, sizeof(double)), ValueType.Float64);


        public override string ToString() => Migration.ToString(Bytes, Type);
    }

    [StructLayout(LayoutKind.Sequential)]
    public readonly unsafe struct ValueView<T> where T : unmanaged
    {
        public ValueType Type => TypeUtil<T>.ValueType;
        private readonly T* value;

        public ValueView(T* value)
        {
            this.value = value;
        }

        public T Read() => *value;
        public void Write(in T value)
        {
            *this.value = value;
        }

        public static implicit operator ReadOnlyValueView(ValueView<T> valueView) => new ValueView(new Span<byte>(valueView.value, TypeUtil<T>.Size), valueView.Type);
        public static implicit operator ValueView(ValueView<T> valueView) => new ValueView(new Span<byte>(valueView.value, TypeUtil<T>.Size), valueView.Type);
        public static implicit operator ValueView<T>(T* v) => new ValueView<T>(v);
    }
}
