using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Amlos.Container
{
    /// <summary>
    /// A fixed-layout byte container defined by a Schema.
    /// Backing buffer is rented from the per-Schema FixedBytePool and returned on Dispose().
    /// </summary>
    internal sealed partial class Container : IDisposable
    {
        public const int Version = 0;
        public static readonly ArrayPool<byte> DefaultPool = ArrayPool<byte>.Create();



        private ulong _id;              // assigned by registry
        private byte[] _buffer;         // exact size == _schema.Stride (or Array.Empty for 0)  
        private bool _disposed;
        private int _generation;


        /// <summary> object id </summary>
        public ulong ID => _id;
        /// <summary> Generation </summary>
        public int Generation => _generation;


        /// <summary> Field Header <summary>
        public unsafe ref ContainerHeader Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return ref *(ContainerHeader*)Unsafe.AsPointer(ref _buffer[0]);
            }
        }

        /// <summary> Logical length in bytes (== Schema.Stride).</summary>
        public unsafe ref int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return ref *(int*)Unsafe.AsPointer(ref _buffer[0]);
            }
        }

        /// <summary> Number of fields </summary>
        public unsafe int FieldCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return *(int*)Unsafe.AsPointer(ref _buffer[ContainerHeader.FieldCountOffset]);
            }
        }

        public ContainerView View => new ContainerView(_buffer);

        /// <summary>Logical memory slice [0..length).</summary>
        public Span<byte> Span => _buffer.AsSpan(0, Length);

        /// <summary>Data slice [DataBase..Stride).</summary>
        public Span<byte> DataSegment => View.DataSegment;

        public ref byte[] Buffer => ref _buffer;




        /// <summary>
        /// Create an empty container (for internal use only).
        /// </summary>
        private Container()
        {
            // mark disposed, since not usable
            _disposed = true;
        }





        private void Initialize(int size)
        {
            if (size < ContainerHeader.Size)
                size = ContainerHeader.Size;

            _generation++;
            _disposed = false;
            _buffer = DefaultPool.Rent((int)size);
            ContainerHeader.WriteLength(_buffer, size);
        }

        private void Initialize(byte[] buffer)
        {
            _generation++;
            _disposed = false;
            _buffer = buffer;
        }

        public void Dispose()
        {
            if (_disposed) return;

            // set disposed
            _disposed = true;

            // return buffer to pool
            if (_buffer.Length != 0 && !ReferenceEquals(_buffer, Array.Empty<byte>()))
            {
                // Clear only the logical slice (avoid clearing entire array for perf)
                DefaultPool.Return(_buffer);
            }
            _buffer = Array.Empty<byte>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Container EnsureNotDisposed()
        {
            return !_disposed ? this : ThrowHelper.ThrowDisposed();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Container EnsureNotDisposed(int generation)
        {
            return !_disposed && this._generation == generation ? this : ThrowHelper.ThrowDisposed();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int IndexOf(ReadOnlySpan<char> fieldName)
        {
            EnsureNotDisposed();
            int lo = 0;
            int hi = FieldCount - 1;
            // Standard binary search (ordinal compare on UTF-16 chars)
            while (lo <= hi)
            {
                // Unsigned shift to avoid overflow on large ranges
                int mid = (int)((uint)(lo + hi) >> 1);

                ref var header = ref GetFieldHeader(mid);
                ReadOnlySpan<char> midName = GetFieldName(in header);

                // Ordinal (code-point) lexicographic comparison
                int cmp = midName.SequenceCompareTo(fieldName);

                if (cmp == 0)
                    return mid;

                if (cmp < 0)
                    lo = mid + 1;
                else
                    hi = mid - 1;
            }

            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryGetFieldHeader(ReadOnlySpan<char> fieldName, out Span<FieldHeader> outHeader)
        {
            outHeader = default;
            int lo = 0;
            int hi = FieldCount - 1;
            // Standard binary search (ordinal compare on UTF-16 chars)
            while (lo <= hi)
            {
                // Unsigned shift to avoid overflow on large ranges
                int mid = (int)((uint)(lo + hi) >> 1);

                ref var header = ref GetFieldHeader(mid);
                ReadOnlySpan<char> midName = GetFieldName(in header);

                // Ordinal (code-point) lexicographic comparison
                int cmp = midName.SequenceCompareTo(fieldName);

                if (cmp == 0)
                {
                    outHeader = MemoryMarshal.CreateSpan(ref header, 1);
                    return true;
                }

                if (cmp < 0)
                    lo = mid + 1;
                else
                    hi = mid - 1;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref FieldHeader GetFieldHeader<T>(ReadOnlySpan<char> fieldName, bool allowRescheme) where T : unmanaged
        {
            if (TryGetFieldHeader(fieldName, out var headerSpan))
            {
                return ref headerSpan[0];
            }
            if (allowRescheme)
            {
                var index = ReschemeForNew<T>(fieldName);
                return ref GetFieldHeader(index);
            }
            throw new ArgumentException(nameof(fieldName));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe ref FieldHeader GetFieldHeader(int index) => ref *(FieldHeader*)Unsafe.AsPointer(ref _buffer[ContainerHeader.Size + index * FieldHeader.Size]);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe ref FieldHeader GetFieldHeader_Unsafe(int index)
        {
            fixed (byte* ap = _buffer)
            {
                ref byte start = ref *ap;
                ref byte at = ref Unsafe.Add(ref start, ContainerHeader.Size + index * FieldHeader.Size);
                return ref Unsafe.As<byte, FieldHeader>(ref at);
            }
        }





        public Span<byte> GetFieldData(in FieldHeader header) => MemoryMarshal.CreateSpan(ref _buffer[header.DataOffset], header.Length);

        public unsafe void* GetFieldData_Unsafe(in FieldHeader header) => Unsafe.AsPointer(ref _buffer[header.DataOffset]);

        /// <summary>Get UTF-16 field name by index without allocations.</summary>
        public ReadOnlySpan<char> GetFieldName(int index) => GetFieldName(in GetFieldHeader(index));

        /// <summary>Get UTF-16 field name by index without allocations.</summary>
        public ReadOnlySpan<char> GetFieldName(in FieldHeader header) => MemoryMarshal.Cast<byte, char>(Span.Slice(header.NameOffset, header.NameLength * sizeof(char)));



        #region Byte-span accessors

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> GetSpan(int offset, int length) => _buffer.AsSpan(offset, length);

        /// <summary>Returns a writable span for the given field.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetSpan(int fieldIndex) => View.GetFieldBytes(fieldIndex);

        /// <summary>Returns a writable span for the given field.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetReadOnlySpan(int fieldIndex) => GetSpan(fieldIndex);

        /// <summary>Returns a read-only span for the given field.</summary>
        public Span<T> GetSpan<T>(int index) where T : unmanaged => MemoryMarshal.Cast<byte, T>(GetSpan(index));

        /// <summary>Returns a read-only span for the given field.</summary>
        public ReadOnlySpan<T> GetReadOnlySpan<T>(int index) where T : unmanaged => GetSpan<T>(index);

        public Span<byte> GetSpan(ReadOnlySpan<char> fieldName) => GetSpan(IndexOf(fieldName));

        public ReadOnlySpan<byte> GetReadOnlySpan(ReadOnlySpan<char> fieldName) => GetReadOnlySpan<byte>(IndexOf(fieldName));

        /// <summary>Returns a read-only span for the given field.</summary>
        public Span<T> GetSpan<T>(string fieldName) where T : unmanaged => MemoryMarshal.Cast<byte, T>(GetSpan(fieldName));

        /// <summary>Returns a read-only span for the given field.</summary>
        public ReadOnlySpan<T> GetReadOnlySpan<T>(string fieldName) where T : unmanaged => GetSpan<T>(fieldName);

        #endregion


        #region Raw byte read/write by name

        public void WriteBytes(string fieldName, ReadOnlySpan<byte> src)
        {
            var index = IndexOf(fieldName);
            var f = View.Fields[index];
            if (src.Length != f.Length)
                throw new ArgumentException($"Source length {src.Length} must equal field length {f.Length}.", nameof(src));
            src.CopyTo(GetSpan(index));
        }

        public void ReadBytes(string fieldName, Span<byte> dst)
        {
            var index = IndexOf(fieldName);
            var f = View.Fields[index];
            if (dst.Length != f.Length)
                throw new ArgumentException($"Destination length {dst.Length} must equal field length {f.Length}.", nameof(dst));
            GetReadOnlySpan(index).CopyTo(dst);
        }

        public bool TryWriteBytes(string fieldName, ReadOnlySpan<byte> src)
        {
            var index = IndexOf(fieldName);
            if (index < 0) return false;
            var f = View.Fields[index];
            if (src.Length != f.Length) return false;
            src.CopyTo(GetSpan(index));
            return true;
        }

        public bool TryReadBytes(string fieldName, Span<byte> dst)
        {
            var index = IndexOf(fieldName);
            if (index < 0) return false;
            var f = View.Fields[index];
            if (dst.Length != f.Length) return false;
            GetReadOnlySpan(index).CopyTo(dst);
            return true;
        }

        #endregion


        #region Blittable T read/write (unmanaged) 

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(string fieldName, T value, bool allowRescheme = true) where T : unmanaged
        {
            EnsureNotDisposed();
            // nonexist
            Write_Internal(ref GetFieldHeader<T>(fieldName, allowRescheme), value, allowRescheme);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWrite<T>(string fieldName, T value, bool allowRescheme = true) where T : unmanaged
        {
            EnsureNotDisposed();
            // nonexist
            if (!TryGetFieldHeader(fieldName, out var headerSpan)) return false;
            return TryWrite_Internal(ref headerSpan[0], value, allowRescheme) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write_Internal<T>(ref FieldHeader header, T value, bool allowResize = true) where T : unmanaged
        {
            int v = TryWrite_Internal(ref header, value, allowResize);
            if (v == 0) return;
            ThrowHelper.ThrowWriteError(v, typeof(T), this, -1, allowResize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int TryWrite_Internal<T>(ref FieldHeader f, T value, bool allowResize) where T : unmanaged
        {
            if (TryWriteScalarImplicit(ref f, value))
                return 0;

            int sz = Unsafe.SizeOf<T>();
            // same size, override 
            if (f.Length == sz)
            {
                Write_Override(ref f, ref value);
                return 0;
            }
            // too small? rescheme
            if (f.Length < sz)
            {
                if (!allowResize) return 2;
                // currently can't contain the value, rescheme
                ReschemeAndWrite(ref f, value);
                return 0;
            }
            // too large? explicit cast
            else
            {
                return TryWriteScalarExplicit(ref f, value) ? 0 : 1;
            }
        }

        private void ReschemeAndWrite<T>(ref FieldHeader header, T value) where T : unmanaged
        {
            int newIndex = ReschemeFor<T>(GetFieldName(in header));
            // update to new field
            Write_Override(ref GetFieldHeader(newIndex), ref value);
        }

        private void Write_Override<T>(ref FieldHeader header, ref T value) where T : unmanaged
        {
            var span = GetFieldData(in header);
            if (Unsafe.SizeOf<T>() < header.Length) span.Clear(); // avoid stale trailing bytes
            MemoryMarshal.Write(span, ref value);
            header.FieldType = TypeUtil.PrimOf<T>();
        }







        public T Read<T>(ReadOnlySpan<char> fieldName) where T : unmanaged
        {
            EnsureNotDisposed();

            ref var f = ref GetFieldHeader<T>(fieldName, true);
            if (TryReadScalarExplicit(ref f, out T result))
                return result;

            throw new InvalidOperationException();
        }

        public bool TryRead<T>(ReadOnlySpan<char> fieldName, out T value) where T : unmanaged
        {
            EnsureNotDisposed();

            if (!TryGetFieldHeader(fieldName, out var outHeader))
            {
                value = default;
                return false;
            }

            if (TryReadScalarExplicit(ref outHeader[0], out value))
                return true;

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe T Read_Unsafe<T>(ReadOnlySpan<char> fieldName) where T : unmanaged => Unsafe.Read<T>(GetFieldData_Unsafe(in GetFieldHeader<T>(fieldName, true)));




        public ReadOnlyValueView GetValueView(string fieldName)
        {
            int index = IndexOf(fieldName);
            return new ReadOnlyValueView(GetSpan(index), View.Fields[index].FieldType.Type);
        }

        public ReadOnlyValueView GetValueView(int index)
        {
            return new ReadOnlyValueView(GetSpan(index), View.Fields[index].FieldType.Type);
        }

        #endregion


        #region Object

        public ref ContainerReference GetRef(string fieldName) => ref GetRef(GetFieldIndexOrReschemeObject(fieldName));

        public ref ContainerReference GetRefNoRescheme(string fieldName) => ref GetRef(IndexOf(fieldName));

        public ref ContainerReference GetRefNoRescheme(int index) => ref GetRef(index);

        public ref ContainerReference GetRef(int index)
        {
            var f = View.GetField(index);
            if (!f.IsRef)
                throw new ArgumentException($"Field '{f.Name.ToString()}' is not a ref slot.");
            return ref f.GetSpan<ContainerReference>()[0];
        }

        public Span<ContainerReference> GetRefSpan(string fieldName) => GetRefSpan(IndexOf(fieldName));

        public Span<ContainerReference> GetRefSpan(int index)
        {
            var f = View.GetField(index);
            if (!f.IsRef) throw new ArgumentException($"Field '{f.Name.ToString()}' is not a ref field.");
            if (f.Length % ContainerReference.Size != 0)
                throw new ArgumentException($"Field '{f.Name.ToString()}' byte length is not multiple of {ContainerReference.Size}.");
            return f.GetSpan<ContainerReference>();
        }

        public void WriteObject(string fieldName, Container container)
        {
            GetRefNoRescheme(fieldName) = container.ID;
        }

        #endregion 


        #region Type Hint 

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetScalarType<T>(int fieldIndex) where T : unmanaged
        {
            ref var header = ref View[fieldIndex].Header;
            header.FieldType = TypeUtil.FieldType<T>(false);
            header.ElemSize = (short)Unsafe.SizeOf<T>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetArrayType<T>(int fieldIndex) where T : unmanaged
        {
            ref var header = ref View[fieldIndex].Header;
            header.FieldType = TypeUtil.FieldType<T>(true);
            header.ElemSize = (short)Unsafe.SizeOf<T>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetType(int fieldIndex, ValueType type, bool isArray, short elementSize)
        {
            ref var header = ref View[fieldIndex].Header;
            header.FieldType = TypeUtil.Pack(type, isArray);
            header.ElemSize = elementSize;
        }

        #endregion


        #region Whole-record helpers

        public void Clear()
        {
            EnsureNotDisposed();
            Span.Clear();
        }

        public Container Clone()
        {
            EnsureNotDisposed();
            return Registry.Shared.CreateWild(_buffer);
        }

        public void CopyFrom(Container other)
        {
            EnsureNotDisposed();
            if (other is null) throw new ArgumentNullException(nameof(other));
            if (Length < other.Length)
                throw new ArgumentException($"Destination length {Length} is smaller than source length {other.Length}.", nameof(other));
            other.Span.CopyTo(Span);
        }

        public void CopyTo(Span<byte> destination)
        {
            EnsureNotDisposed();
            if (destination.Length != Length)
                throw new ArgumentException($"Destination length {destination.Length} must equal {Length}.", nameof(destination));
            Span.CopyTo(destination);
        }

        #endregion





        public override string ToString()
        {
            StringBuilder sb = new();
            sb.Append("{");
            for (int i = 0; i < View.FieldCount; i++)
            {
                var field = View[i];
                var valueView = GetValueView(i);
                sb.AppendLine();
                sb.Append("\"");
                sb.Append(field.Name);
                sb.Append("\"");
                sb.Append(": ");
                sb.Append(valueView.ToString());
                sb.Append(",");
            }
            if (sb.Length > 1)
                sb.Length--;

            sb.AppendLine();
            sb.AppendLine("}");
            return sb.ToString();
        }
    }

    static class ThrowHelper
    {
        private const string ParamName = "value";

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowWriteError(int err, Type t, Container c, int index, bool allowRescheme)
        {
            throw err switch
            {
                1 => new ArgumentException($"Type {t.Name} cannot cast to {c.GetFieldHeader(index).Type}.", ParamName),
                2 => new ArgumentException($"Type {t.Name} exceeds field length and cannot write into {c.GetFieldName(index).ToString()} without rescheme.", ParamName),
                _ => new ArgumentException($"Type {t.Name} cannot write to {c.GetFieldHeader(index).Type}.", ParamName),
            };
        }

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static Container ThrowDisposed() => throw new ObjectDisposedException(nameof(Container));


        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowIndexOutOfRange() => throw new ArgumentOutOfRangeException("index");

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowInvalidOperation() => throw new InvalidOperationException();
    }
}
