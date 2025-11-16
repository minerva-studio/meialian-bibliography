using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Minerva.DataStorage
{
    /// <summary>
    /// A fixed-layout byte container defined by a Schema.
    /// Backing buffer is rented from the per-Schema FixedBytePool and returned on Dispose().
    /// </summary>
    internal sealed partial class Container : IDisposable
    {
        public const int Version = 0;


        private ulong _id;              // assigned by registry 
        private AllocatedMemory _memory;
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
                return ref Unsafe.As<byte, ContainerHeader>(ref _memory[0]);
            }
        }

        /// <summary> Logical length in bytes (== Schema.Stride).</summary>
        public unsafe ref int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return ref Unsafe.As<byte, int>(ref _memory[0]);
            }
        }

        /// <summary> Number of fields </summary>
        public unsafe int FieldCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return *(int*)Unsafe.AsPointer(ref _memory[ContainerHeader.FieldCountOffset]);
            }
        }

        public ContainerView View => new ContainerView(Span);

        /// <summary>Logical memory slice [0..length).</summary>
        public Span<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _memory.Span;
        }

        /// <summary>Headers slice [0..DataBase).</summary>
        public Span<byte> HeadersSegment
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                ref var header = ref this.Header;
                return _memory.AsSpan(0, header.DataOffset);
            }
        }

        /// <summary>Data slice [DataBase..Stride).</summary>
        public Span<byte> DataSegment
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                ref var header = ref this.Header;
                return _memory.AsSpan(header.DataOffset, header.Length - header.DataOffset);
            }
        }

        /// <summary>Data slice [NameBase..DataBase).</summary>
        public Span<byte> NameSegment
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                ref var header = ref this.Header;
                return _memory.AsSpan(header.NameOffset, header.DataOffset - header.NameOffset);
            }
        }

        public ref AllocatedMemory Memory => ref _memory;




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
            _memory = AllocatedMemory.Create(size);
            ContainerHeader.WriteLength(_memory.Span, size);
        }

        private void Initialize(in AllocatedMemory m)
        {
            _generation++;
            _disposed = false;
            _memory = m;
        }

        public void Dispose()
        {
            if (_disposed) return;

            // set disposed
            _disposed = true;
            _memory.Dispose();
            _memory = default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Container EnsureNotDisposed() => !_disposed ? this : ThrowHelper.ThrowDisposed<Container>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Container EnsureNotDisposed(int generation) => !_disposed && this._generation == generation ? this : ThrowHelper.ThrowDisposed<Container>();






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
        public ref FieldHeader GetFieldHeader(ReadOnlySpan<char> fieldName)
        {
            if (!TryGetFieldHeader(fieldName, out var headerSpan))
                ThrowHelper.ThrowArugmentException(nameof(fieldName));
            return ref headerSpan[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref FieldHeader GetFieldHeader<T>(ReadOnlySpan<char> fieldName, bool allowRescheme) where T : unmanaged
        {
            if (!TryGetFieldHeader(fieldName, out var headerSpan))
            {
                if (allowRescheme)
                {
                    int index = ReschemeForNew<T>(fieldName);
                    return ref GetFieldHeader(index);
                }
                else ThrowHelper.ThrowArugmentException(nameof(fieldName));
            }
            return ref headerSpan[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe ref FieldHeader GetFieldHeader(int index) => ref Unsafe.As<byte, FieldHeader>(ref _memory[ContainerHeader.Size + index * FieldHeader.Size]);






        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetFieldData(in FieldHeader header) => _memory.AsSpan(header.DataOffset, header.Length);// MemoryMarshal.CreateSpan(ref _memory[header.DataOffset], header.Length);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> GetFieldData<T>(in FieldHeader header) where T : unmanaged => MemoryMarshal.Cast<byte, T>(GetFieldData(in header));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void* GetFieldData_Unsafe(in FieldHeader header) => _memory.GetPointer(header.DataOffset);

        /// <summary>Get UTF-16 field name by index without allocations.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<char> GetFieldName(int index) => GetFieldName(in GetFieldHeader(index));

        /// <summary>Get UTF-16 field name by index without allocations.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<char> GetFieldName(in FieldHeader header) => MemoryMarshal.Cast<byte, char>(Span.Slice(header.NameOffset, header.NameLength * sizeof(char)));





        #region Raw byte read/write by name

        public void WriteBytes(string fieldName, ReadOnlySpan<byte> src)
        {
            ref var f = ref GetFieldHeader(fieldName);
            if (src.Length != f.Length)
                throw new ArgumentException($"Source length {src.Length} must equal field length {f.Length}.", nameof(src));
            src.CopyTo(GetFieldData(in f));
        }

        public void ReadBytes(string fieldName, Span<byte> dst)
        {
            ref var f = ref GetFieldHeader(fieldName);
            if (dst.Length != f.Length)
                throw new ArgumentException($"Destination length {dst.Length} must equal field length {f.Length}.", nameof(dst));
            GetFieldData(in f).CopyTo(dst);
        }

        public bool TryWriteBytes(string fieldName, ReadOnlySpan<byte> src)
        {
            if (!TryGetFieldHeader(fieldName, out var outHeader))
                return false;
            ref var f = ref outHeader[0];
            if (src.Length != f.Length)
                return false;
            src.CopyTo(GetFieldData(in f));
            return true;
        }

        public bool TryReadBytes(string fieldName, Span<byte> dst)
        {
            if (!TryGetFieldHeader(fieldName, out var outHeader))
                return false;
            ref var f = ref outHeader[0];
            if (dst.Length != f.Length)
                return false;
            GetFieldData(in f).CopyTo(dst);
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




        public ReadOnlyValueView GetValueView(ReadOnlySpan<char> fieldName)
        {
            ref FieldHeader f = ref GetFieldHeader(fieldName);
            return new ReadOnlyValueView(GetFieldData(in f), f.Type);
        }

        public ReadOnlyValueView GetValueView(int index)
        {
            ref FieldHeader f = ref GetFieldHeader(index);
            return new ReadOnlyValueView(GetFieldData(in f), f.Type);
        }

        #endregion


        #region Object

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ContainerReference GetRef(ReadOnlySpan<char> fieldName)
        {
            int index = IndexOf(fieldName);
            if (index < 0) index = ReschemeForNewObject(fieldName);
            return ref GetRef(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ContainerReference GetRefNoRescheme(ReadOnlySpan<char> fieldName) => ref GetRef(IndexOf(fieldName));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ContainerReference GetRefNoRescheme(int index) => ref GetRef(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ContainerReference GetRef(int index)
        {
            ref var f = ref GetFieldHeader(index);
            if (!f.IsRef)
                throw new ArgumentException($"Field '{GetFieldName(in f).ToString()}' is not a ref slot.");
            return ref GetFieldData<ContainerReference>(in f)[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<ContainerReference> GetRefSpan(ReadOnlySpan<char> fieldName) => GetRefSpan(IndexOf(fieldName));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<ContainerReference> GetRefSpan(int index)
        {
            ref var f = ref GetFieldHeader(index);
            if (!f.IsRef) throw new ArgumentException($"Field '{GetFieldName(in f).ToString()}' is not a ref field.");
            if (f.Length % ContainerReference.Size != 0)
                throw new ArgumentException($"Field '{GetFieldName(in f).ToString()}' byte length is not multiple of {ContainerReference.Size}.");
            return GetFieldData<ContainerReference>(in f);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteObject(ReadOnlySpan<char> fieldName, Container container) => GetRefNoRescheme(fieldName) = container.ID;

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
            return Registry.Shared.CreateWild(_memory.Span);
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
            for (int i = 0; i < FieldCount; i++)
            {
                ref var field = ref GetFieldHeader(i);
                var valueView = GetValueView(i);
                sb.AppendLine();
                sb.Append("\"");
                sb.Append(GetFieldName(in field));
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
        public static T ThrowDisposed<T>() => throw new ObjectDisposedException(nameof(Container));


        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowIndexOutOfRange() => throw new ArgumentOutOfRangeException("index");

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowInvalidOperation() => throw new InvalidOperationException();

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowArugmentException(string v) => throw new ArgumentException(v);
    }
}
