using System;
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
        private ulong _id;              // assigned by registry 
        private AllocatedMemory _memory;
        private int _generation;
        private int _schemaVersion;
        private bool _disposed;

        /// <summary> object id </summary>
        public ulong ID => _id;
        /// <summary> Generation </summary>
        public int Generation => _generation;
        /// <summary> Schema Version, changed when rescheme </summary>
        public int SchemaVersion => _schemaVersion;


        /// <summary> Field Header <summary>
        public ref ContainerHeader Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return ref Unsafe.As<byte, ContainerHeader>(ref _memory[0]);
            }
        }

        /// <summary> Logical length in bytes (== Schema.Stride).</summary>
        public ref int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return ref Unsafe.As<byte, int>(ref _memory[0]);
            }
        }

        /// <summary> Number of fields </summary>
        public int FieldCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return Unsafe.As<byte, int>(ref _memory[ContainerHeader.FieldCountOffset]);
            }
        }

        /// <summary>True if this container represents an array (single field which is an inline array). </summary>
        public bool IsArray => FieldCount == 1 && GetFieldHeader(0).IsInlineArray;

        /// <summary> For internal testing and debugging only. </summary>
        public ContainerView View
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new ContainerView(Span);
        }

        /// <summary>Logical memory slice [0..length).</summary>
        public Span<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _memory.Buffer.Span;
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

        public Span<char> NameSpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                ref var header = ref this.Header;
                return MemoryMarshal.Cast<byte, char>(_memory.AsSpan(header.ContainerNameOffset, header.ContainerNameLength));
            }
        }

        public string Name => NameSpan.ToString();

        /// <summary>
        /// Version of the container. (User defined version)
        /// </summary>
        public int Version
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Header.Version;
            set => Header.Version = value;
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

        ~Container()
        {
            if (_id != Container.Registry.ID.Wild)
            {
                throw new InvalidOperationException("Non-wild Container Disposed");
            }
        }





        private void Initialize(int size)
        {
            if (size < ContainerHeader.Size)
                size = ContainerHeader.Size;

            _generation++;
            _disposed = false;
            _memory = AllocatedMemory.Create(size);
            _schemaVersion = 0;
            ContainerHeader.WriteLength(_memory.Buffer.Span, size);
        }

        private void Initialize(in AllocatedMemory m)
        {
            _generation++;
            _disposed = false;
            _memory = m;
            _schemaVersion = 0;
        }

        /// <summary>
        /// Mark as dispose but not actually dispose the internal memory
        /// </summary>
        public void MarkDispose()
        {
            _disposed = true;
        }

        public void Dispose()
        {
            // set disposed
            _disposed = true;
            _memory.Dispose();
            _memory = default;
        }

        public bool IsDisposed(int generation) => _disposed || _generation != generation;

        public bool IsDisposed(int generation, int schemaVersion) => _disposed || _generation != generation || _schemaVersion != schemaVersion;




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Container EnsureNotDisposed() => !_disposed ? this : ThrowHelper.ThrowDisposed<Container>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Container EnsureNotDisposed(int generation) => !_disposed && this._generation == generation ? this : ThrowHelper.ThrowDisposed<Container>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Container EnsureNotDisposed(int generation, int schemaVersion) => !IsDisposed(generation, schemaVersion) ? this : ThrowHelper.ThrowDisposed<Container>();







        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int IndexOf(ref FieldHeader f)
        {
            ref byte origin = ref _memory[0];
            ref byte target = ref Unsafe.As<FieldHeader, byte>(ref f);
            nint byteOffset = Unsafe.ByteOffset(ref origin, ref target);
            if (byteOffset < ContainerHeader.Size)
                ThrowHelper.ArgumentException(nameof(f), "FieldHeader does not belong to this Container.");
            if (byteOffset >= ContainerHeader.Size + FieldCount * FieldHeader.Size)
                ThrowHelper.ArgumentException(nameof(f), "FieldHeader does not belong to this Container.");
            // misaligned, skip for now
            //if ((byteOffset - ContainerHeader.Size) % FieldHeader.Size != 0)
            //    return;
            int index = ((int)byteOffset - ContainerHeader.Size) / FieldHeader.Size;
            return index;
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

            return ~lo;
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
                ThrowHelper.ArgumentException(nameof(fieldName));
            return ref headerSpan[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref FieldHeader GetFieldHeader<T>(ReadOnlySpan<char> fieldName, bool allowRescheme) where T : unmanaged
        {
            if (!TryGetFieldHeader(fieldName, out var headerSpan))
            {
                if (allowRescheme)
                {
                    int index = ReschemeFor<T>(fieldName);
                    return ref GetFieldHeader(index);
                }
                else ThrowHelper.ArgumentException(nameof(fieldName));
            }
            return ref headerSpan[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe ref FieldHeader GetFieldHeader(int index) => ref Unsafe.As<byte, FieldHeader>(ref _memory[ContainerHeader.Size + index * FieldHeader.Size]);






        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetFieldData(in FieldHeader header) => _memory.Buffer.Span.Slice(header.DataOffset, header.Length);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> GetFieldData<T>(in FieldHeader header) where T : unmanaged => MemoryMarshal.Cast<byte, T>(_memory.Buffer.Span.Slice(header.DataOffset, header.Length));

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

        /// <remarks>
        /// On rescheme for larger size, field header reference becomes invalid.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write_Internal<T>(ref FieldHeader header, T value, bool allowResize = true) where T : unmanaged
        {
            int v = TryWrite_Internal(ref header, value, allowResize);
            if (v == 0) return;
            ThrowHelper.ThrowWriteError(v, typeof(T), this, header.FieldType, allowResize);
        }

        /// <summary>
        /// Write unmanaged scalar value to field.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="f"></param>
        /// <param name="value"></param>
        /// <param name="allowResize"></param>
        /// <returns></returns>
        /// <remarks>
        /// On rescheme for larger size, field header reference becomes invalid.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int TryWrite_Internal<T>(ref FieldHeader f, T value, bool allowResize) where T : unmanaged
        {
            if (TryWriteScalarImplicit(ref f, value))
                return 0;

            // same size, override 
            if (f.Length == TypeUtil<T>.Size)
            {
                var span = GetFieldData(in f);
                if (TypeUtil<T>.Size < f.Length) span.Clear(); // avoid stale trailing bytes
                MemoryMarshal.Write(span, ref value);
                f.FieldType = TypeUtil<T>.ValueType;
                return 0;
            }
            // too small? rescheme
            if (f.Length < TypeUtil<T>.Size)
            {
                if (!allowResize) return 2;
                Override(GetFieldName(in f), MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan<T>(ref value, 1)), TypeUtil<T>.ValueType);
                return 0;
            }
            // too large? explicit cast
            else
            {
                return TryWriteScalarExplicit(ref f, value) ? 0 : 1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Override(ReadOnlySpan<char> fieldName, ReadOnlySpan<byte> value, ValueType valueType, int? inlineArrayLength = null)
        {
            int elementCount = inlineArrayLength ?? 1;
            int elemSize = valueType == ValueType.Blob ? value.Length / elementCount : TypeUtil.SizeOf(valueType);
            int index = ReschemeFor(fieldName, new TypeData(valueType, (short)elemSize), inlineArrayLength);
            ref var header = ref GetFieldHeader(index);
            value.CopyTo(GetFieldData(in header));
            return index;
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




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueView GetValueView(ReadOnlySpan<char> fieldName)
        {
            ref FieldHeader f = ref GetFieldHeader(fieldName);
            return new ValueView(GetFieldData(in f), f.Type);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueView GetValueView(int index) => GetValueView(in GetFieldHeader(index));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueView GetValueView(in FieldHeader f) => new ValueView(GetFieldData(in f), f.Type);

        #endregion


        #region Object

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ContainerReference GetRef(ReadOnlySpan<char> fieldName)
        {
            int index = IndexOf(fieldName);
            if (index < 0) index = ReschemeForObject(fieldName);
            return ref GetRef(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ContainerReference GetRefNoRescheme(ReadOnlySpan<char> fieldName) => ref GetRef(IndexOf(fieldName));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ContainerReference GetRefNoRescheme(int index) => ref GetRef(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ContainerReference GetRef(int index)
        {
            if (index < 0 || index >= FieldCount)
                ThrowHelper.ThrowIndexOutOfRange();
            ref var f = ref GetFieldHeader(index);
            if (!f.IsRef)
                throw new ArgumentException($"Field '{GetFieldName(in f).ToString()}' is not a ref slot.");
            return ref GetFieldData<ContainerReference>(in f)[0];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<ContainerReference> GetRefSpan(ReadOnlySpan<char> fieldName) => GetRefSpan(in GetFieldHeader(fieldName));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<ContainerReference> GetRefSpan(int index) => GetRefSpan(in GetFieldHeader(index));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<ContainerReference> GetRefSpan(in FieldHeader f)
        {
            if (!f.IsRef) throw new ArgumentException($"Field '{GetFieldName(in f).ToString()}' is not a ref field.");
            if (f.Length % ContainerReference.Size != 0)
                throw new ArgumentException($"Field '{GetFieldName(in f).ToString()}' byte length is not multiple of {ContainerReference.Size}.");
            return GetFieldData<ContainerReference>(in f);
        }
        public bool TryGetRef(ReadOnlySpan<char> fieldName, out Span<ContainerReference> containerReferences)
        {
            int index = IndexOf(fieldName);
            if (index < 0)
            {
                containerReferences = default;
                return false;
            }
            ref var header = ref GetFieldHeader(index);
            if (!header.IsRef)
            {
                containerReferences = default;
                return false;
            }
            containerReferences = GetRefSpan(index);
            return true;
        }

        /// <summary>
        /// INTERNAL DEBUG USE ONLY: Write object reference without validation.
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="container"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteObject(ReadOnlySpan<char> fieldName, Container container)
        {
            GetRefNoRescheme(fieldName) = container.ID;
            if (container != null)
                Registry.Shared.RegisterParent(container, this);
        }

        #endregion



        #region Whole-record helpers

        public void Clear()
        {
            EnsureNotDisposed();
            Span.Clear();
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
}
