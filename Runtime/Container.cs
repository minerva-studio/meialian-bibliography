using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Amlos.Container
{
    /// <summary>
    /// A fixed-layout byte container defined by a Schema.
    /// Backing buffer is rented from the per-Schema FixedBytePool and returned on Dispose().
    /// </summary>
    internal sealed partial class Container : IDisposable
    {
        private Schema _schema;
        private byte[] _buffer;        // exact size == _schema.Stride (or Array.Empty for 0)
        private bool _disposed;

        public Schema Schema => _schema;

        /// <summary>Logical length in bytes (== Schema.Stride).</summary>
        public int Length => Schema.Stride;

        /// <summary>Logical memory slice [0..Stride).</summary>
        public Span<byte> Span { get { EnsureNotDisposed(); return span; } }

        /// <summary>Data slice [DataBase..Stride).</summary>
        public Span<byte> DataSegment { get { EnsureNotDisposed(); return span[_schema.DataBase..]; } }

        /// <summary>Per-container 1B-per-field type hints stored at the header.</summary>
        public Span<byte> HeaderSegment => span[.._schema.HeaderSize];

        /// <summary> Shortcut </summary>
        private Span<byte> span => _buffer;

        private Header Header => new(HeaderSegment);




        /// <summary>
        /// Create a container 
        /// </summary> 
        private Container(Schema schema, bool zeroInit = true)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));

            if (schema.Stride == 0)
            {
                // Zero-length buffers still get a valid object id and registry entry.
                _buffer = Array.Empty<byte>();
            }
            else
            {
                _buffer = _schema.Pool.Rent(zeroInit);
            }

            for (int i = 0; i < schema.Fields.Count; i++)
            {
                var field = schema.Fields[i];
                if (field.IsRef)
                    SetRefType(i, field.RefCount > 1);
            }
        }

        /// <summary>
        /// Create a container; rent from schema's pool
        /// </summary>
        private Container(Schema schema) : this(schema, zeroInit: true) { }

        /// <summary>
        /// Create a container initialized with provided bytes (length must equal stride).
        /// </summary>
        private Container(Schema schema, ReadOnlySpan<byte> source)
            : this(schema, zeroInit: false)
        {
            if (source.Length != Length)
                throw new ArgumentException($"Source length {source.Length} must equal schema stride {Length}.", nameof(source));
            source.CopyTo(span);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (_buffer.Length != 0 && !ReferenceEquals(_buffer, Array.Empty<byte>()))
            {
                // Clear only the logical slice (avoid clearing entire array for perf)
                _schema.Pool.Return(_buffer);
            }

            _buffer = Array.Empty<byte>();
        }

        private void EnsureNotDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(Container));
        }



        #region Byte-span accessors

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> GetSpan(int offset, int length) => span.Slice(offset, length);

        /// <summary>Returns a writable span for the given field.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> GetSpan(int fieldIndex) => GetSpan(Schema.Fields[fieldIndex]);

        /// <summary>Returns a writable span for the given field.</summary>
        public Span<byte> GetSpan(in FieldDescriptor field)
        {
            EnsureNotDisposed();
            return GetSpan(field.Offset, field.AbsLength);
        }

        /// <summary>Returns a read-only span for the given field.</summary>
        public ReadOnlySpan<byte> GetReadOnlySpan(FieldDescriptor field) => GetSpan(field);

        /// <summary>Returns a read-only span for the given field.</summary>
        public Span<T> GetSpan<T>(FieldDescriptor field) where T : unmanaged => MemoryMarshal.Cast<byte, T>(GetSpan(field));

        /// <summary>Returns a read-only span for the given field.</summary>
        public ReadOnlySpan<T> GetReadOnlySpan<T>(FieldDescriptor field) where T : unmanaged => GetSpan<T>(field);

        public Span<byte> GetSpan(string fieldName) => GetSpan(_schema.GetField(fieldName));

        public ReadOnlySpan<byte> GetReadOnlySpan(string fieldName) => GetReadOnlySpan(_schema.GetField(fieldName));

        /// <summary>Returns a read-only span for the given field.</summary>
        public Span<T> GetSpan<T>(string fieldName) where T : unmanaged => MemoryMarshal.Cast<byte, T>(GetSpan(fieldName));

        /// <summary>Returns a read-only span for the given field.</summary>
        public ReadOnlySpan<T> GetReadOnlySpan<T>(string fieldName) where T : unmanaged => GetSpan<T>(fieldName);
        #endregion


        #region Raw byte read/write by name

        public void WriteBytes(string fieldName, ReadOnlySpan<byte> src)
        {
            var f = _schema.GetField(fieldName);
            if (src.Length != f.AbsLength)
                throw new ArgumentException($"Source length {src.Length} must equal field length {f.Length}.", nameof(src));
            src.CopyTo(GetSpan(f));
        }

        public void ReadBytes(string fieldName, Span<byte> dst)
        {
            var f = _schema.GetField(fieldName);
            if (dst.Length != f.AbsLength)
                throw new ArgumentException($"Destination length {dst.Length} must equal field length {f.Length}.", nameof(dst));
            GetReadOnlySpan(f).CopyTo(dst);
        }

        public bool TryWriteBytes(string fieldName, ReadOnlySpan<byte> src)
        {
            if (!_schema.TryGetField(fieldName, out var f)) return false;
            if (src.Length != f.AbsLength) return false;
            src.CopyTo(GetSpan(f));
            return true;
        }

        public bool TryReadBytes(string fieldName, Span<byte> dst)
        {
            if (!_schema.TryGetField(fieldName, out var f)) return false;
            if (dst.Length != f.AbsLength) return false;
            GetReadOnlySpan(f).CopyTo(dst);
            return true;
        }

        #endregion


        #region Blittable T read/write (unmanaged) 

        public void Write<T>(string fieldName, in T value) where T : unmanaged
        {
            FieldDescriptor f = GetFieldDescriptorOrRescheme<T>(fieldName);
            WriteNoRescheme(f, value);
        }

        public void WriteNoRescheme<T>(string fieldName, in T value) where T : unmanaged => WriteNoRescheme(_schema.GetField(fieldName), value);

        public void WriteNoRescheme<T>(in FieldDescriptor f, T value) where T : unmanaged
        {
            int sz = Unsafe.SizeOf<T>();
            if (sz > f.Length)
                throw new ArgumentException($"Type {typeof(T).Name} size {sz} exceeds field length {f.Length}.", nameof(value));

            WriteNoRescheme_Internal(f, value, sz);
        }

        public bool TryWrite<T>(string fieldName, in T value) where T : unmanaged
        {
            if (!_schema.TryGetField(fieldName, out var f)) return false;
            return TryWrite(f, value);
        }

        public bool TryWrite<T>(in FieldDescriptor field, in T value) where T : unmanaged
        {
            int sz = Unsafe.SizeOf<T>();
            if (sz > field.Length) return false;
            WriteNoRescheme_Internal(field, value, sz);
            return true;
        }

        private void WriteNoRescheme_Internal<T>(FieldDescriptor f, T value, int sz) where T : unmanaged
        {
            var span = GetSpan(in f);
            if (sz < f.Length) span.Clear(); // avoid stale trailing bytes
            MemoryMarshal.Write(span, ref Unsafe.AsRef(value));

            int idx = _schema.IndexOf(f.Name);
            if (idx >= 0) SetScalarType<T>(idx);
        }










        public T Read<T>(string fieldName) where T : unmanaged
        {
            EnsureNotDisposed();
            EnsureFieldForRead<T>(fieldName);

            var f = _schema.GetField(fieldName);
            return Read_Unsafe<T>(f);
        }

        public bool TryRead<T>(string fieldName, out T value) where T : unmanaged
        {
            EnsureNotDisposed();

            value = default;
            if (!_schema.TryGetField(fieldName, out var f)) return false;

            EnsureFieldForRead<T>(fieldName);

            int sz = Unsafe.SizeOf<T>();
            if (sz > f.Length) return false;
            value = Read_Unsafe<T>(f);
            return true;
        }

        public T Read_Unsafe<T>(string fieldName) where T : unmanaged
        {
            var f = _schema.GetField(fieldName);
            return Read_Unsafe<T>(f);
        }

        private T Read_Unsafe<T>(FieldDescriptor f) where T : unmanaged
        {
            return MemoryMarshal.Read<T>(GetReadOnlySpan(f));
        }


        internal ValueView GetValueView(string fieldName)
        {
            int index = _schema.IndexOf(fieldName);
            return new ValueView(GetSpan(index), Header[index].Type);
        }

        #endregion


        #region Object

        public ref ulong GetRef(string fieldName)
        {
            var f = GetFieldDescriptorOrRescheme<ulong>(fieldName);
            return ref GetRef(f);
        }

        public ref ulong GetRefNoRescheme(string fieldName) => ref GetRef(_schema.GetField(fieldName));

        public ref ulong GetRef(FieldDescriptor f)
        {
            if (!f.IsRef || f.RefCount != 1)
                throw new ArgumentException($"Field '{f.Name}' is not a single ref slot.");
            var span = GetSpan(f.Offset, FieldDescriptor.REF_SIZE);
            return ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, ulong>(span));
        }

        public Span<ulong> GetRefSpan(string fieldName) => GetRefSpan(_schema.GetField(fieldName));

        public Span<ulong> GetRefSpan(FieldDescriptor f)
        {
            if (!f.IsRef) throw new ArgumentException($"Field '{f.Name}' is not a ref field.");
            if (f.AbsLength % FieldDescriptor.REF_SIZE != 0)
                throw new ArgumentException($"Field '{f.Name}' byte length is not multiple of {FieldDescriptor.REF_SIZE}.");
            return MemoryMarshal.Cast<byte, ulong>(GetSpan(f));
        }

        public void WriteObject(string fieldName, Container container)
        {
            GetRefNoRescheme(fieldName) = container.ID;
        }

        #endregion



        #region Type Hint

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ref byte HintRef(int fieldIndex) => ref HeaderSegment[fieldIndex];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetScalarType<T>(int fieldIndex) where T : unmanaged
            => HintRef(fieldIndex) = TypeUtil.Pack(TypeUtil.PrimOf<T>(), isArray: false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetArrayType<T>(int fieldIndex) where T : unmanaged
            => HintRef(fieldIndex) = TypeUtil.Pack(TypeUtil.PrimOf<T>(), isArray: true);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetRefType(int fieldIndex, bool isArray)
            => HintRef(fieldIndex) = TypeUtil.Pack(ValueType.Ref, isArray);

        #endregion


        #region Whole-record helpers

        public void Clear()
        {
            EnsureNotDisposed();
            span.Clear();
        }

        public Container Clone()
        {
            EnsureNotDisposed();
            var c = new Container(_schema, zeroInit: false);
            span.CopyTo(c.span);
            return c;
        }

        public void CopyFrom(Container other)
        {
            EnsureNotDisposed();
            if (other is null) throw new ArgumentNullException(nameof(other));
            if (!ReferenceEquals(_schema, other._schema) && !_schema.Equals(other._schema))
                throw new ArgumentException("Schema mismatch. Copy requires identical schema.", nameof(other));

            other.span.CopyTo(span);
        }

        public void CopyTo(Span<byte> destination)
        {
            EnsureNotDisposed();
            if (destination.Length != Length)
                throw new ArgumentException($"Destination length {destination.Length} must equal {Length}.", nameof(destination));
            span.CopyTo(destination);
        }

        #endregion


        #region Create

        public static Container CreateAt(ref ulong position, Schema schema)
        {
            if (schema is null) throw new ArgumentNullException(nameof(schema));

            // 1) If an old tracked container exists in the slot, unregister it first.
            var old = Registry.Shared.GetContainer(position);
            if (old != null)
                Registry.Shared.Unregister(old);

            // 2) Create a new container and register it (assign a unique tracked ID).
            var created = new Container(schema);
            Registry.Shared.Register(created);

            // 3) Bind atomically: write ID into the slot.
            position = created.ID;
            return created;
        }

        /// <summary>
        /// Create a wild container, which means that container is not tracked by anything
        /// </summary>
        /// <param name="position"></param>
        /// <param name="schema"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public static Container CreateAt(ref ulong position, Schema schema, ReadOnlySpan<byte> span)
        {
            if (schema is null) throw new ArgumentNullException(nameof(schema));

            // 1) If an old tracked container exists in the slot, unregister it first.
            var old = Registry.Shared.GetContainer(position);
            if (old != null)
                Registry.Shared.Unregister(old);

            // 2) Create a new container and register it (assign a unique tracked ID).
            var created = new Container(schema, span);
            Registry.Shared.Register(created);

            // 3) Bind atomically: write ID into the slot.
            position = created.ID;
            return created;
        }

        /// <summary>
        /// Create a wild container, which means that container is not tracked by anything
        /// </summary>
        /// <param name="position"></param>
        /// <param name="schema"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public static Container CreateWild(Schema schema)
        {
            if (schema is null) throw new ArgumentNullException(nameof(schema));
            Container newContainer = new(schema);
            newContainer._id = ulong.MaxValue;
            return newContainer;
        }

        /// <summary>
        /// Create a wild container, which means that container is not tracked by anything
        /// </summary>
        /// <param name="position"></param>
        /// <param name="schema"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public static Container CreateWild(Schema schema, ReadOnlySpan<byte> span)
        {
            if (schema is null) throw new ArgumentNullException(nameof(schema));
            Container newContainer = new(schema, span);
            newContainer._id = ulong.MaxValue;
            return newContainer;
        }

        #endregion

    }
}