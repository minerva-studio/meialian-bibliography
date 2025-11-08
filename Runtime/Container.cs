using System;
using System.Buffers;
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



        private byte[] _buffer;         // exact size == _schema.Stride (or Array.Empty for 0) 
        private bool _disposed;
        private ulong _id;              // assigned by registry



        /// <summary> object id </summary>
        public ulong ID => _id;

        /// <summary>Logical length in bytes (== Schema.Stride).</summary>
        public unsafe ref ContainerHeader Header => ref *(ContainerHeader*)Unsafe.AsPointer(ref _buffer[0]);

        /// <summary>Logical length in bytes (== Schema.Stride).</summary>
        public unsafe ref int Length => ref *(int*)Unsafe.AsPointer(ref _buffer[0]);

        public int FieldCount => View.FieldCount;

        public ContainerView View => new ContainerView(_buffer);

        /// <summary>Logical memory slice [0..length).</summary>
        public Span<byte> Span => View.Span;

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

            _disposed = false;
            _buffer = DefaultPool.Rent((int)size);
            ContainerHeader.WriteLength(_buffer, size);
        }

        private void Initialize(byte[] buffer)
        {
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

        private void EnsureNotDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(Container));
        }



        public int IndexOf(ReadOnlySpan<char> fieldName)
        {
            EnsureNotDisposed();
            return View.IndexOf(fieldName);
        }

        private unsafe ref FieldHeader GetFieldHeader(int index)
        {
            ref readonly var header = ref this.Header;
            return ref *(FieldHeader*)Unsafe.AsPointer(ref _buffer[ContainerHeader.Size + index * FieldHeader.Size]);
        }

        private Span<byte> GetFieldData(int index)
        {
            ref FieldHeader header = ref GetFieldHeader(index);
            return MemoryMarshal.CreateSpan(ref _buffer[header.DataOffset], header.Length);
        }



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
        public void Write<T>(string fieldName, in T value, bool allowRescheme = true) where T : unmanaged
        {
            EnsureNotDisposed();

            // nonexist
            var index = IndexOf(fieldName);
            if (index < 0)
            {
                if (allowRescheme)
                    index = GetFieldIndexOrRescheme<T>(fieldName);
                else
                    throw new ArgumentException($"{fieldName} does not exist in object.", nameof(value));
            }
            Write_Internal(index, value, allowRescheme);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWrite<T>(string fieldName, in T value, bool allowRescheme = true) where T : unmanaged
        {
            // nonexist
            var index = IndexOf(fieldName);
            if (index < 0) return false;
            return TryWrite_Internal(index, value, allowRescheme) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write_Internal<T>(int index, T value, bool allowRescheme = true) where T : unmanaged
        {
            switch (TryWrite_Internal(index, value, allowRescheme))
            {
                case 0:
                    return;
                case 1:
                    throw new ArgumentException($"Type {typeof(T).Name} cannot cast to {View[index].Type}.", nameof(value));
                case 2:
                    throw new ArgumentException($"Type {typeof(T).Name} exceeds field length and cannot write into {View.GetFieldName(index).ToString()} without rescheme.", nameof(value));
                default:
                    throw new ArgumentException($"Type {typeof(T).Name} cannot write to {View[index].Type}.", nameof(value));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int TryWrite_Internal<T>(int index, T value, bool allowRescheme = true) where T : unmanaged
        {
            if (TryWriteScalarImplicit(index, value))
                return 0;

            int sz = Unsafe.SizeOf<T>();
            var f = View.Fields[index];
            // same size, override 
            if (f.Length == sz)
            {
                Write_Override(index, value);
                return 0;
            }
            // too small? rescheme
            if (f.Length < sz)
            {
                if (!allowRescheme) return 2;
                // currently can't contain the value, rescheme
                ReschemeAndWrite(index, value);
                return 0;
            }
            // too large? explicit cast
            else
            {
                return TryWriteScalarExplicit(index, value) ? 0 : 1;
            }
        }
        private void ReschemeAndWrite<T>(int index, T value) where T : unmanaged
        {
            var f = View.Fields[index];
            index = ReschemeFor<T>(View.GetFieldName(index));

            // update to new field
            Write_Override(index, value);
        }
        private void Write_Override<T>(int index, T value) where T : unmanaged
        {
            var f = View[index];
            var span = f.Data;
            if (Unsafe.SizeOf<T>() < f.Length) span.Clear(); // avoid stale trailing bytes
            MemoryMarshal.Write(span, ref Unsafe.AsRef(value));
            SetScalarType<T>(index);
        }







        public T Read<T>(string fieldName) where T : unmanaged
        {
            EnsureNotDisposed();

            int index = GetFieldIndexOrRescheme<T>(fieldName);

            if (TryReadScalarExplicit(index, out T result))
                return result;

            throw new InvalidOperationException();
        }

        public bool TryRead<T>(string fieldName, out T value) where T : unmanaged
        {
            EnsureNotDisposed();

            int index = IndexOf(fieldName);
            if (index < 0)
            {
                value = default;
                return false;
            }

            if (TryReadScalarExplicit(index, out value))
                return true;

            return false;
        }

        public T Read_Unsafe<T>(string fieldName) where T : unmanaged
        {
            return Read_Unsafe<T>(IndexOf(fieldName));
        }

        private T Read_Unsafe<T>(int index) where T : unmanaged
        {
            return MemoryMarshal.Read<T>(View.GetFieldBytes(index));
        }


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
}