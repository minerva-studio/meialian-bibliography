using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Stack-only view of a container within a Storage tree.
    /// Cannot be persisted; exposes only read/write and navigation helpers.
    /// </summary>
    public readonly struct StorageObject : IEquatable<StorageObject>
    {
        private const char DefaultPathSeparator = '.';

        internal readonly Container _container;
        internal readonly int _generation;


        /// <summary>
        /// Object ID   
        /// </summary>
        public ulong ID
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => IsNull ? Container.Registry.ID.Empty : _container.ID;
        }

        /// <summary>
        /// Is object null
        /// </summary>
        public bool IsNull
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _container == null || _container.ID == Container.Registry.ID.Empty || IsDisposed;
        }

        /// <summary>
        /// True if this StorageObject represents a single string field, then this container is really just a string.
        /// </summary>
        public bool IsString
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => IsArray() && _container.GetFieldHeader(0).Type == ValueType.Char16;
        }

        /// <summary>
        /// Is the storage object already disposed
        /// </summary>
        public bool IsDisposed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _container.IsDisposed(_generation);
        }

        public int FieldCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Container.FieldCount;
        }

        /// <summary>
        /// DO NOT EXPOSE THIS PROPERTY OUTSIDE THE ASSEMBLY, INTERNAL USE ONLY (AND FOR DEBUGGING ONLY)
        /// </summary>
        internal readonly ref AllocatedMemory Memory
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref Container.Memory;
        }

        /// <summary>
        /// Validated container reference, or throws if disposed.
        /// </summary>
        internal Container Container => _container.EnsureNotDisposed(_generation);

        public StorageMember this[ReadOnlySpan<char> path]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => GetMember(path);
        }




        internal StorageObject(Container container)
        {
            _container = container ?? throw new InvalidOperationException();
            _generation = container.Generation;
        }



        /// <summary>
        /// Ensure container is not disposed
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EnsureNotDisposed() => _container.EnsureNotDisposed(_generation);




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal FieldView GetFieldView(ReadOnlySpan<char> fieldName) => _container.View[fieldName];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal FieldView GetFieldView(int index) => _container.View[index];





        // Basic read/write passthroughs (blittable)

        #region Simple Read/Write

        #region Write
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(string fieldName, string value)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            ThrowHelper.ThrowIfNull(value, nameof(value));
            WriteString(fieldName.AsSpan(), value.AsSpan());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(ReadOnlySpan<char> fieldName, ReadOnlySpan<char> value) => WriteString(fieldName, value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(int index, ReadOnlySpan<char> value) => WriteString(index, value);




        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>   
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(int index, T value) where T : unmanaged
        {
            var container = _container.EnsureNotDisposed(_generation);
            container.Write_Internal(ref container.GetFieldHeader(index), value, true);
            NotifyFieldWrite(container, index);
        }

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>   
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(int index, T value, bool allowResize = true) where T : unmanaged
        {
            var container = _container.EnsureNotDisposed(_generation);
            container.Write_Internal(ref container.GetFieldHeader(index), value, allowResize);
            NotifyFieldWrite(container, index);
        }

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>
        /// <remarks>
        /// - If implicit conversion is possible, write the converted code without changing the type.
        /// - If implicit conversion is not possible:
        /// <ul> 
        /// <li> If the current size is the same, change the type. </li>
        /// <li> If is too small, rescheme. </li>
        /// <li> If is too large, explicit conversion. </li>
        /// </ul>
        /// </remarks>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(string fieldName, T value) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            Write(fieldName.AsSpan(), value);
        }

        /// <summary>
        /// Write a value to an existing field without rescheming, if the field does not exist, an exception is thrown.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(ReadOnlySpan<char> fieldName, T value, bool allowRescheme = true) where T : unmanaged
        {
            var container = _container.EnsureNotDisposed(_generation);
            container.Write_Internal(ref container.GetFieldHeader<T>(fieldName, allowRescheme), value, allowRescheme);
            NotifyFieldWrite(container, fieldName);
        }

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWrite<T>(string fieldName, T value) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return TryWrite(fieldName.AsSpan(), value);
        }

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWrite<T>(ReadOnlySpan<char> fieldName, T value, bool allowRescheme = true) where T : unmanaged
        {
            var container = _container.EnsureNotDisposed(_generation);
            if (container.TryWrite_Internal(ref container.GetFieldHeader<T>(fieldName, allowRescheme), value, allowRescheme) == 0)
            {
                NotifyFieldWrite(container, fieldName);
                return true;
            }
            return false;
        }

        #endregion

        #region Override

        /// <summary> 
        /// Override existing data with given bytes
        /// </summary>
        /// <remarks>
        /// Try avoid using this method, use typed Write/Override methods instead.
        /// </remarks>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Override<T>(string fieldName, T value) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            Override(fieldName.AsSpan(), MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref value, 1)), TypeUtil<T>.ValueType);
        }

        /// <summary> 
        /// Override existing data with given bytes
        /// </summary>
        /// <remarks>
        /// Try avoid using this method, use typed Write/Override methods instead.
        /// </remarks>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Override<T>(ReadOnlySpan<char> fieldName, T value) where T : unmanaged => Override(fieldName, MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref value, 1)), TypeUtil<T>.ValueType);

        /// <summary> 
        /// Override existing data with given bytes
        /// </summary>
        /// <remarks>
        /// Try avoid using this method, use typed Write/Override methods instead.
        /// </remarks>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Override<T>(string fieldName, ReadOnlySpan<T> value) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            Override(fieldName.AsSpan(), MemoryMarshal.AsBytes(value), TypeUtil<T>.ValueType, value.Length);
        }

        /// <summary> 
        /// Override existing data with given bytes
        /// </summary>
        /// <remarks>
        /// Try avoid using this method, use typed Write/Override methods instead.
        /// </remarks>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Override<T>(ReadOnlySpan<char> fieldName, ReadOnlySpan<T> value) where T : unmanaged => Override(fieldName, MemoryMarshal.AsBytes(value), TypeUtil<T>.ValueType, value.Length);

        /// <summary> 
        /// Override existing data with given bytes
        /// </summary>
        /// <remarks>
        /// Try avoid using this method, use typed Write/Override methods instead.
        /// </remarks>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        /// <param name="valueType"></param>
        /// <param name="inlineArrayLength"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Override<T>(string fieldName, ReadOnlySpan<byte> value, ValueType valueType, int? inlineArrayLength = null)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            Override(fieldName.AsSpan(), value, valueType, inlineArrayLength);
        }

        /// <summary>
        /// Override existing data with given bytes
        /// </summary>
        /// <remarks>
        /// Try avoid using this method, use typed Write/Override methods instead.
        /// </remarks>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        /// <param name="valueType"></param>
        /// <param name="inlineArrayLength"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Override(ReadOnlySpan<char> fieldName, ReadOnlySpan<byte> value, ValueType valueType, int? inlineArrayLength = null)
        {
            if (valueType == ValueType.Ref)
                ThrowHelper.ArgumentException(nameof(value));

            var container = _container.EnsureNotDisposed(_generation);
            _container.Override(fieldName, value, valueType, inlineArrayLength);
            NotifyFieldWrite(container, fieldName);
        }

        #endregion

        #region Read


        /// <summary>
        /// Read the field
        /// </summary>
        /// <remarks>
        /// - If field does not exist, read <typeparamref name="T"/> default value and create the field
        /// - Always return explicit conversion result, unless conversion is not supported, then exception will throw
        /// </remarks>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>(string fieldName) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return Read<T>(fieldName.AsSpan());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>(ReadOnlySpan<char> fieldName) where T : unmanaged
        {
            if (_container.TryReadScalarExplicit(ref _container.EnsureNotDisposed(_generation).GetFieldHeader<T>(fieldName, true), out T result))
                return result;

            ThrowHelper.ThrowInvalidOperation();
            return default;
        }

        /// <summary>
        /// Read the field
        /// </summary>
        /// <remarks>
        /// - If field does not exist, read <typeparamref name="T"/> default value and create the field
        /// - Always return explicit conversion result, unless conversion is not supported, then exception will throw
        /// </remarks>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>(int index) where T : unmanaged
        {
            if (_container.TryReadScalarExplicit(ref _container.EnsureNotDisposed(_generation).GetFieldHeader(index), out T result))
                return result;

            ThrowHelper.ThrowInvalidOperation();
            return default;
        }

        /// <summary>
        /// Try Read the field
        /// </summary>
        /// <remarks>
        /// - If field does not exist, return false and will not create the field
        /// - Always return explicit conversion result, unless conversion is not supported, return false
        /// - return true with explicitly casted value, false otherwise
        /// </remarks>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRead<T>(string fieldName, out T value) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return TryRead(fieldName.AsSpan(), out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRead<T>(ReadOnlySpan<char> fieldName, out T value) where T : unmanaged
        {
            value = default;
            return _container.EnsureNotDisposed(_generation).TryGetFieldHeader(fieldName, out var outHeader) && _container.TryReadScalarExplicit(ref outHeader[0], out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T ReadOrDefault<T>(string fieldName) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return ReadOrDefault<T>(fieldName.AsSpan());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T ReadOrDefault<T>(ReadOnlySpan<char> fieldName1) where T : unmanaged
        {
            return _container.EnsureNotDisposed(_generation).TryRead(fieldName1, out T value) ? value : default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T ReadOrDefault<T>(string fieldName, T defaultValue) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return ReadOrDefault(fieldName.AsSpan(), defaultValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T ReadOrDefault<T>(ReadOnlySpan<char> fieldName, T defaultValue) where T : unmanaged => _container.EnsureNotDisposed(_generation).TryRead(fieldName, out T value) ? value : defaultValue;

        /// <summary>
        /// Read data regardless actual stored type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read_Unsafe<T>(string fieldName) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return _container.Read_Unsafe<T>(fieldName);
        }

        /// <summary>
        /// Read data regardless actual stored type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read_Unsafe<T>(ReadOnlySpan<char> fieldName) where T : unmanaged => _container.Read_Unsafe<T>(fieldName);

        #endregion

        #endregion

        #region Read/Write by Path

        /// <summary>
        /// Write a scalar value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePath<T>(string path, T value) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            WritePath(path.AsSpan(), value);
        }

        /// <summary>
        /// Write a scalar value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        public void WritePath<T>(ReadOnlySpan<char> path, T value, char separator = DefaultPathSeparator) where T : unmanaged
        {
            var obj = NavigateToObject(path, separator, createIfMissing: true, out var fieldSegment, out var index);
            if (fieldSegment.Length == 0)
                ThrowHelper.ArgumentException("Path must contain at least one segment.", nameof(path));

            if (index >= 0) obj.GetArray(fieldSegment).Write(index, value);
            else obj.Write(fieldSegment, value);
        }

        /// <summary>
        /// Read a scalar field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T ReadPath<T>(string path) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            return ReadPath<T>(path.AsSpan());
        }

        /// <summary>
        /// Read a scalar field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        public T ReadPath<T>(ReadOnlySpan<char> path, char separator = DefaultPathSeparator) where T : unmanaged
        {
            var obj = NavigateToObject(path, separator, createIfMissing: false, out var fieldSegment, out var index);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            if (index >= 0)
                return obj.GetArray(fieldSegment).Read<T>(index);

            if (!obj.TryRead(fieldSegment, out T value))
                throw new InvalidOperationException($"Field '{fieldSegment.ToString()}' not found for path '{path.ToString()}'.");

            return value;
        }

        /// <summary>
        /// Try to read a scalar field by path without allocating intermediate
        /// objects. Returns false if any segment or field is missing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryReadPath<T>(string path, out T value) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            return TryReadPath(path.AsSpan(), out value);
        }

        /// <summary>
        /// Try to read a scalar field by path without allocating intermediate
        /// objects. Returns false if any segment or field is missing.
        /// </summary>
        public bool TryReadPath<T>(ReadOnlySpan<char> path, out T value, char separator = DefaultPathSeparator) where T : unmanaged
        {
            value = default;
            try
            {
                var container = NavigateToObject(path, separator, createIfMissing: false, out var fieldSegment, out var index);
                if (container.IsNull || fieldSegment.Length == 0)
                    return false;
                if (index >= 0)
                {
                    return container.GetArray(fieldSegment).TryRead<T>(index, out value);
                }
                return container.TryRead(fieldSegment, out value);
            }
            catch (ArgumentException)
            {
                return false;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }

        #endregion

        #region String/Array 

        #region String 
        #region Write

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(int index, ReadOnlySpan<char> value)
        {
            GetObject(index).WriteArray(value);
            NotifyFieldWrite(_container, index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(string fieldName, ReadOnlySpan<char> value)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            WriteString(fieldName.AsSpan(), value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(ReadOnlySpan<char> fieldName, ReadOnlySpan<char> value)
        {
            GetObject(fieldName).WriteArray(value);
            // since write array already notifies, no need to notify again
            //NotifyFieldWrite(_container, fieldName);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(string value)
        {
            ThrowHelper.ThrowIfNull(value, nameof(value));
            WriteArray(value.AsSpan());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(ReadOnlySpan<char> value) => WriteArray(value);


        #endregion



        #region Read

        /// <summary>
        /// Read as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadString(string fieldName)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return ReadString(fieldName.AsSpan());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadString(ReadOnlySpan<char> fieldName) => GetArray(fieldName).ToString();

        /// <summary>
        /// Read entire container as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadString()
        {
            var container = _container.EnsureNotDisposed(_generation);
            if (!IsString)
            {
                if (FieldCount == 0)
                    throw new InvalidOperationException($"This StorageObject does not represent a single string field because object is empty.");
                if (FieldCount == 1)
                    throw new InvalidOperationException($"This StorageObject does not represent a single string field because the field field is of type {_container.GetFieldHeader(0).FieldType}.");
                throw new InvalidOperationException($"This StorageObject does not represent a single string field because the field count is {FieldCount}.");
            }

            return StorageArray.AsString(in container.GetFieldHeader(0), container);
        }

        #endregion
        #endregion

        #region Array

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArray<T>(string fieldName, ReadOnlySpan<T> value) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            WriteArray(fieldName.AsSpan(), value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArray<T>(ReadOnlySpan<char> fieldName, ReadOnlySpan<T> value) where T : unmanaged
        {
            var container = _container.EnsureNotDisposed(_generation);
            GetObject(fieldName).WriteArray(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArray<T>(ReadOnlySpan<T> value) where T : unmanaged
        {
            if (!IsArray())
            {
                if (FieldCount != 0)
                    throw new InvalidOperationException("This StorageObject does not represent an array.");
            }

            MakeArray<T>(value.Length);
            ref FieldHeader header = ref _container.GetFieldHeader(0);
            MemoryMarshal.AsBytes(value).CopyTo(_container.GetFieldData(in header));
            NotifyFieldWrite(_container, _container.GetFieldName(in header));
        }

        /// <summary>
        /// Read an array from a child field.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T[] ReadArray<T>(string fieldName) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return ReadArray<T>(fieldName.AsSpan());
        }

        public T[] ReadArray<T>(ReadOnlySpan<char> fieldName) where T : unmanaged => GetArray(fieldName).ToArray<T>();

        /// <summary>
        /// Read entire container as an array of unmanaged type T.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T[] ReadArray<T>() where T : unmanaged
        {
            if (!IsArray())
                throw new InvalidOperationException("This StorageObject does not represent an array.");

            return StorageArray.ToArray<T>(in _container.GetFieldHeader(0), _container);
        }



        /// <summary>
        /// Make this field an array
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="length"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MakeArray<T>(int length) where T : unmanaged
        {
            // scalar array fast path
            _container.ReschemeForArray(length, TypeUtil<T>.Type);
        }

        /// <summary>
        /// Make this field an array
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="length"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MakeArray(TypeData type, int length)
        {
            _container.ReschemeForArray(length, type);
        }

        /// <summary>
        /// Ensure array capacity
        /// </summary>
        /// <param name="capacity"></param>
        internal void EnsureArrayCapacity(int capacity)
        {
            // already ensure this is either empty object or array
            if (FieldCount == 0)
            {
                MakeArray(TypeData.Ref, capacity);
                return;
            }
            ref var header = ref _container.GetFieldHeader(0);
            var arrayLength = header.ElementCount;
            // satified
            if (arrayLength >= capacity)
                return;
            // resize
            _container.ReschemeForArray(capacity, header.ElementType);
        }





        public bool IsArray(string fieldName)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return IsArray(fieldName.AsSpan());
        }

        public bool IsArray(ReadOnlySpan<char> fieldName)
        {
            _container.EnsureNotDisposed(_generation);
            if (!_container.TryGetFieldHeader(fieldName, out var headerSpan))
                return false;

            ref var header = ref headerSpan[0];
            // inline
            if (header.IsInlineArray)
                return true;
            // ref
            if (header.IsRef)
                return GetObject(in header, null).Container.IsArray;
            return false;
        }

        internal bool IsArray(int index)
        {
            _container.EnsureNotDisposed(_generation);
            if (index < 0 || index > FieldCount)
                return false;

            ref var header = ref _container.GetFieldHeader(index);
            // inline
            if (header.IsInlineArray)
                return true;
            // ref
            if (header.IsRef)
                return GetObject(in header, null).Container.IsArray;
            return false;
        }

        /// <summary>
        /// Is an array object
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool IsArray() => Container.IsArray;

        #endregion




        /// <summary>
        /// Make storage an array
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray AsArray() => new(_container.EnsureNotDisposed(_generation));




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray GetArray(string fieldName)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return GetArray(fieldName.AsSpan());
        }

        /// <summary>
        /// Get a stack-only view over a value array field.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray GetArray(ReadOnlySpan<char> fieldName)
        {
            _container.EnsureNotDisposed(_generation);
            ref var header = ref _container.GetFieldHeader(fieldName);
            // inline array
            if (header.IsInlineArray)
                return new(_container, fieldName);

            // obj array
            var obj = GetObject(in header, null);
            if (!obj.IsNull && obj.IsArray())
                return new(obj.Container);

            throw new InvalidOperationException($"Field {fieldName.ToString()} is not an array");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal StorageArray GetArray(ref FieldHandle fieldIndex)
        {
            ref var header = ref _container.GetFieldHeader(fieldIndex.Index);
            // inline array
            if (header.IsInlineArray)
                return new(fieldIndex);

            // obj array
            var obj = GetObject(in header, null);
            if (!obj.IsNull && obj.IsArray())
                return new(obj.Container);

            throw new InvalidOperationException($"Field is not an array");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray GetArray(ReadOnlySpan<char> fieldSegment, TypeData? type = null, bool createIfMissing = true, bool reschemeOnTypeMismatch = true, bool overrideExisting = false)
        {
            _container.EnsureNotDisposed(_generation);
            return GetArray_Internal(fieldSegment, type, createIfMissing, reschemeOnTypeMismatch, overrideExisting);
        }

        private StorageArray GetArray_Internal(ReadOnlySpan<char> fieldSegment, TypeData? type, bool createIfMissing, bool reschemeOnTypeMismatch, bool overrideExisting)
        {
            if (TryGetArray_Internal(fieldSegment, null, out var storageArray))
            {
                if (type.HasValue && !storageArray.IsConvertibleTo(type.Value))
                {
                    if (reschemeOnTypeMismatch || overrideExisting)
                        storageArray.Rescheme(type.Value);
                    else ThrowHelper.ArgumentException("Array type mismatch");
                }
                return storageArray;
            }

            if (!type.HasValue)
                ThrowHelper.ArgumentNull(nameof(type));

            if (!_container.TryGetFieldHeader(fieldSegment, out var outHeader))
            {
                if (!createIfMissing)
                    ThrowHelper.ThrowInvalidOperation();

                var holder = GetObject(fieldSegment);
                holder.MakeArray(type.Value, 0);
                return holder.AsArray();
            }
            // found but not array
            else
            {
                if (!overrideExisting)
                    ThrowHelper.ArgumentException("Field exists but is not an array.");

                FieldHeader fieldHeader = outHeader[0];
                StorageObject holder;
                bool reschemeForField = !fieldHeader.IsRef;
                if (reschemeForField)
                {
                    _container.ReschemeFor(fieldSegment, TypeData.Ref, null);
                }
                holder = GetObject(fieldSegment);
                holder.MakeArray(type.Value, 0);
                StorageArray storageArr = holder.AsArray();
                if (reschemeForField)
                {
                    NotifyFieldDelete(_container, fieldSegment.ToString(), fieldHeader.FieldType);
                }
                return storageArr;
            }
        }



        /// <summary>
        /// Get a stack-only view over a value array field.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetArray(string fieldName, out StorageArray storageArray)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            _container.EnsureNotDisposed(_generation);
            return TryGetArray(fieldName.AsSpan(), null, out storageArray);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetArray(ReadOnlySpan<char> fieldName, out StorageArray storageArray)
        {
            _container.EnsureNotDisposed(_generation);
            return TryGetArray(fieldName, null, out storageArray);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetArray<T>(ReadOnlySpan<char> fieldSegment, out StorageArray storageArray) where T : unmanaged
        {
            _container.EnsureNotDisposed(_generation);
            return TryGetArray_Internal(fieldSegment, TypeUtil<T>.Type, out storageArray);
        }

        public bool TryGetArray(ReadOnlySpan<char> fieldSegment, TypeData? type, out StorageArray storageArray)
        {
            _container.EnsureNotDisposed(_generation);
            return TryGetArray_Internal(fieldSegment, type, out storageArray);
        }

        private bool TryGetArray_Internal(ReadOnlySpan<char> fieldSegment, TypeData? type, out StorageArray storageArray)
        {
            storageArray = default;
            if (!_container.TryGetFieldHeader(fieldSegment, out var headerSpan))
                return false;
            ref var header = ref headerSpan[0];

            if (header.IsInlineArray)
            {
                if (type.HasValue && !header.ElementType.CanImplicitlyCastTo(type.Value)) return false;
                storageArray = new StorageArray(_container, fieldSegment);
                return true;
            }

            if (!TryGetObject(fieldSegment, out var child) || child.IsNull || !child.IsArray())
                return false;

            if (child.FieldCount > 0)
            {
                ref var childInlineArrayHeader = ref child._container.GetFieldHeader(0);
                if (type.HasValue && !childInlineArrayHeader.ElementType.CanImplicitlyCastTo(type.Value)) return false;
            }

            storageArray = new StorageArray(child.Container);
            return true;
        }



        /// <summary>
        /// Get the array located by a dot-separated path.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="path"></param>
        /// <param name="createIfMissing"></param>
        /// <param name="reschemeOnTypeMismatch">Rescheme the array itself if type mismatch</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray GetArrayByPath<T>(string path, bool createIfMissing = true, bool reschemeOnTypeMismatch = true, bool overrideExisting = false) where T : unmanaged
            => GetArrayByPath(path, TypeUtil<T>.Type, createIfMissing, reschemeOnTypeMismatch, overrideExisting);

        /// <summary>
        /// Get the array located by a dot-separated path.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="path"></param>
        /// <param name="createIfMissing"></param>
        /// <param name="reschemeOnTypeMismatch">Rescheme the array itself if type mismatch</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray GetArrayByPath<T>(ReadOnlySpan<char> path, bool createIfMissing = true, bool reschemeOnTypeMismatch = true, bool overrideExisting = false) where T : unmanaged
            => GetArrayByPath(path, TypeUtil<T>.Type, createIfMissing, reschemeOnTypeMismatch, overrideExisting);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageArray GetArrayByPath(string path, TypeData? type, bool createIfMissing = true, bool reschemeOnTypeMismatch = true, bool overrideExisting = false)
            => GetArrayByPath(path.AsSpan(), type, createIfMissing, reschemeOnTypeMismatch, overrideExisting);

        public StorageArray GetArrayByPath(ReadOnlySpan<char> path, TypeData? type, bool createIfMissing = true, bool reschemeOnTypeMismatch = true, bool overrideExisting = false)
        {
            if (path.Length == 0) ThrowHelper.ArgumentException(nameof(path));

            var parent = NavigateToObject(path, DefaultPathSeparator, createIfMissing, out var fieldSegment, out var _);
            if (fieldSegment.Length == 0) ThrowHelper.ArgumentException(nameof(path));

            return parent.GetArray_Internal(fieldSegment, type, createIfMissing, reschemeOnTypeMismatch, overrideExisting);
        }

        public bool TryGetArrayByPath<T>(ReadOnlySpan<char> path, out StorageArray storageArray) where T : unmanaged
        {
            storageArray = default;
            if (path.Length == 0) return false;

            if (!TryNavigateToObject(path, DefaultPathSeparator, out var parent, out var fieldSegment, out var _))
                return false;
            if (fieldSegment.Length == 0)
                return false;

            // First check header (to detect inline arrays without allocating) 
            return parent.TryGetArray_Internal(fieldSegment, TypeUtil<T>.Type, out storageArray);
        }

        public bool TryGetArrayByPath(ReadOnlySpan<char> path, TypeData? type, out StorageArray storageArray)
        {
            storageArray = default;
            if (path.Length == 0) return false;

            if (!TryNavigateToObject(path, DefaultPathSeparator, out var parent, out var fieldSegment, out var _))
                return false;
            if (fieldSegment.Length == 0)
                return false;
            return parent.TryGetArray_Internal(fieldSegment, type, out storageArray);
        }

        #endregion




        #region String/Array Path

        /// <summary>
        /// Write a string value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WritePath(string path, string value)
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            WritePath(path.AsSpan(), value.AsSpan());
        }

        /// <summary>
        /// Write a string value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        public void WritePath(ReadOnlySpan<char> path, ReadOnlySpan<char> value, char separator = DefaultPathSeparator)
        {
            var obj = NavigateToObject(path, separator, createIfMissing: true, out var fieldSegment, out var index);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            if (index >= 0)
            {
                obj.GetArray(fieldSegment).GetObject(index).WriteString(value);
            }
            obj.WriteString(fieldSegment, value);
        }

        /// <summary>
        /// Write an inline array value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArrayPath<T>(string path, T[] value) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            ThrowHelper.ThrowIfNull(value, nameof(value));
            WriteArrayPath<T>(path.AsSpan(), value.AsSpan());
        }

        /// <summary>
        /// Write an inline array value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        public void WriteArrayPath<T>(ReadOnlySpan<char> path, ReadOnlySpan<T> value, char separator = DefaultPathSeparator) where T : unmanaged
        {
            var container = NavigateToObject(path, separator, createIfMissing: true, out var fieldSegment, out var index);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            if (index >= 0)
            {
                container.GetObjectInArray(fieldSegment, index).WriteArray(value);
            }
            else container.WriteArray(fieldSegment, value);
        }

        /// <summary>
        /// Read a string field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadStringPath(string path)
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            return ReadStringPath(path.AsSpan());
        }

        /// <summary>
        /// Read a string field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        public string ReadStringPath(ReadOnlySpan<char> path, char separator = DefaultPathSeparator)
        {
            var container = NavigateToObject(path, separator, createIfMissing: false, out var fieldSegment, out var index);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            if (!container.HasField(fieldSegment))
                throw new ArgumentException($"Path segment '{fieldSegment.ToString()}' does not exist on the current object.", nameof(path));

            var child = container.GetObject(fieldSegment, reschemeOnMissing: false, layout: null);
            if (child.IsNull)
                throw new InvalidOperationException($"Path segment '{fieldSegment.ToString()}' refers to a null child object.");

            var ts = child.AsArray();
            if (index >= 0) ts = ts.GetObject(index).AsArray();
            return ts.ToString();
        }

        /// <summary>
        /// Read an inline array field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T[] ReadArrayPath<T>(string path) where T : unmanaged
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            return ReadArrayPath<T>(path.AsSpan());
        }

        /// <summary>
        /// Read an inline array field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        public T[] ReadArrayPath<T>(ReadOnlySpan<char> path, char separator = DefaultPathSeparator) where T : unmanaged
        {
            var container = NavigateToObject(path, separator, createIfMissing: false, out var fieldSegment, out var index);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            if (!container.HasField(fieldSegment))
                throw new ArgumentException($"Path segment '{fieldSegment.ToString()}' does not exist on the current object.", nameof(path));

            var child = container.GetObject(fieldSegment, reschemeOnMissing: false, layout: null);
            if (child.IsNull)
                throw new InvalidOperationException($"Path segment '{fieldSegment.ToString()}' refers to a null child object.");

            var ts = child.AsArray();
            if (index >= 0) ts = ts.GetObject(index).AsArray();
            return ts.ToArray<T>();
        }

        #endregion



        #region Subscription methods

        /// <summary>
        /// Subscribe to write notifications for this container (all fields under it).
        /// </summary>
        public StorageSubscription Subscribe(StorageMemberHandler handler)
        {
            ThrowHelper.ThrowIfNull(handler, nameof(handler));
            var container = _container.EnsureNotDisposed(_generation);
            return StorageEventRegistry.SubscribeToContainer(container, handler);
        }

        /// <summary>
        /// Subscribe to a field or child container specified by path segments separated with the default separator.
        /// </summary>
        public StorageSubscription Subscribe(string path, StorageMemberHandler handler, char separator = DefaultPathSeparator)
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            ThrowHelper.ThrowIfNull(handler, nameof(handler));

            if (path.Length == 0)
                return Subscribe(handler);

            // ignore the index for subscription, subscribe to the array field instead
            // e.g. "items[3]" -> subscribe to "items"
            // but it might be to noisy if someone is writing to other index and we only interested in one item
            var container = NavigateToObject(path, separator, createIfMissing: false, out var fieldSegment, out _);
            if (fieldSegment.Length == 0)
                return container.Subscribe(handler);

            return container.SubscribeLocal(fieldSegment, handler);
        }

        /// <summary>
        /// Subscribe to a local field/child without performing path navigation.
        /// </summary>
        private StorageSubscription SubscribeLocal(ReadOnlySpan<char> name, StorageMemberHandler handler)
        {
            if (name.Length == 0)
                return Subscribe(handler);

            var container = _container.EnsureNotDisposed(_generation);
            if (!container.TryGetFieldHeader(name, out var headerSpan))
                throw new ArgumentException($"Field '{name.ToString()}' does not exist on this container.", nameof(name));

            ref var header = ref headerSpan[0];
            if (header.IsRef)
            {
                var child = GetObject(name, reschemeOnMissing: false, layout: null);
                if (child.IsNull)
                    throw new ArgumentException($"Container '{name.ToString()}' is null or missing.", nameof(name));
                return child.Subscribe(handler);
            }

            return StorageEventRegistry.Subscribe(container, name.ToString(), handler);
        }

        #endregion


        #region Navigate

        // ---------------- Segment parsing & forward-only path state machine ----------------
        // A forward-only reader over path segments supporting optional [index] suffix.


        /// <summary>
        /// Navigate to the object owning the final field in the path.
        /// Returns the StorageObject containing the final field, and outputs:
        ///   - fieldSegment: the name of the last segment (without index)
        ///   - index: optional index for an array field (or -1 if no index)
        /// Throws on any syntax error or structural mismatch.
        /// </summary>
        private StorageObject NavigateToObject(ReadOnlySpan<char> path, char separator, bool createIfMissing, out ReadOnlySpan<char> fieldSegment, out int index)
        {
            if (path.Length == 0)
                throw new ArgumentException("Path cannot be empty.", nameof(path));

            var current = this;
            fieldSegment = default;
            index = -1;

            var reader = new PathReader(path, separator);

            while (reader.MoveNext(out var segName, out var segIndex))
            {
                if (!reader.HasNext)
                {
                    if (segName.Length == 0)
                        throw new ArgumentException("Path cannot end with a separator.", nameof(path));

                    fieldSegment = segName;
                    index = segIndex;
                    return current;
                }

                StorageObject next;

                if (segIndex >= 0)
                {
                    // Segment like "Items[3]" -> array field on the current object
                    next = current.GetObjectInArray(segName, segIndex, createIfMissing, createIfMissing); // ensure array and object exist
                }
                else
                {
                    // Plain child object segment like "Player"
                    if (createIfMissing)
                        next = current.GetObject(segName);
                    else if (!current.TryGetObject(segName, out next))
                        throw new ArgumentException($"Path segment '{segName.ToString()}' does not exist on the current object.", nameof(path));
                }

                if (next.IsNull)
                    throw new InvalidOperationException($"Path segment '{segName.ToString()}' refers to a null child object.");

                current = next;
            }

            throw new ArgumentException($"Cannot resovle Path '{path.ToString()}'.");
        }

        /// <summary>
        /// Non-throwing version of NavigateToObject.
        /// Returns false on any syntax error, missing object, or invalid array access.
        /// </summary>
        private bool TryNavigateToObject(ReadOnlySpan<char> path, char separator, out StorageObject storageObject, out ReadOnlySpan<char> fieldSegment, out int index)
        {
            storageObject = default;
            fieldSegment = default;
            index = -1;

            if (path.Length == 0)
                return false;

            var current = this;
            var reader = new PathReader(path, separator);

            while (reader.MoveNext(out var segName, out var segIndex))
            {
                if (!reader.HasNext)
                {
                    if (segName.Length == 0)
                        return false;

                    storageObject = current;
                    fieldSegment = segName;
                    index = segIndex;
                    return true;
                }

                StorageObject next;

                if (segIndex >= 0)
                {
                    // "Items[3]" segment
                    var arrayView = current.GetArray(segName);
                    if (arrayView.IsDisposed)
                        return false;
                    if (!arrayView.TryGetObject(segIndex, out next))
                        return false;
                }
                else
                {
                    // Non-allocating child lookup
                    if (!current.TryGetObject(segName, out next))
                        return false;
                }

                current = next;
            }
            return false;
        }

        #endregion


        #region Object

        /// <summary>
        /// Get child object (always not null)
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObject(int index) => GetObject(index, reschemeOnMissing: true, layout: ContainerLayout.Empty);

        /// <summary>
        /// Get child object with layout
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="reschemeOnMissing"></param>
        /// <param name="layout"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObject(int index, bool reschemeOnMissing, ContainerLayout layout)
        {
            _container.EnsureNotDisposed(_generation);
            ref ContainerReference idRef = ref reschemeOnMissing ? ref _container.GetRef(index) : ref _container.GetRefNoRescheme(index);
            var obj = layout == null
                ? StorageObjectFactory.GetNoAllocate(idRef)
                : (idRef.TryGet(out var existingObj)
                    ? existingObj
                    : StorageObjectFactory.GetOrCreate(ref idRef, _container, layout, _container.GetFieldName(index))
                );

            return obj;
        }

        /// <summary>
        /// Get child object (always not null)
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObject(string fieldName)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return GetObject(fieldName, reschemeOnMissing: true, layout: ContainerLayout.Empty);
        }

        /// <summary>
        /// Get child object (always not null)
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObject(ReadOnlySpan<char> fieldName) => GetObject(fieldName, reschemeOnMissing: true, layout: ContainerLayout.Empty);

        /// <summary>
        /// Get child object with layout
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="reschemeOnMissing"></param>
        /// <param name="layout"></param>
        /// <returns>storage object, null storage object if object does not exist </returns>
        /// <exception cref="ObjectDisposedException">If container is disposed</exception>
        /// <exception cref="InvalidOperationException">If field is not a reference field</exception>"
        /// <exception cref="ArgumentException">If field does not exist and reschemeOnMissing is false</exception>"
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObject(ReadOnlySpan<char> fieldName, bool reschemeOnMissing = true, ContainerLayout layout = null)
        {
            _container.EnsureNotDisposed(_generation);
            ref ContainerReference idRef = ref reschemeOnMissing ? ref _container.GetRef(fieldName) : ref _container.GetRefNoRescheme(fieldName);

            var obj = layout == null
                ? StorageObjectFactory.GetNoAllocate(idRef)
                : (idRef.TryGet(out var existingObj)
                    ? existingObj
                    : StorageObjectFactory.GetOrCreate(ref idRef, _container, layout, fieldName)
                );
            return obj;
        }

        /// <summary>
        /// Get child object with layout
        /// </summary>
        /// <param name="header"></param>
        /// <param name="layout"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private StorageObject GetObject(in FieldHeader header, ContainerLayout layout)
        {
            ref ContainerReference idRef = ref _container.GetRefSpan(header)[0];
            var obj = layout == null
                ? StorageObjectFactory.GetNoAllocate(idRef)
                : (idRef.TryGet(out var existingObj)
                    ? existingObj
                    : StorageObjectFactory.GetOrCreate(ref idRef, _container, layout, _container.GetFieldName(in header))
                );
            return obj;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetObject(string fieldName, out StorageObject storageObject)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return TryGetObject(fieldName.AsSpan(), out storageObject);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetObject(ReadOnlySpan<char> fieldName, out StorageObject storageObject)
        {
            _container.EnsureNotDisposed(_generation);
            if (!_container.TryGetRef(fieldName, out var containerReferences))
            {
                storageObject = default;
                return false;
            }
            //ref var idRef = ref _container.GetRefNoRescheme(fieldName);
            storageObject = StorageObjectFactory.GetNoAllocate(containerReferences[0]);
            return !storageObject.IsNull;
        }


        #endregion


        #region Object by Path

        /// <summary>
        /// Get a child object by dot-separated path, creating intermediate
        /// objects as needed. The last segment is treated as an object field.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObjectByPath(string path)
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            return GetObjectByPath(path.AsSpan());
        }

        /// <summary>
        /// Get a child object by dot-separated path, creating intermediate
        /// objects as needed. The last segment is treated as an object field.
        /// </summary>
        public StorageObject GetObjectByPath(ReadOnlySpan<char> path, bool createIfMissing = true, char separator = DefaultPathSeparator)
        {
            if (path.Length == 0)
                throw new ArgumentException("Path cannot be empty.", nameof(path));

            var parent = NavigateToObject(path, separator, createIfMissing, out var fieldName, out var index);
            return index < 0
                ? parent.GetObject(fieldName, reschemeOnMissing: createIfMissing, layout: ContainerLayout.Empty)
                : parent.GetObjectInArray(fieldName, index, createIfMissing, createIfMissing);
        }




        /// <summary>
        /// Get a child object by dot-separated path, creating intermediate
        /// objects as needed. The last segment is treated as an object field.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetObjectByPath(string path, out StorageObject storageObject)
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            return TryGetObjectByPath(path.AsSpan(), out storageObject);
        }

        /// <summary>
        /// Get a child object by dot-separated path, creating intermediate
        /// objects as needed. The last segment is treated as an object field.
        /// </summary>
        public bool TryGetObjectByPath(ReadOnlySpan<char> path, out StorageObject storageObject, char separator = DefaultPathSeparator)
        {
            if (!TryNavigateToObject(path, separator, out var parent, out var fieldSegment, out var index))
            {
                storageObject = default;
                return false;
            }
            if (index < 0)
            {
                return parent.TryGetObject(fieldSegment, out storageObject);
            }
            else if (parent.TryGetArray(fieldSegment, out var arr))
            {
                return arr.TryGetObject(index, out storageObject);
            }
            storageObject = default;
            return false;
        }


        // --- Restored object-in-array accessors ---
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal StorageObject GetObjectInArray(string fieldName, int index)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return GetObjectInArray(fieldName.AsSpan(), index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal StorageObject GetObjectInArray(ReadOnlySpan<char> fieldName, int index)
        {
            return GetObjectInArray(in _container.GetFieldHeader(fieldName), index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal StorageObject GetObjectInArray(int fieldIndex, int index)
        {
            return GetObjectInArray(in _container.GetFieldHeader(fieldIndex), index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal StorageObject GetObjectInArray(in FieldHeader header, int index)
        {
            // inline array of refs
            if (header.IsInlineArray)
            {
                if (header.FieldType != TypeUtil<ContainerReference>.ArrayFieldType)
                    throw new InvalidOperationException($"Field {_container.GetFieldName(in header).ToString()} is not an object array.");
                var refs = _container.GetFieldData<ContainerReference>(in header);
                return StorageObjectFactory.GetNoAllocate(refs[index]);
            }
            // referenced object that itself is an array
            var obj = GetObject(in header, null);
            if (!obj.IsNull && obj.IsArray())
            {
                var refs = obj._container.GetRefSpan(0);
                return StorageObjectFactory.GetNoAllocate(refs[index]);
            }
            return default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal StorageObject GetObjectInArray(ReadOnlySpan<char> fieldName, int index, bool allowCreateArray, bool allowCreateObject)
        {
            if (index < 0) ThrowHelper.ThrowIndexOutOfRange();
            _container.EnsureNotDisposed(_generation);

            if (!TryGetArray(fieldName, out StorageArray arrayView))
            {
                if (!allowCreateArray)
                    return default;
                var holder = GetObject(fieldName); // create ref field if missing
                switch (holder.FieldCount)
                {
                    case 0:
                        holder.MakeArray(TypeData.Ref, index + 1);
                        arrayView = holder.AsArray();
                        break;
                    default:
                        arrayView = holder.IsArray() ? holder.AsArray() : throw new InvalidOperationException($"Field {fieldName.ToString()} exists but is not an array.");
                        break;
                }
            }
            if (index >= arrayView.Length)
            {
                if (!allowCreateArray) return default;
                arrayView.EnsureLength(index + 1);
            }
            if (allowCreateObject) return arrayView.GetObject(index);
            return arrayView.TryGetObject(index, out var child) ? child : default;
        }

        #endregion





        /// <summary>
        /// Delete a field from this object. Returns true if the field was found and deleted, false otherwise.
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Delete(string fieldName)
        {
            _container.EnsureNotDisposed(_generation);
            if (!_container.TryGetFieldHeader(fieldName, out var headerSpan))
                return false;

            bool removed = false;
            _container.Rescheme(b => removed = b.Remove(fieldName));
            return removed;
        }

        /// <summary>
        /// Delete multiple fields from this object. Returns the number of fields deleted.
        /// </summary>
        /// <param name="names"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Delete(params string[] names)
        {
            _container.EnsureNotDisposed(_generation);
            int count = 0;
            _container.Rescheme(b =>
            {
                foreach (var name in names)
                {
                    if (b.Remove(name)) count++;
                }
            });
            return count;
        }






        /// <summary>
        /// Get a scala value view
        /// </summary>
        /// <remarks>
        /// Write to the value view will not trigger write event.
        /// </remarks>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueView GetValueView(string fieldName)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return GetValueView(fieldName.AsSpan());
        }

        /// <summary>
        /// Get a scala value view
        /// </summary>
        /// <remarks>
        /// Write to the value view will not trigger write event.
        /// </remarks>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueView GetValueView(ReadOnlySpan<char> fieldName) => _container.EnsureNotDisposed(_generation).GetValueView(fieldName);

        /// <summary>
        /// Get a scala value view
        /// </summary>
        /// <remarks>
        /// Write to the value view will not trigger write event.
        /// </remarks>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueView GetValueView(int index) => _container.EnsureNotDisposed(_generation).GetValueView(index);




        #region Persistent Field Info

        /// <summary>Check a field exist.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasField(string fieldName)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return _container.EnsureNotDisposed(_generation).IndexOf(fieldName) >= 0;
        }

        /// <summary>Check a field exist.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasField(ReadOnlySpan<char> fieldName) => _container.EnsureNotDisposed(_generation).IndexOf(fieldName) >= 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FieldInfo GetField(string fieldName)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return GetField(fieldName.AsSpan());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FieldInfo GetField(ReadOnlySpan<char> fieldName)
        {
            _container.EnsureNotDisposed(_generation);
            _container.TryGetFieldHeader(fieldName, out var headerSpan);
            var name = _container.GetFieldName(in headerSpan[0]);
            return new FieldInfo(name, in headerSpan[0]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FieldInfo GetField(int index)
        {
            _container.EnsureNotDisposed(_generation);
            ref var header = ref _container.GetFieldHeader(index);
            var name = _container.GetFieldName(in header);
            return new FieldInfo(name, in header);
        }

        #endregion





        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal FieldHandle GetFieldHandle(ReadOnlySpan<char> fieldName)
        {
            return new FieldHandle(_container, fieldName);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal FieldHandle GetFieldHandle(string fieldName)
        {
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            return new FieldHandle(_container, fieldName);
        }





        /// <summary>
        /// General member access by path
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public StorageMember GetMember(string path, char separator = DefaultPathSeparator)
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            return GetMember(path.AsSpan(), separator);
        }

        /// <summary>
        /// General member access by path
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public StorageMember GetMember(ReadOnlySpan<char> path, char separator = DefaultPathSeparator)
        {
            var obj = NavigateToObject(path, separator, true, out var fieldName, out var index);
            return new StorageMember(obj, fieldName, index);
        }

        /// <summary>
        /// General member access by path
        /// </summary>
        /// <param name="path"></param>
        /// <param name="member"></param>
        /// <param name="separator"></param>
        /// <returns></returns>
        public bool TryGetMember(string path, out StorageMember member, char separator = DefaultPathSeparator)
        {
            ThrowHelper.ThrowIfNull(path, nameof(path));
            return TryGetMember(path.AsSpan(), out member, separator);
        }

        /// <summary>
        /// General member access by path
        /// </summary>
        /// <param name="path"></param>
        /// <param name="member"></param>
        /// <param name="separator"></param>
        /// <returns></returns>
        public bool TryGetMember(ReadOnlySpan<char> path, out StorageMember member, char separator = DefaultPathSeparator)
        {
            member = default;
            if (!TryNavigateToObject(path, separator, out var obj, out var fieldName, out var index))
                return false;
            if (!obj.HasField(fieldName))
                return false;
            member = new StorageMember(obj, fieldName, index);
            return member.Exist;
        }










        /// <summary>
        /// Rescheme the container to match the target schema.
        /// </summary>
        /// <param name="target"></param> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Rescheme(ContainerLayout target) => _container.EnsureNotDisposed(_generation).Rescheme(target);





        public static bool operator ==(StorageObject left, StorageObject right) => left.Equals(right);
        public static bool operator !=(StorageObject left, StorageObject right) => !(left == right);
        public override int GetHashCode() => HashCode.Combine(_container, _generation);
        public override bool Equals(object obj) => obj is StorageObject storageObject && Equals(storageObject);
        public override string ToString() => _container.ToString();
        public bool Equals(StorageObject other)
        {
            if (IsNull) return other.IsNull;
            if (other.IsNull) return false;
            return _container == other._container && _generation == other._generation;
        }






        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void NotifyFieldWrite(Container container, int fieldIndex)
        {
            if (!StorageEventRegistry.HasSubscribers(container))
                return;
            ref var header = ref container.GetFieldHeader(fieldIndex);
            var fieldName = container.GetFieldName(in header).ToString();
            StorageEventRegistry.NotifyFieldWrite(container, fieldName, header.FieldType);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void NotifyFieldWrite(Container container, ReadOnlySpan<char> fieldName)
        {
            if (!StorageEventRegistry.HasSubscribers(container))
                return;
            var type = container.GetFieldHeader(fieldName).FieldType;
            StorageEventRegistry.NotifyFieldWrite(container, fieldName.ToString(), type);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void NotifyFieldWrite(Container container, string fieldName)
        {
            if (!StorageEventRegistry.HasSubscribers(container))
                return;
            var type = container.GetFieldHeader(fieldName).FieldType;
            StorageEventRegistry.NotifyFieldWrite(container, fieldName, type);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void NotifyFieldDelete(Container container, string fieldName, FieldType fieldType)
        {
            if (!StorageEventRegistry.HasSubscribers(container))
                return;

            StorageEventRegistry.NotifyFieldDelete(container, fieldName, fieldType);
            StorageEventRegistry.RemoveFieldSubscriptions(container, fieldName);
        }
    }
}

