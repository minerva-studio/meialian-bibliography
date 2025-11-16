using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Stack-only view of a container within a Storage tree.
    /// Cannot be persisted; exposes only read/write and navigation helpers.
    /// </summary>
    public readonly struct StorageObject
    {
        private readonly Container _container;
        private readonly int _generation;


        /// <summary>
        /// Object ID   
        /// </summary>
        public ulong ID
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _container?.ID ?? Container.Registry.ID.Empty;
        }

        /// <summary>
        /// Is object null
        /// </summary>
        public bool IsNull
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _container == null || _container.ID == Container.Registry.ID.Empty;
        }

        /// <summary>
        /// True if this StorageObject represents a single string field, then this container is really just a string.
        /// </summary>
        public bool IsString
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => IsArray && _container.GetFieldHeader(0).Type == ValueType.Char16;
        }

        /// <summary>
        /// Is an array object
        /// </summary>
        public bool IsArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _container.FieldCount == 1 && _container.GetFieldHeader(0).IsInlineArray;
        }

        public int FieldCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _container.FieldCount;
        }

        internal readonly ref AllocatedMemory Memory
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref _container.Memory;
        }

        internal Container Container => _container;





        internal StorageObject(Container container)
        {
            _container = container ?? throw new InvalidOperationException();
            _generation = container.Generation;
        }




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal FieldView GetFieldView(ReadOnlySpan<char> fieldName) => _container.View[fieldName];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal FieldView GetFieldView(int index) => _container.View[index];





        // Basic read/write passthroughs (blittable)

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(string fieldName, string value) => WriteString(fieldName, (ReadOnlySpan<char>)value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(int index, string value) => WriteString(index, (ReadOnlySpan<char>)value);

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
            => _container.Write_Internal(ref _container.EnsureNotDisposed(_generation).GetFieldHeader<T>(fieldName, true), value, true);

        /// <summary>
        /// Write a value to an existing field without rescheming, if the field does not exist, an exception is thrown.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(string fieldName, T value, bool allowRescheme = true) where T : unmanaged
            => _container.Write_Internal(ref _container.EnsureNotDisposed(_generation).GetFieldHeader<T>(fieldName, allowRescheme), value, allowRescheme);

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>   
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(int index, T value) where T : unmanaged
            => _container.Write_Internal(ref _container.EnsureNotDisposed(_generation).GetFieldHeader(index), value, true);
        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>   
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write<T>(int index, T value, bool allowResize = true) where T : unmanaged
            => _container.Write_Internal(ref _container.EnsureNotDisposed(_generation).GetFieldHeader(index), value, allowResize);

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void TryWrite<T>(string fieldName, T value) where T : unmanaged
            => _container.TryWrite_Internal(ref _container.EnsureNotDisposed(_generation).GetFieldHeader<T>(fieldName, true), value, true);

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void TryWrite<T>(string fieldName, T value, bool allowRescheme = true) where T : unmanaged
            => _container.TryWrite_Internal(ref _container.EnsureNotDisposed(_generation).GetFieldHeader<T>(fieldName, allowRescheme), value, allowRescheme);








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
        public bool TryRead<T>(ReadOnlySpan<char> fieldName, out T value) where T : unmanaged
        {
            value = default;
            return _container.EnsureNotDisposed(_generation).TryGetFieldHeader(fieldName, out var outHeader) && _container.TryReadScalarExplicit(ref outHeader[0], out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T ReadOrDefault<T>(string fieldName) where T : unmanaged => _container.EnsureNotDisposed(_generation).TryRead(fieldName, out T value) ? value : default;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T ReadOrDefault<T>(string fieldName, T defaultValue) where T : unmanaged => _container.EnsureNotDisposed(_generation).TryRead(fieldName, out T value) ? value : defaultValue;

        /// <summary>
        /// Read data regardless actual stored type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read_Unsafe<T>(string fieldName) where T : unmanaged => _container.Read_Unsafe<T>(fieldName);


        private const char DefaultPathSeparator = '.';

        /// <summary>
        /// Get a child object by dot-separated path, creating intermediate
        /// objects as needed. The last segment is treated as an object field.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObjectByPath(string path)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            return GetObjectByPath(path.AsSpan());
        }

        /// <summary>
        /// Get a child object by dot-separated path, creating intermediate
        /// objects as needed. The last segment is treated as an object field.
        /// </summary>
        public StorageObject GetObjectByPath(ReadOnlySpan<char> path, char separator = DefaultPathSeparator)
        {
            if (path.Length == 0)
                throw new ArgumentException("Path cannot be empty.", nameof(path));

            var current = this;
            int start = 0;

            while (true)
            {
                var remaining = path.Slice(start);
                int rel = remaining.IndexOf(separator);
                if (rel < 0)
                {
                    var segment = remaining;
                    if (segment.Length == 0)
                        throw new ArgumentException("Path cannot end with a separator.", nameof(path));

                    return current.GetObject(segment);
                }

                var part = remaining.Slice(0, rel);
                if (part.Length == 0)
                    throw new ArgumentException("Path contains an empty segment.", nameof(path));

                current = current.GetObject(part);
                start += rel + 1;
            }
        }

        /// <summary>
        /// Write a scalar value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByPath<T>(string path, T value) where T : unmanaged
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            WriteByPath(path.AsSpan(), value);
        }

        /// <summary>
        /// Write a scalar value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        public void WriteByPath<T>(ReadOnlySpan<char> path, T value, char separator = DefaultPathSeparator) where T : unmanaged
        {
            var container = NavigateToContainer(path, separator, createIfMissing: true, out var fieldSegment);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            container.Write(fieldSegment.ToString(), value);
        }

        /// <summary>
        /// Write a string value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteByPath(string path, string value)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            WriteByPath(path.AsSpan(), value.AsSpan());
        }

        /// <summary>
        /// Write a string value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        public void WriteByPath(ReadOnlySpan<char> path, ReadOnlySpan<char> value, char separator = DefaultPathSeparator)
        {
            var container = NavigateToContainer(path, separator, createIfMissing: true, out var fieldSegment);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            container.WriteString(fieldSegment.ToString(), value);
        }

        /// <summary>
        /// Write an inline array value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArrayByPath<T>(string path, T[] value) where T : unmanaged
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (value == null) throw new ArgumentNullException(nameof(value));
            WriteArrayByPath<T>(path.AsSpan(), value.AsSpan());
        }

        /// <summary>
        /// Write an inline array value to a field located by a dot-separated path.
        /// Intermediate objects are created as needed.
        /// </summary>
        public void WriteArrayByPath<T>(ReadOnlySpan<char> path, ReadOnlySpan<T> value, char separator = DefaultPathSeparator) where T : unmanaged
        {
            var container = NavigateToContainer(path, separator, createIfMissing: true, out var fieldSegment);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            container.WriteArray(fieldSegment, value);
        }

        /// <summary>
        /// Read a scalar field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T ReadByPath<T>(string path) where T : unmanaged
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            return ReadByPath<T>(path.AsSpan());
        }

        /// <summary>
        /// Read a scalar field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        public T ReadByPath<T>(ReadOnlySpan<char> path, char separator = DefaultPathSeparator) where T : unmanaged
        {
            var container = NavigateToContainer(path, separator, createIfMissing: false, out var fieldSegment);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            if (!container.TryRead(fieldSegment, out T value))
                throw new InvalidOperationException($"Field '{fieldSegment.ToString()}' not found for path '{path.ToString()}'.");

            return value;
        }

        /// <summary>
        /// Try to read a scalar field by path without allocating intermediate
        /// objects. Returns false if any segment or field is missing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryReadByPath<T>(string path, out T value) where T : unmanaged
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            return TryReadByPath(path.AsSpan(), out value);
        }

        /// <summary>
        /// Try to read a scalar field by path without allocating intermediate
        /// objects. Returns false if any segment or field is missing.
        /// </summary>
        public bool TryReadByPath<T>(ReadOnlySpan<char> path, out T value, char separator = DefaultPathSeparator) where T : unmanaged
        {
            value = default;

            try
            {
                var container = NavigateToContainer(path, separator, createIfMissing: false, out var fieldSegment);
                if (fieldSegment.Length == 0)
                    return false;

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

        /// <summary>
        /// Read a string field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadStringByPath(string path)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            return ReadStringByPath(path.AsSpan());
        }

        /// <summary>
        /// Read a string field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        public string ReadStringByPath(ReadOnlySpan<char> path, char separator = DefaultPathSeparator)
        {
            var container = NavigateToContainer(path, separator, createIfMissing: false, out var fieldSegment);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            if (!container.HasField(fieldSegment))
                throw new ArgumentException($"Path segment '{fieldSegment.ToString()}' does not exist on the current object.", nameof(path));

            var child = container.GetObject(fieldSegment, reschemeOnMissing: false, layout: null);
            if (child.IsNull)
                throw new InvalidOperationException($"Path segment '{fieldSegment.ToString()}' refers to a null child object.");

            return child.ReadString();
        }

        /// <summary>
        /// Read an inline array field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T[] ReadArrayByPath<T>(string path) where T : unmanaged
        {
            if (path == null) throw new ArgumentNullException(nameof(path));
            return ReadArrayByPath<T>(path.AsSpan());
        }

        /// <summary>
        /// Read an inline array field located by a dot-separated path.
        /// Does not create missing fields; throws if any segment or field is missing.
        /// </summary>
        public T[] ReadArrayByPath<T>(ReadOnlySpan<char> path, char separator = DefaultPathSeparator) where T : unmanaged
        {
            var container = NavigateToContainer(path, separator, createIfMissing: false, out var fieldSegment);
            if (fieldSegment.Length == 0)
                throw new ArgumentException("Path must contain at least one segment.", nameof(path));

            if (!container.HasField(fieldSegment))
                throw new ArgumentException($"Path segment '{fieldSegment.ToString()}' does not exist on the current object.", nameof(path));

            var child = container.GetObject(fieldSegment, reschemeOnMissing: false, layout: null);
            if (child.IsNull)
                throw new InvalidOperationException($"Path segment '{fieldSegment.ToString()}' refers to a null child object.");

            return child.ReadArray<T>();
        }

        /// <summary>
        /// Navigate to the container that owns the final field in a path.
        /// For example, with "a.b.c", this returns object "b" and fieldSegment "c".
        /// </summary>
        private StorageObject NavigateToContainer(ReadOnlySpan<char> path, char separator, bool createIfMissing, out ReadOnlySpan<char> fieldSegment)
        {
            if (path.Length == 0)
                throw new ArgumentException("Path cannot be empty.", nameof(path));

            var current = this;
            int start = 0;

            while (true)
            {
                var remaining = path.Slice(start);
                int rel = remaining.IndexOf(separator);
                if (rel < 0)
                {
                    fieldSegment = remaining;
                    if (fieldSegment.Length == 0)
                        throw new ArgumentException("Path cannot end with a separator.", nameof(path));
                    return current;
                }

                var part = remaining.Slice(0, rel);
                if (part.Length == 0)
                    throw new ArgumentException("Path contains an empty segment.", nameof(path));

                if (createIfMissing)
                {
                    current = current.GetObject(part);
                }
                else
                {
                    if (!current.HasField(part))
                        throw new ArgumentException($"Path segment '{part.ToString()}' does not exist on the current object.", nameof(path));

                    var next = current.GetObject(part, reschemeOnMissing: false, layout: null);
                    if (next.IsNull)
                        throw new InvalidOperationException($"Path segment '{part.ToString()}' refers to a null child object.");

                    current = next;
                }

                start += rel + 1;
            }
        }




        /// <summary>
        /// Delete a field from this object. Returns true if the field was found and deleted, false otherwise.
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Delete(string fieldName)
        {
            bool result = false;
            _container.EnsureNotDisposed(_generation).Rescheme(b => result = b.Remove(fieldName));
            return result;
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
            int result = 0;
            for (int i = 0; i < names.Length; i++)
            {
                string item = names[i];
                _container.Rescheme(b => result += b.Remove(item) ? 1 : 0);
            }
            return result;
        }





        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(int index, ReadOnlySpan<char> value) => GetObject(index).WriteArray(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(string fieldName, ReadOnlySpan<char> value) => GetObject(fieldName).WriteArray(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteString(ReadOnlySpan<char> value) => WriteArray(value);

        /// <summary>
        /// Read as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadString(string fieldName) => GetObject(fieldName).ReadString();

        /// <summary>
        /// Read entire container as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadString()
        {
            if (!IsString)
            {
                if (FieldCount == 0)
                    throw new InvalidOperationException($"This StorageObject does not represent a single string field because object is empty.");
                if (FieldCount == 1)
                    throw new InvalidOperationException($"This StorageObject does not represent a single string field because the field field is of type {_container.GetFieldHeader(0).FieldType}.");
                throw new InvalidOperationException($"This StorageObject does not represent a single string field because the field count is {FieldCount}.");
            }

            ReadOnlySpan<char> value = _container.GetFieldData<char>(in _container.GetFieldHeader(0));
            return new(value);
        }




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArray<T>(ReadOnlySpan<char> fieldName, ReadOnlySpan<T> value) where T : unmanaged => GetObject(fieldName).WriteArray(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteArray<T>(ReadOnlySpan<T> value) where T : unmanaged
        {
            if (!IsArray)
            {
                if (FieldCount != 0)
                    throw new InvalidOperationException("This StorageObject does not represent an array.");
            }

            Rescheme(ContainerLayout.BuildArray<T>(value.Length));
            MemoryMarshal.AsBytes(value).CopyTo(_container.GetFieldData(in _container.GetFieldHeader(0)));
        }

        /// <summary>
        /// Read an array from a child field.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T[] ReadArray<T>(ReadOnlySpan<char> fieldName) where T : unmanaged => GetObject(fieldName).ReadArray<T>();

        /// <summary>
        /// Read entire container as an array of unmanaged type T.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T[] ReadArray<T>() where T : unmanaged
        {
            if (!IsArray)
                throw new InvalidOperationException("This StorageObject does not represent an array.");

            return _container.GetFieldData<T>(in _container.GetFieldHeader(0)).ToArray();
        }





        // Child navigation by reference field (single)

        /// <summary>
        /// Get child object (always not null)
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObject(int index) => GetObject(index, reschemeOnMissing: true, layout: ContainerLayout.Empty);
        /// <summary>
        /// Get child object (always not null)
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObject(ReadOnlySpan<char> fieldName) => GetObject(fieldName, reschemeOnMissing: true, layout: ContainerLayout.Empty);
        /// <summary>
        /// Get child with layout, if null, create a new object with given layout
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="layout"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObject(ReadOnlySpan<char> fieldName, ContainerLayout layout = null) => GetObject(fieldName, reschemeOnMissing: true, layout: layout ?? ContainerLayout.Empty);
        /// <summary>
        /// Get without allocating a 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObjectNoAllocate(string fieldName) => GetObject(fieldName, reschemeOnMissing: false, layout: null);
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
            ref ContainerReference idRef = ref reschemeOnMissing ? ref _container.GetRef(index) : ref _container.GetRefNoRescheme(index);
            return layout != null ? StorageObjectFactory.GetOrCreate(ref idRef, layout) : StorageObjectFactory.GetNoAllocate(idRef);
        }
        /// <summary>
        /// Get child object with layout
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="reschemeOnMissing"></param>
        /// <param name="layout"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObject GetObject(ReadOnlySpan<char> fieldName, bool reschemeOnMissing, ContainerLayout layout)
        {
            ref ContainerReference idRef = ref reschemeOnMissing ? ref _container.GetRef(fieldName) : ref _container.GetRefNoRescheme(fieldName);
            return layout != null ? StorageObjectFactory.GetOrCreate(ref idRef, layout) : StorageObjectFactory.GetNoAllocate(idRef);
        }


        /// <summary>
        /// Get a stack-only view over a value array field T[].
        /// Field must be non-ref and length divisible by sizeof(T).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageInlineArray GetArray(ReadOnlySpan<char> fieldName) => new(_container.View[fieldName]);

        /// <summary>
        /// Get a stack-only view over a child reference array (IDs).
        /// Field must be a ref field.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StorageObjectArray GetObjectArray(ReadOnlySpan<char> fieldName)
        {
            return new StorageObjectArray(_container, _container.View[fieldName]);
        }

        /// <summary>
        /// Get a scala value view
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlyValueView GetValueView(string fieldName) => _container.GetValueView(fieldName);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FieldInfo GetField(ReadOnlySpan<char> fieldName)
        {
            _container.TryGetFieldHeader(fieldName, out var headerSpan);
            var name = _container.GetFieldName(in headerSpan[0]);
            return new FieldInfo(name, in headerSpan[0]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public FieldInfo GetField(int index)
        {
            ref var header = ref _container.GetFieldHeader(index);
            var name = _container.GetFieldName(in header);
            return new FieldInfo(name, in header);
        }


        /// <summary>Check a field exist.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasField(ReadOnlySpan<char> fieldName) => _container.IndexOf(fieldName) >= 0;

        /// <summary>
        /// Rescheme the container to match the target schema.
        /// </summary>
        /// <param name="target"></param> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Rescheme(ContainerLayout target) => _container.Rescheme(target);




        public static bool operator ==(StorageObject left, StorageObject right) => left._container == right._container;
        public static bool operator !=(StorageObject left, StorageObject right) => !(left == right);
        public override int GetHashCode() => _container.GetHashCode();
        public override bool Equals(object obj) => false;
        public override string ToString() => _container.ToString();
    }



    public static class StorageObjectExtension
    {

        /// <summary>
        /// Write a value to an existing field without rescheming, if the field does not exist, an exception is thrown.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        public static void WriteNoRescheme<T>(this StorageObject so, string fieldName, in T value) where T : unmanaged => so.Write(fieldName, value, false);
    }
}

