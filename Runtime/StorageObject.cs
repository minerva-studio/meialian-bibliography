using System;
using System.Runtime.InteropServices;

namespace Amlos.Container
{
    /// <summary>
    /// Stack-only view of a container within a Storage tree.
    /// Cannot be persisted; exposes only read/write and navigation helpers.
    /// </summary>
    public readonly ref struct StorageObject
    {
        private readonly Container _container;


        /// <summary>
        /// Object ID   
        /// </summary>
        public ulong ID => _container?.ID ?? 0;

        /// <summary>
        /// Is object null
        /// </summary>
        public bool IsNull => _container == null || _container.ID == 0;

        /// <summary>
        /// True if this StorageObject represents a single string field, then this container is really just a string.
        /// </summary>
        public bool IsString => IsArray && _container.View[0].Type == ValueType.Char16;

        /// <summary>
        /// Is an array object
        /// </summary>
        public bool IsArray => _container.FieldCount == 1 && _container.View[0].IsArray;

        public int FieldCount => _container.FieldCount;

        internal ref byte[] Buffer => ref _container.Buffer;

        internal Container Container => _container;

        internal StorageObject(Container container)
        {
            _container = container ?? throw new InvalidOperationException();
        }




        private void EnsureNotNull()
        {
            if (_container is null)
                throw new InvalidOperationException("This StorageObject is null.");
        }




        internal FieldView GetFieldView(ReadOnlySpan<char> fieldName) => _container.View[fieldName];
        internal FieldView GetFieldView(int index) => _container.View[index];




        public int IndexOf(ReadOnlySpan<char> fieldName) => _container.IndexOf(fieldName);




        // Basic read/write passthroughs (blittable)

        public void Write(string fieldName, in string value) => WriteString(fieldName, (ReadOnlySpan<char>)value);

        public void Write(int index, in string value) => WriteString(index, (ReadOnlySpan<char>)value);

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
        public void Write<T>(string fieldName, in T value) where T : unmanaged => _container.Write(fieldName, value, true);

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>
        public void Write<T>(int index, in T value) where T : unmanaged => _container.Write_Internal(index, value, true);

        /// <summary>
        /// Write a value to an existing field without rescheming, if the field does not exist, an exception is thrown.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        public void Write<T>(string fieldName, in T value, bool allowRescheme = true) where T : unmanaged => _container.Write(fieldName, value, allowRescheme);

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        public void TryWrite<T>(string fieldName, in T value) where T : unmanaged => _container.TryWrite(fieldName, value, true);

        /// <summary>
        /// Write a value to a field, if the field does not exist, it will be added to the schema.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        public void TryWrite<T>(string fieldName, in T value, bool allowRescheme = true) where T : unmanaged => _container.TryWrite(fieldName, value, allowRescheme);





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
        public T Read<T>(string fieldName) where T : unmanaged => _container.Read<T>(fieldName);

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
        public bool TryRead<T>(string fieldName, out T value) where T : unmanaged => _container.TryRead(fieldName, out value);

        public T ReadOrDefault<T>(string fieldName) where T : unmanaged => _container.TryRead(fieldName, out T value) ? value : default;

        public T ReadOrDefault<T>(string fieldName, T defaultValue) where T : unmanaged => _container.TryRead(fieldName, out T value) ? value : defaultValue;

        /// <summary>
        /// Read data regardless actual stored type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public T Read_Unsafe<T>(string fieldName) where T : unmanaged => _container.Read_Unsafe<T>(fieldName);






        public void Override<T>(string fieldName, T value)
        {

        }






        /// <summary>
        /// Delete a field from this object. Returns true if the field was found and deleted, false otherwise.
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public bool Delete(string fieldName)
        {
            bool result = false;
            _container.Rescheme(b => result = b.Remove(fieldName));
            return result;
        }

        /// <summary>
        /// Delete multiple fields from this object. Returns the number of fields deleted.
        /// </summary>
        /// <param name="names"></param>
        /// <returns></returns>
        public int Delete(params string[] names)
        {
            EnsureNotNull();
            int result = 0;
            for (int i = 0; i < names.Length; i++)
            {
                string item = names[i];
                _container.Rescheme(b => result += b.Remove(item) ? 1 : 0);
            }
            return result;
        }





        public void WriteString(int index, ReadOnlySpan<char> value) => GetObject(index).WriteArray(value);

        public void WriteString(string fieldName, ReadOnlySpan<char> value) => GetObject(fieldName).WriteArray(value);

        public void WriteString(ReadOnlySpan<char> value) => WriteArray(value);

        /// <summary>
        /// Read as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        public string ReadString(string fieldName) => GetObject(fieldName).ReadString();

        /// <summary>
        /// Read entire container as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        public string ReadString()
        {
            if (!IsString)
            {
                if (FieldCount == 0)
                    throw new InvalidOperationException($"This StorageObject does not represent a single string field because object is empty.");
                if (FieldCount == 1)
                    throw new InvalidOperationException($"This StorageObject does not represent a single string field because the field field is of type {_container.View.GetFieldHeader(0).FieldType}.");
                throw new InvalidOperationException($"This StorageObject does not represent a single string field because the field count is {FieldCount}.");
            }

            ReadOnlySpan<char> value = _container.GetReadOnlySpan<char>(0);
            return new(value);
        }




        public void WriteArray<T>(string fieldName, ReadOnlySpan<T> value) where T : unmanaged => GetObject(fieldName).WriteArray(value);

        public void WriteArray<T>(ReadOnlySpan<T> value) where T : unmanaged
        {
            if (!IsArray)
            {
                if (FieldCount != 0)
                    throw new InvalidOperationException("This StorageObject does not represent an array.");
            }

            Rescheme(ContainerLayout.BuildArray<T>(value.Length));
            MemoryMarshal.AsBytes(value).CopyTo(_container.View[0].Data);
        }

        /// <summary>
        /// Read as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        public T[] ReadArray<T>(string fieldName) where T : unmanaged => GetObject(fieldName).ReadArray<T>();

        /// <summary>
        /// Read entire container as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        public T[] ReadArray<T>() where T : unmanaged
        {
            if (!IsString)
                throw new InvalidOperationException("This StorageObject does not represent a single string field.");

            return _container.GetReadOnlySpan<T>(0).ToArray();
        }





        // Child navigation by reference field (single)

        /// <summary>
        /// Get child object (always not null)
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public StorageObject GetObject(int index) => GetObject(index, reschemeOnMissing: true, layout: ContainerLayout.Empty);
        /// <summary>
        /// Get child object (always not null)
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public StorageObject GetObject(string fieldName) => GetObject(fieldName, reschemeOnMissing: true, layout: ContainerLayout.Empty);
        /// <summary>
        /// Get child with layout, if null, create a new object with given layout
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="layout"></param>
        /// <returns></returns>
        public StorageObject GetObject(string fieldName, ContainerLayout layout = null) => GetObject(fieldName, reschemeOnMissing: true, layout: layout ?? ContainerLayout.Empty);
        /// <summary>
        /// Get without allocating a 
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public StorageObject GetObjectNoAllocate(string fieldName) => GetObject(fieldName, reschemeOnMissing: false, layout: null);
        /// <summary>
        /// Get child object with layout
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="reschemeOnMissing"></param>
        /// <param name="layout"></param>
        /// <returns></returns>
        public StorageObject GetObject(int index, bool reschemeOnMissing, ContainerLayout layout)
        {
            ref ContainerReference idRef = ref reschemeOnMissing ? ref _container.GetRef(index) : ref _container.GetRefNoRescheme(index);
            return layout != null ? StorageFactory.Get(ref idRef, layout) : StorageFactory.GetNoAllocate(idRef);
        }
        /// <summary>
        /// Get child object with layout
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="reschemeOnMissing"></param>
        /// <param name="layout"></param>
        /// <returns></returns>
        public StorageObject GetObject(string fieldName, bool reschemeOnMissing, ContainerLayout layout)
        {
            ref ContainerReference idRef = ref reschemeOnMissing ? ref _container.GetRef(fieldName) : ref _container.GetRefNoRescheme(fieldName);
            return layout != null ? StorageFactory.Get(ref idRef, layout) : StorageFactory.GetNoAllocate(idRef);
        }


        /// <summary>
        /// Get a stack-only view over a value array field T[].
        /// Field must be non-ref and length divisible by sizeof(T).
        /// </summary>
        public StorageInlineArray GetArray(string fieldName) => new(_container.View[fieldName]);

        /// <summary>
        /// Get a stack-only view over a child reference array (IDs).
        /// Field must be a ref field.
        /// </summary>
        public StorageObjectArray GetObjectArray(string fieldName)
        {
            return new StorageObjectArray(_container, _container.View[fieldName]);
        }

        /// <summary>
        /// Get a scala value view
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public ReadOnlyValueView GetValueView(string fieldName) => _container.GetValueView(fieldName);

        public FieldInfo GetField(string fieldName) => GetField(IndexOf(fieldName));

        public FieldInfo GetField(int index) => GetFieldView(index).ToFieldInfo();



        /// <summary>Check a field exist.</summary>
        public bool HasField(string fieldName) => _container.IndexOf(fieldName) >= 0;

        /// <summary>
        /// Rescheme the container to match the target schema.
        /// </summary>
        /// <param name="target"></param> 
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

