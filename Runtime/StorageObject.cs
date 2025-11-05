using System;

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
        /// Object Schema
        /// </summary>
        public readonly Schema Schema => _container.Schema;

        /// <summary>
        /// Object ID   
        /// </summary>
        public ulong ID => _container.ID;

        /// <summary>
        /// Is object null
        /// </summary>
        public bool IsNull => _container == null || _container._id == 0;

        /// <summary>
        /// True if this StorageObject represents a single string field, then this container is really just a string.
        /// </summary>
        public bool IsString => Schema.Fields.Count == 1 && !Schema.Fields[0].IsRef && TypeUtil.PrimOf(HeaderHints[0]) == ValueType.Char16;

        internal Span<byte> HeaderHints => _container.HeaderSegment;



        internal StorageObject(Container container)
        {
            _container = container;
        }




        private void EnsureNotNull()
        {
            if (_container is null)
                throw new InvalidOperationException("This StorageObject is null.");
        }



        // Basic read/write passthroughs (blittable)

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







        /// <summary>
        /// Delete a field from this object. Returns true if the field was found and deleted, false otherwise.
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public bool Delete(string fieldName)
        {
            bool result = false;
            _container.Rescheme(b => result = b.RemoveField(fieldName));
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
            _container.Rescheme(b => result = b.RemoveFields(names));
            return result;
        }





        public void WriteString(string fieldName, ReadOnlySpan<char> value) => GetObject(fieldName).WriteString(value);

        public void WriteString(ReadOnlySpan<char> value)
        {
            Rescheme(SchemaBuilder.BuildString(value.Length));
            value.CopyTo(_container.GetSpan<char>(_container.Schema.Fields[0]));
        }

        /// <summary>
        /// Read as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        public string ReadString(string fieldName) => new(_container.GetReadOnlySpan<char>(fieldName));

        /// <summary>
        /// Read entire container as a string (UTF-16)
        /// </summary>
        /// <returns></returns>
        public string ReadString()
        {
            if (!IsString)
                throw new InvalidOperationException("This StorageObject does not represent a single string field.");

            return new(_container.GetReadOnlySpan<char>(Schema.Fields[0]));
        }




        // Child navigation by reference field (single)
        public StorageObject GetObject(string fieldName, bool reschemeOnMissing, Schema newSchema)
        {
            ref ulong idRef = ref reschemeOnMissing ? ref _container.GetRef(fieldName) : ref _container.GetRefNoRescheme(fieldName);
            return newSchema != null ? StorageFactory.Get(ref idRef, newSchema) : StorageFactory.GetNoAllocate(idRef);
        }

        // Child navigation by reference field (single)
        public StorageObject GetObject(string fieldName) => GetObject(fieldName, reschemeOnMissing: true, newSchema: Schema.Empty);
        public StorageObject GetObject(string fieldName, Schema schema = null) => GetObject(fieldName, reschemeOnMissing: true, newSchema: schema ?? Schema.Empty);

        // Child navigation by reference field (single)
        public StorageObject GetObjectNoAllocate(string fieldName) => GetObject(fieldName, reschemeOnMissing: true, newSchema: null);

        /// <summary>
        /// Get a stack-only view over a value array field T[].
        /// Field must be non-ref and length divisible by sizeof(T).
        /// </summary>
        public StorageArray<T> GetArray<T>(string fieldName) where T : unmanaged => StorageArray<T>.CreateView(_container, Schema.IndexOf(fieldName));

        /// <summary>
        /// Get a stack-only view over a child reference array (IDs).
        /// Field must be a ref field.
        /// </summary>
        public StorageObjectArray GetObjectArray(string fieldName)
        {
            var f = _container.Schema.GetField(fieldName);
            return new StorageObjectArray(_container, f);
        }

        /// <summary>
        /// Get a scala value view
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public ValueView GetValueView(string fieldName) => _container.GetValueView(fieldName);






        /// <summary>Check a field exist.</summary>
        public bool HasField(string fieldName) => _container.Schema.IndexOf(fieldName) >= 0;

        /// <summary>
        /// Rescheme the container to match the target schema.
        /// </summary>
        /// <param name="target"></param>
        public void Rescheme(Schema target) => _container.Rescheme(target);





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

