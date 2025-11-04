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
        public readonly Schema Schema => _container.Schema;
        public ulong ID => _container.ID;
        public bool IsNull => _container == null || _container._id == 0;
        public ReadOnlySpan<byte> HeaderHints => _container.HeaderHints;
        public StorageField this[string name] => GetField(name);



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
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        public void Write<T>(string fieldName, in T value) where T : unmanaged => _container.Write(fieldName, value);

        /// <summary>
        /// Write a value to an existing field without rescheming, if the field does not exist, an exception is thrown.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        public void WriteNoRescheme<T>(string fieldName, in T value) where T : unmanaged => _container.WriteNoRescheme(fieldName, value);

        public T Read<T>(string fieldName) where T : unmanaged => _container.Read<T>(fieldName);

        public bool TryRead<T>(string fieldName, out T value) where T : unmanaged => _container.TryRead(fieldName, out value);

        public T ReadOrDefault<T>(string fieldName) where T : unmanaged => _container.TryRead(fieldName, out T value) ? value : default;

        public T ReadOrDefault<T>(string fieldName, T defaultValue) where T : unmanaged => _container.TryRead(fieldName, out T value) ? value : defaultValue;


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



        // Child navigation by reference field (single)
        public StorageObject GetObject(string fieldName, bool reschemeOnMissing, bool allocateOnNull)
        {
            ref ulong idRef = ref reschemeOnMissing ? ref _container.GetRef(fieldName) : ref _container.GetRefNoRescheme(fieldName);
            return allocateOnNull ? StorageFactory.Get(ref idRef, Schema.Empty) : StorageFactory.GetNoAllocate(idRef);
        }

        // Child navigation by reference field (single)
        public StorageObject GetObject(string fieldName) => GetObject(fieldName, reschemeOnMissing: true, allocateOnNull: true);

        // Child navigation by reference field (single)
        public StorageObject GetObjectNoAllocate(string fieldName) => GetObject(fieldName, reschemeOnMissing: true, allocateOnNull: false);

        /// <summary>
        /// Get a stack-only view over a value array field T[].
        /// Field must be non-ref and length divisible by sizeof(T).
        /// </summary>
        public StorageArray<T> GetArray<T>(string fieldName) where T : unmanaged => StorageArray<T>.CreateView(_container, fieldName);

        /// <summary>
        /// Get a stack-only view over a child reference array (IDs).
        /// Field must be a ref field.
        /// </summary>
        public StorageObjectArray GetObjectArray(string fieldName)
        {
            var f = _container.Schema.GetField(fieldName);
            return new StorageObjectArray(_container, f);
        }





        /// <summary>Get a field view by name.</summary>
        public StorageField GetField(string fieldName)
        {
            var f = _container.Schema.GetField(fieldName);
            return new StorageField(_container, f);
        }

        /// <summary>Try get a field view by name.</summary>
        public bool TryGetField(string fieldName, out StorageField field)
        {
            if (_container.Schema.TryGetField(fieldName, out var f))
            {
                field = new StorageField(_container, f);
                return true;
            }
            field = default;
            return false;
        }






        public static bool operator ==(StorageObject left, StorageObject right) => left._container == right._container;
        public static bool operator !=(StorageObject left, StorageObject right) => !(left == right);
        public override int GetHashCode() => _container.GetHashCode();
        public override bool Equals(object obj) => false;
    }
}

