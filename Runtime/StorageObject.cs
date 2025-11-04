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


        internal StorageObject(Container container)
        {
            _container = container;
        }

        public readonly Schema Schema => _container.Schema;
        public ulong ID => _container.ID;
        public bool IsNull => _container == null || _container._id == 0;



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
        public void Write<T>(string fieldName, in T value) where T : unmanaged
        {
            EnsureNotNull();
            // if field not exists, add it
            if (!Schema.TryGetField(fieldName, out var field))
            {
                _container.Rescheme(b => b.AddFieldOf<T>(fieldName));
                field = _container.Schema.GetField(fieldName);
            }
            _container.Write(field, value);
        }

        /// <summary>
        /// Write a value to an existing field without rescheming, if the field does not exist, an exception is thrown.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="value"></param>
        public void WriteNoRescheme<T>(string fieldName, in T value) where T : unmanaged
        {
            EnsureNotNull();
            _container.Write(fieldName, value);
        }

        public T Read<T>(string fieldName) where T : unmanaged
        {
            EnsureNotNull();
            return _container.Read<T>(fieldName);
        }

        public bool TryRead<T>(string fieldName, out T value) where T : unmanaged
        {
            EnsureNotNull();
            return _container.TryRead(fieldName, out value);
        }

        /// <summary>
        /// Delete a field from this object. Returns true if the field was found and deleted, false otherwise.
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public bool Delete(string fieldName)
        {
            EnsureNotNull();
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

        // Child navigation by reference field (single)
        public StorageObject GetObject(string fieldName)
        {
            ulong id = _container.GetRef(fieldName);
            return Get(id);
        }

        /// <summary>
        /// Get a stack-only view over a value array field T[].
        /// Field must be non-ref and length divisible by sizeof(T).
        /// </summary>
        public StorageArray<T> GetArray<T>(string fieldName) where T : unmanaged
        {
            var f = _container.Schema.GetField(fieldName);
            return new StorageArray<T>(_container, f);
        }

        /// <summary>
        /// Get a stack-only view over a child reference array (IDs).
        /// Field must be a ref field.
        /// </summary>
        public StorageObjectArray GetObjectArray(string fieldName)
        {
            var f = _container.Schema.GetField(fieldName);
            return new StorageObjectArray(_container, f);
        }





        internal static StorageObject Get(ulong position)
        {
            var id = position;
            if (id == 0UL) return default; // null-like

            var child = Container.Registry.Shared.GetContainer(id);
            if (child is null) return default; // dangling -> treat as null

            return new StorageObject(child);
        }

        internal static StorageObject GetOrNew(ref ulong position, Schema schema)
        {
            ref var id = ref position;
            var child = Container.Registry.Shared.GetContainer(id);
            if (child is null)
            {
                Container.CreateAt(ref position, schema ?? Schema.Empty);
                child = Container.Registry.Shared.GetContainer(id);
            }
            return new StorageObject(child);
        }

        internal static bool TryGet(ulong position, out StorageObject obj)
        {
            obj = default;

            if (position == 0UL) return false;
            var c = Container.Registry.Shared.GetContainer(position);
            if (c == null) return false;
            obj = new StorageObject(c);
            return true;
        }




        public static bool operator ==(StorageObject left, StorageObject right) => left._container == right._container;
        public static bool operator !=(StorageObject left, StorageObject right) => !(left == right);
        public override int GetHashCode() => _container.GetHashCode();
        public override bool Equals(object obj) => false;
    }
}

