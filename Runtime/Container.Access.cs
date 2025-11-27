namespace Minerva.DataStorage
{
    internal partial class Container :
        IAccessor<int>,
        IAccessor<long>,
        IAccessor<float>,
        IAccessor<double>,
        IStringAccessor
    {
        int IAccessor<int>.this[string key]
        {
            get => new StorageObject(this).Read<int>(key, true);
            set => new StorageObject(this).Write<int>(key, value, true);
        }
        long IAccessor<long>.this[string key]
        {
            get => new StorageObject(this).Read<long>(key, true);
            set => new StorageObject(this).Write<long>(key, value);
        }
        float IAccessor<float>.this[string key]
        {
            get => new StorageObject(this).Read<float>(key, true);
            set => new StorageObject(this).Write<float>(key, value);
        }
        double IAccessor<double>.this[string key]
        {
            get => new StorageObject(this).Read<double>(key, true);
            set => new StorageObject(this).Write<double>(key, value);
        }
        string IStringAccessor.this[string key]
        {
            get => new StorageObject(this).ReadString(key);
            set => new StorageObject(this).WriteString(key, value);
        }
    }
}
