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
            get => new StorageObject(this).ReadOrDefault<int>(key);
            set => new StorageObject(this).Write<int>(key, value, true);
        }
        long IAccessor<long>.this[string key]
        {
            get => new StorageObject(this).ReadOrDefault<long>(key);
            set => new StorageObject(this).Write<long>(key, value);
        }
        float IAccessor<float>.this[string key]
        {
            get => new StorageObject(this).ReadOrDefault<float>(key);
            set => new StorageObject(this).Write<float>(key, value);
        }
        double IAccessor<double>.this[string key]
        {
            get => new StorageObject(this).ReadOrDefault<double>(key);
            set => new StorageObject(this).Write<double>(key, value);
        }
        string IStringAccessor.this[string key]
        {
            get => new StorageObject(this).TryReadString(key, out var str) ? str : string.Empty;
            set => new StorageObject(this).WriteString(key, value);
        }
    }
}
