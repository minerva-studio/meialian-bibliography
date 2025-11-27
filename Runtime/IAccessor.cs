namespace Minerva.DataStorage
{
    public interface IAccessor<T> where T : unmanaged
    {
        T this[string key] { get; set; }
    }

    public interface IStringAccessor
    {
        string this[string key] { get; set; }
    }
}

