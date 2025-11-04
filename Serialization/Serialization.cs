using Unity.Serialization.Json;

namespace Amlos.Container.Serialization
{
    public class StorageAdapter : IJsonAdapter<Storage>
    {
        public Storage Deserialize(in JsonDeserializationContext<Storage> context)
        {
            throw new System.NotImplementedException();
        }

        public void Serialize(in JsonSerializationContext<Storage> context, Storage value)
        {
            throw new System.NotImplementedException();
        }
    }
}
