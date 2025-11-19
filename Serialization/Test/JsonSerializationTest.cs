using Minerva.DataStorage.Tests;
using NUnit.Framework;
using Unity.Serialization.Json;

namespace Minerva.DataStorage.Serialization.Tests
{
    [TestFixture]
    public class JsonSerializationTest : JsonSerializationTestsBase
    {
        private static Unity.Serialization.Json.JsonSerializationParameters ParamsWithAdapter() => new Unity.Serialization.Json.JsonSerializationParameters
        {
            UserDefinedAdapters = new System.Collections.Generic.List<IJsonAdapter>
            {
                new StorageAdapter()
            }
        };

        public override Storage Parse(string json)
        {
            return Unity.Serialization.Json.JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
        }
    }
}
