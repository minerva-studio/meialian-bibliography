#if UNITY_EDITOR
using Minerva.DataStorage.Serialization.Tests;
using NUnit.Framework;
using Unity.Serialization.Json;

namespace Minerva.DataStorage.Serialization.Test.Unity
{
    [TestFixture]
    public class JsonSerializationTest : JsonSerializationTestsBase
    {
        private static JsonSerializationParameters ParamsWithAdapter() => new JsonSerializationParameters
        {
            UserDefinedAdapters = new System.Collections.Generic.List<IJsonAdapter>
            {
                new StorageAdapter()
            }
        };

        public override Storage Parse(string json)
        {
            return global::Unity.Serialization.Json.JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
        }
    }
}
#endif
