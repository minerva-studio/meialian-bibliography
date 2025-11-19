using NUnit.Framework;

namespace Minerva.DataStorage.Serialization.Tests
{
    [TestFixture]
    public class JsonSerializationTest : JsonSerializationTestsBase
    {
        public override Storage Parse(string json)
        {
            return JsonSerialization.Parse(json);
        }
    }
}
