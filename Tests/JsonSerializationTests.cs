using Minerva.DataStorage.Serialization;
using NUnit.Framework;

namespace Minerva.DataStorage.Tests
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
