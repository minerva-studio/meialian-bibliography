using NUnit.Framework;
using System;
using Unity.Serialization.Json;

namespace Amlos.Container.Serialization.Tests
{
    [TestFixture]
    public class StorageSerializationTests
    {
        /// <summary>
        /// Build a schema: 
        ///   - hp: Int32 (scalar)
        ///   - children: Ref[3] (inline object array, all null in this test)
        ///   - speeds: Float32[4] (inline value array)
        /// Fill data and verify JSON shape.
        /// </summary>
        [Test]
        public void Serialize_SimpleRoot_HpChildrenSpeeds_CorrectJsonShape()
        {
            // --- Arrange: schema ---
            var schema = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .AddRefArray("children", 3)
                .AddArrayOf<float>("speeds", 4)
                .Build();

            // Create a storage + root object (adjust to your actual constructors).
            var storage = new Storage(schema);
            var root = storage.Root;

            // --- Arrange: write data ---
            // hp = 100
            // TODO: replace with your actual write API. Examples:
            // root.Write("hp", 100);
            // or: storage.Container.TryWrite(schema.GetField("hp"), 100);
            root.Write("hp", 100); // <--- replace if your API differs

            // speeds = [1.5, 3.33, 2.0, 74.0]
            // TODO: replace with your actual array write API.
            var speeds = new float[] { 1.5f, 3.33f, 2.0f, 74.0f };
            StorageInlineArray<float> storageArray = root.GetArray<float>("speeds");
            MemoryExtensions.CopyTo(speeds, storageArray.AsSpan());

            var parameters = new JsonSerializationParameters
            {
                UserDefinedAdapters = new System.Collections.Generic.List<IJsonAdapter>
                {
                    new StorageAdapter()
                }
            };
            string json = JsonSerialization.ToJson(storage, parameters);

            // --- Assertions (robust to pretty printing) ---
            StringAssert.Contains("\"hp\":", json);
            StringAssert.Contains("100", json);

            StringAssert.Contains("\"speeds\":", json);
            StringAssert.Contains("1.5", json);
            StringAssert.Contains("3.33", json);
            StringAssert.Contains("74", json);

            StringAssert.Contains("\"children\":", json);
            // Count 'null' occurrences after the "children" key appears.
            var idx = json.IndexOf("\"children\":");
            Assert.That(idx, Is.GreaterThanOrEqualTo(0));
            var tail = json.Substring(idx);
            Assert.That(CountSubstring(tail, "null"), Is.GreaterThanOrEqualTo(3));

            StringAssert.StartsWith("{", json.Trim());
            StringAssert.EndsWith("}", json.Trim());
        }

        /// <summary>
        /// Helper to count a substring in a string (case-sensitive).
        /// </summary>
        private static int CountSubstring(string s, string needle)
        {
            int count = 0, idx = 0;
            while (true)
            {
                idx = s.IndexOf(needle, idx);
                if (idx < 0) break;
                count++;
                idx += needle.Length;
            }
            return count;
        }
    }
}
