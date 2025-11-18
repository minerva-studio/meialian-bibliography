using System;
using Minerva.DataStorage.Serialization;
using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class JsonSerializationTests
    {
        /// <summary>
        /// Simple round-trip with only scalar values and a string.
        /// Verifies that basic fields survive ToBinary + Parse.
        /// </summary>
        [Test]
        public void RoundTrip_SimpleScalarAndString()
        {
            var storage = new Storage();
            var root = storage.Root;

            root.Write("Health", 100);
            root.Write("Mana", 50f);
            root.WriteString("Name", "Hero");

            var json = storage.ToJson().ToString();
            storage.Dispose();   // Dispose original tree after serialization

            var deserialized = JsonSerialization.Parse(json);
            var root2 = deserialized.Root;

            Log(json);
            Log(deserialized.ToJson().ToString());

            Assert.AreEqual(100, root2.Read<int>("Health"));
            Assert.AreEqual(50f, root2.Read<float>("Mana"));
            Assert.AreEqual("Hero", root2.ReadString("Name"));

            // Root ID after parse should be valid and different from the original one
            Assert.AreNotEqual(0UL, root2.ID);

            deserialized.Dispose();
        }

        /// <summary>
        /// Round-trip with a single child object referenced from the root.
        /// Exercises the recursive WriteBinaryTo / ReadContainer path.
        /// </summary>
        [Test]
        public void RoundTrip_WithSingleChild()
        {
            var storage = new Storage();
            var root = storage.Root;

            root.Write("Id", 1);

            // Create a child object via a ref field on the root
            var child = root.GetObject("Child");
            child.Write("X", 10f);
            child.Write("Y", 20f);

            var originalRootId = root.ID;
            var json = storage.ToJson();
            storage.Dispose();

            var deserialized = JsonSerialization.Parse(json);
            var root2 = deserialized.Root;

            Assert.AreEqual(1, root2.Read<int>("Id"));

            var child2 = root2.GetObject("Child");
            Assert.AreEqual(10f, child2.Read<float>("X"));
            Assert.AreEqual(20f, child2.Read<float>("Y"));

            // Parsed tree should have a different root ID to avoid registry conflicts
            Assert.AreNotEqual(originalRootId, root2.ID);

            deserialized.Dispose();
        }

        /// <summary>
        /// Round-trip with a deeper hierarchy: Root -> Child -> GrandChild.
        /// Ensures multiple levels of nested containers are serialized correctly.
        /// </summary>
        [Test]
        public void RoundTrip_WithDeepHierarchy()
        {
            var storage = new Storage();
            var root = storage.Root;

            root.Write("RootValue", 42);

            var child = root.GetObject("Child");
            child.Write("ChildValue", 99);

            var grandChild = child.GetObject("GrandChild");
            grandChild.Write("GrandValue", -123);

            var json = storage.ToJson();
            storage.Dispose();

            var deserialized = JsonSerialization.Parse(json);
            var root2 = deserialized.Root;

            Assert.AreEqual(42, root2.Read<int>("RootValue"));

            var child2 = root2.GetObject("Child");
            Assert.AreEqual(99, child2.Read<int>("ChildValue"));

            var grandChild2 = child2.GetObject("GrandChild");
            Assert.AreEqual(-123, grandChild2.Read<int>("GrandValue"));

            deserialized.Dispose();
        }

        /// <summary>
        /// Round-trip with a value array stored in a child container.
        /// Assumes WriteArray(fieldName, values) and ReadArray<T>(fieldName)
        /// are implemented via child containers.
        /// </summary>
        [Test]
        public void RoundTrip_WithValueArray()
        {
            var storage = new Storage();
            var root = storage.Root;

            int[] values = { 1, 2, 3, 4, 5 };

            // Write an inline value array field: child container backing the array
            root.WriteArray<int>("Values".AsSpan(), values.AsSpan());

            var json = storage.ToJson().ToString();
            storage.Dispose();

            var deserialized = JsonSerialization.Parse(json);
            var root2 = deserialized.Root;

            Log(json);
            Log(deserialized.ToJson().ToString());

            var values2 = root2.ReadArray<int>("Values".AsSpan());
            CollectionAssert.AreEqual(values, values2);

            deserialized.Dispose();
        }
        /// <summary>
        /// Round-trip for an array-of-objects: [{...}, {...}].
        /// Ensures arrays of JSON objects are represented as a reference array
        /// (StorageObjectArray) and preserve their scalar fields across Parse + ToJson + Parse.
        /// </summary>
        [Test]
        public void RoundTrip_ArrayOfObjects_FromJson()
        {
            const string json = "{\"Enemies\":[{\"Id\":1,\"Hp\":10},{\"Id\":2,\"Hp\":20}]}";

            // JSON -> Storage
            var storage = JsonSerialization.Parse(json);
            var root = storage.Root;

            var enemies = root.GetArray("Enemies");
            Assert.AreEqual(2, enemies.Length);

            var e0 = enemies.GetObject(0);
            var e1 = enemies.GetObject(1);

            Assert.AreEqual(1L, e0.Read<long>("Id"));
            Assert.AreEqual(10L, e0.Read<long>("Hp"));
            Assert.AreEqual(2L, e1.Read<long>("Id"));
            Assert.AreEqual(20L, e1.Read<long>("Hp"));

            // Storage -> JSON -> Storage again
            var jsonRoundTrip = storage.ToJson().ToString();
            Log(json);
            Log(jsonRoundTrip);

            var storage2 = JsonSerialization.Parse(jsonRoundTrip);
            var root2 = storage2.Root;
            var enemies2 = root2.GetArray("Enemies");
            Assert.AreEqual(2, enemies2.Length);

            var e0b = enemies2.GetObject(0);
            var e1b = enemies2.GetObject(1);

            Assert.AreEqual(1L, e0b.Read<long>("Id"));
            Assert.AreEqual(10L, e0b.Read<long>("Hp"));
            Assert.AreEqual(2L, e1b.Read<long>("Id"));
            Assert.AreEqual(20L, e1b.Read<long>("Hp"));

            storage.Dispose();
            storage2.Dispose();
        }

        /// <summary>
        /// Round-trip for an array-of-arrays of numbers: [[1,2],[3,4,5]].
        /// Top-level field should be a reference array, each element an "array container"
        /// that reports IsArray == true and exposes an inline numeric array.
        /// </summary>
        [Test]
        public void RoundTrip_ArrayOfArrays_OfNumbers_FromJson()
        {
            const string json = "{\"Nested\":[[1,2],[3,4,5]]}";

            var storage = JsonSerialization.Parse(json);
            var root = storage.Root;
            var json2 = storage.ToJson().ToString();
            Log(json);
            Log(json2);

            var nested = root.GetArray("Nested");
            Assert.AreEqual(2, nested.Length);

            var a0 = nested.GetObject(0);
            var a1 = nested.GetObject(1);

            Assert.IsTrue(a0.IsArray);
            Assert.IsTrue(a1.IsArray);

            var v0 = a0.ReadArray<long>();
            var v1 = a1.ReadArray<long>();

            CollectionAssert.AreEqual(new long[] { 1, 2 }, v0);
            CollectionAssert.AreEqual(new long[] { 3, 4, 5 }, v1);


            var storage2 = JsonSerialization.Parse(json2);
            var root2 = storage2.Root;
            var nested2 = root2.GetArray("Nested");

            Assert.AreEqual(2, nested2.Length);
            var v0b = nested2.GetObject(0).ReadArray<long>();
            var v1b = nested2.GetObject(1).ReadArray<long>();

            CollectionAssert.AreEqual(new long[] { 1, 2 }, v0b);
            CollectionAssert.AreEqual(new long[] { 3, 4, 5 }, v1b);

            storage.Dispose();
            storage2.Dispose();
        }

        /// <summary>
        /// Round-trip for an array-of-empty-arrays: [[],[]].
        /// This mainly checks that the parser can handle nested empty arrays
        /// and that serialization does not crash or corrupt structure.
        /// 
        /// Implementation detail: each inner empty array may be represented
        /// as an "array container" with a zero-length inline array; we only
        /// assert that the containers exist and expose empty arrays of bytes.
        /// </summary>
        [Test]
        public void RoundTrip_ArrayOfEmptyArrays_FromJson()
        {
            const string json = "{\"Nested\":[[],[]]}";

            var storage = JsonSerialization.Parse(json);
            var root = storage.Root;

            var nested = root.GetArray("Nested");
            Assert.AreEqual(2, nested.Length);

            var a0 = nested.GetObject(0);
            var a1 = nested.GetObject(1);

            Assert.IsTrue(a0.IsArray);
            Assert.IsTrue(a1.IsArray);

            // We do not assume a specific numeric type for empty arrays; byte[] is a safe fallback.
            var e0 = a0.ReadArray<byte>();
            var e1 = a1.ReadArray<byte>();
            Assert.AreEqual(0, e0.Length);
            Assert.AreEqual(0, e1.Length);

            var json2 = storage.ToJson().ToString();
            Log(json);
            Log(json2);

            var storage2 = JsonSerialization.Parse(json2);
            var root2 = storage2.Root;
            var nested2 = root2.GetArray("Nested");

            Assert.AreEqual(2, nested2.Length);
            Assert.IsTrue(nested2.GetObject(0).IsArray);
            Assert.IsTrue(nested2.GetObject(1).IsArray);

            storage.Dispose();
            storage2.Dispose();
        }

        /// <summary>
        /// Round-trip for an array-of-objects with empty bodies: [{}, {}].
        /// Validates that ref-arrays of empty objects are handled and round-trip
        /// does not inject spurious fields.
        /// </summary>
        [Test]
        public void RoundTrip_ArrayOfEmptyObjects_FromJson()
        {
            const string json = "{\"Items\":[{},{}]}";

            var storage = JsonSerialization.Parse(json);
            var root = storage.Root;

            var items = root.GetArray("Items");
            Assert.AreEqual(2, items.Length);

            var i0 = items.GetObject(0);
            var i1 = items.GetObject(1);

            Assert.AreEqual(0, i0.FieldCount);
            Assert.AreEqual(0, i1.FieldCount);

            var json2 = storage.ToJson().ToString();
            Log(json);
            Log(json2);

            var storage2 = JsonSerialization.Parse(json2);
            var root2 = storage2.Root;
            var items2 = root2.GetArray("Items");

            Assert.AreEqual(2, items2.Length);
            Assert.AreEqual(0, items2.GetObject(0).FieldCount);
            Assert.AreEqual(0, items2.GetObject(1).FieldCount);

            storage.Dispose();
            storage2.Dispose();
        }

        /// <summary>
        /// Round-trip for an array-of-strings: ["Alice","Bob","Carol"].
        /// The JSON layer sees a single field "Names" that is an array of strings;
        /// the Storage layer should represent it as a reference array where each
        /// child container is a UTF-16 string container.
        /// </summary>
        [Test]
        public void RoundTrip_ArrayOfStrings_FromJson()
        {
            const string json = "{\"Names\":[\"Alice\",\"Bob\",\"Carol\"]}";
            string[] expected = { "Alice", "Bob", "Carol" };

            var storage = JsonSerialization.Parse(json);
            var root = storage.Root;

            var names = root.GetArray("Names");
            Assert.AreEqual(expected.Length, names.Length);

            for (int i = 0; i < expected.Length; i++)
            {
                var child = names.GetObject(i);
                string s = child.ReadString();
                Assert.AreEqual(expected[i], s);
            }

            var json2 = storage.ToJson().ToString();
            Log(json);
            Log(json2);

            var storage2 = JsonSerialization.Parse(json2);
            var root2 = storage2.Root;
            var names2 = root2.GetArray("Names");
            Assert.AreEqual(expected.Length, names2.Length);

            for (int i = 0; i < expected.Length; i++)
            {
                var child = names2.GetObject(i);
                string s = child.ReadString();
                Assert.AreEqual(expected[i], s);
            }

            storage.Dispose();
            storage2.Dispose();
        }

        /// <summary>
        /// Mixed-type arrays such as [1, "x"] should be rejected by the parser.
        /// This protects the Storage schema from ambiguous or heterogeneous array types.
        /// </summary>
        [Test]
        public void Parse_MixedTypeArray_ShouldThrow()
        {
            const string json = "{\"Mixed\":[1,\"x\"]}";

            Assert.Throws<InvalidOperationException>(() =>
            {
                var storage = JsonSerialization.Parse(json);
                storage.Dispose();
            });
        }




        /// <summary>
        /// Invalid JSON syntax (e.g., missing closing brace) should result in a well-defined
        /// exception and must not poison any global state. Subsequent Storage operations
        /// and JSON round-trips must still succeed.
        /// </summary>
        [Test]
        public void Parse_InvalidJsonSyntax_ShouldThrow_AndAllowSubsequentRoundTrips()
        {
            // Missing closing brace and value after comma -> syntactically invalid.
            const string invalidJson = "{ \"Health\": 100, ";

            // 1) Bad JSON should throw a predictable exception, not crash the process.
            Assert.Throws<InvalidOperationException>(() =>
            {
                JsonSerialization.Parse(invalidJson);
            });

            // 2) After the failure, we should still be able to use Storage normally.
            for (int i = 0; i < 3; i++)
            {
                var storage = new Storage();
                var root = storage.Root;

                int expectedHealth = 100 + i;
                root.Write("Health", expectedHealth);
                root.Write("Mana", 50);

                var json = storage.ToJson().ToString();
                storage.Dispose();

                var parsed = JsonSerialization.Parse(json);
                var root2 = parsed.Root;

                Assert.AreEqual(expectedHealth, root2.Read<int>("Health"));
                Assert.AreEqual(50, root2.Read<int>("Mana"));

                parsed.Dispose();
            }
        }



        /// <summary>
        /// A JSON root that is not an object (e.g., an array) should be rejected.
        /// This tests that such an error does not leave the registry or global state
        /// in a broken state for subsequent parses.
        /// </summary>
        [Test]
        public void Parse_RootArray_ShouldThrow_AndNotBreakLaterParses()
        {
            const string invalidRoot = "[1,2,3]"; // root must be an object in our format

            Assert.Throws<InvalidOperationException>(() =>
            {
                JsonSerialization.Parse(invalidRoot);
            });

            // Follow-up: simple valid parse still works.
            var storage = new Storage();
            var root = storage.Root;
            root.Write("Value", 42);

            var json = storage.ToJson().ToString();
            storage.Dispose();

            var parsed = JsonSerialization.Parse(json);
            var root2 = parsed.Root;
            Assert.AreEqual(42, root2.Read<int>("Value"));

            parsed.Dispose();
        }

        /// <summary>
        /// Application-level wrapper around JsonSerialization.Parse should be able to
        /// catch and handle parse errors without propagating them. This verifies that
        /// bad JSON does not cause uncaught exceptions that would crash the host.
        /// </summary>
        [Test]
        public void Parse_InvalidJson_WrappedHandler_ShouldNotThrow()
        {
            const string invalidJson = "{ \"Health\": 100,, }"; // double comma

            Storage storage = null;

            // The wrapper is responsible for catching the parse exception.
            Assert.DoesNotThrow(() =>
            {
                try
                {
                    storage = JsonSerialization.Parse(invalidJson);
                }
                catch (Exception)
                {
                    // In real code you would log the error here.
                    storage = null;
                }
            });

            Assert.IsNull(storage, "Wrapper should return null on invalid JSON.");

            // Sanity check: after wrapper handling, we can still create and use Storage.
            var ok = new Storage();
            ok.Root.Write("Health", 10);
            var json = ok.ToJson().ToString();
            ok.Dispose();

            var parsed = JsonSerialization.Parse(json);
            Assert.AreEqual(10, parsed.Root.Read<int>("Health"));
            parsed.Dispose();
        }









        private void Log(string v)
        {
            UnityEngine.Debug.Log(v);
        }
    }
}
