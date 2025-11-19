using NUnit.Framework;
using System;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage.Serialization.Tests
{
    public abstract class JsonSerializationTestsBase
    {
        public abstract Storage Parse(string json);

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

            var deserialized = Parse(json);
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
            var json = storage.ToJson().ToString();
            storage.Dispose();

            var deserialized = Parse(json);
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

            var json = storage.ToJson().ToString();
            storage.Dispose();

            var deserialized = Parse(json);
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

            var deserialized = Parse(json);
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
            var storage = Parse(json);
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

            var storage2 = Parse(jsonRoundTrip);
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

            var storage = Parse(json);
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


            var storage2 = Parse(json2);
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

            var storage = Parse(json);
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

            var storage2 = Parse(json2);
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

            var storage = Parse(json);
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

            var storage2 = Parse(json2);
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

            var storage = Parse(json);
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

            var storage2 = Parse(json2);
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
                var storage = Parse(json);
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
            Assert.Catch<Exception>(() =>
            {
                Parse(invalidJson);
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

                var parsed = Parse(json);
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
                Parse(invalidRoot);
            });

            // Follow-up: simple valid parse still works.
            var storage = new Storage();
            var root = storage.Root;
            root.Write("Value", 42);

            var json = storage.ToJson().ToString();
            storage.Dispose();

            var parsed = Parse(json);
            var root2 = parsed.Root;
            Assert.AreEqual(42, root2.Read<int>("Value"));

            parsed.Dispose();
        }

        /// <summary>
        /// Application-level wrapper aroundParse should be able to
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
                    storage = Parse(invalidJson);
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

            var parsed = Parse(json);
            Assert.AreEqual(10, parsed.Root.Read<int>("Health"));
            parsed.Dispose();
        }




        /// <summary>
        /// Simple custom struct that should be stored as a blob (ValueType.Blob).
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        private struct TestBlob
        {
            public int A;
            public float B;
            public long C;
        }

        /// <summary>
        /// Round-trip a single custom struct field through Storage.ToJson +Parse.
        /// Verifies that blob-backed fields preserve their raw bytes and can be re-hydrated as T.
        /// </summary>
        [Test]
        public void RoundTrip_CustomStructBlob_SingleField()
        {
            var original = new TestBlob
            {
                A = 123,
                B = 4.5f,
                C = -9876543210L
            };

            var storage = new Storage();
            var root = storage.Root;

            // Write custom struct -> should be stored as a blob under the hood.
            root.Write("BlobField", original);

            var json = storage.ToJson().ToString();
            Log(json);

            storage.Dispose();

            // Parse JSON back into a new Storage tree.
            var deserialized = Parse(json);
            var root2 = deserialized.Root;

            var roundTripped = root2.Read<TestBlob>("BlobField");

            Assert.AreEqual(original.A, roundTripped.A, "Field A mismatch after blob round-trip.");
            Assert.AreEqual(original.B, roundTripped.B, "Field B mismatch after blob round-trip.");
            Assert.AreEqual(original.C, roundTripped.C, "Field C mismatch after blob round-trip.");

            deserialized.Dispose();
        }
        /// <summary>
        /// Round-trip an inline array of custom structs via WriteArray + ReadArray.
        /// Each element should be serialized as a blob element and restored bit-exact.
        /// </summary>
        [Test]
        public void RoundTrip_CustomStructBlob_InlineArray()
        {
            var blobs = new[]
            {
                new TestBlob { A = 1, B = 1.5f, C = 10 },
                new TestBlob { A = 2, B = -3.25f, C = -20 },
                new TestBlob { A = 3, B = 100.0f, C = 9999999L }
            };

            var storage = new Storage();
            var root = storage.Root;

            // Use the same pattern as the integer array test:
            // child container backing an inline array.
            root.WriteArray<TestBlob>("Blobs", blobs);

            var json = storage.ToJson().ToString();
            Log(json);

            storage.Dispose();

            var deserialized = Parse(json);
            var root2 = deserialized.Root;

            var blobs2 = root2.ReadArray<TestBlob>("Blobs".AsSpan());

            Assert.AreEqual(blobs.Length, blobs2.Length, "Blob array length mismatch.");
            for (int i = 0; i < blobs.Length; i++)
            {
                Assert.AreEqual(blobs[i].A, blobs2[i].A, $"Blob[{i}].A mismatch.");
                Assert.AreEqual(blobs[i].B, blobs2[i].B, $"Blob[{i}].B mismatch.");
                Assert.AreEqual(blobs[i].C, blobs2[i].C, $"Blob[{i}].C mismatch.");
            }

            deserialized.Dispose();
        }
        /// <summary>
        /// Round-trip a custom struct stored inside a child object:
        /// Root -> "Node" (object) -> "State" (blob-backed custom struct).
        /// Ensures recursion + blob are both handled correctly.
        /// </summary>
        [Test]
        public void RoundTrip_CustomStructBlob_NestedInChildObject()
        {
            var state = new TestBlob
            {
                A = 42,
                B = 0.75f,
                C = 123456789L
            };

            var storage = new Storage();
            var root = storage.Root;

            var node = root.GetObject("Node");
            node.Write("State", state);

            var json = storage.ToJson().ToString();
            Log(json);

            storage.Dispose();

            var deserialized = Parse(json);
            var root2 = deserialized.Root;

            var node2 = root2.GetObject("Node");
            var state2 = node2.Read<TestBlob>("State");

            Assert.AreEqual(state.A, state2.A, "Nested blob field A mismatch.");
            Assert.AreEqual(state.B, state2.B, "Nested blob field B mismatch.");
            Assert.AreEqual(state.C, state2.C, "Nested blob field C mismatch.");

            deserialized.Dispose();
        }

        /// <summary>
        /// Corrupted blob payload (e.g., invalid base64 for a blob field) should
        /// raise a controlled exception during parse and must not leave global
        /// state or the registry in a broken state.
        /// </summary>
        [Test]
        public void Parse_CorruptedBlobPayload_ShouldThrow_AndNotBreakLaterParses()
        {
            // Build a valid blob first.
            var storage = new Storage();
            var root = storage.Root;

            var blob = new TestBlob { A = 7, B = -1.25f, C = 123 };
            root.Write("BlobField", blob);

            var json = storage.ToJson().ToString();
            storage.Dispose();

            // Locate the JSON string value of "BlobField" and inject an invalid
            // character into the base64 payload to guarantee corruption.
            const string blobMarker = "\"$blob\":\"";
            int markerIndex = json.IndexOf(blobMarker, StringComparison.Ordinal);
            Assert.GreaterOrEqual(markerIndex, 0, "Blob marker not found.");

            int valueStart = markerIndex + blobMarker.Length;
            int valueEnd = json.IndexOf('"', valueStart);
            Assert.Greater(valueEnd, valueStart, "Closing quote for $blob value not found.");

            string originalBase64 = json.Substring(valueStart, valueEnd - valueStart);
            string corruptedBase64 = originalBase64 + "!";

            string corruptedJson =
                json.Substring(0, valueStart) +
                corruptedBase64 +
                json.Substring(valueEnd);

            // Parse must throw (invalid base64 inside a blob field).
            Assert.Throws<InvalidOperationException>(() =>
            {
                Log(corruptedJson);
                var s = Parse(corruptedJson);
                Log(s.ToJson().ToString());
                s.Dispose();
            });

            // After the failure, a clean round-trip must still work.
            var storage2 = new Storage();
            var root2 = storage2.Root;
            root2.Write("Health", 10);
            var json2 = storage2.ToJson().ToString();
            storage2.Dispose();

            var parsed2 = Parse(json2);
            Assert.AreEqual(10, parsed2.Root.Read<int>("Health"));
            parsed2.Dispose();
        }




        /// <summary>
        /// Round-trip an empty root object: Storage with no fields should
        /// serialize to "{}" and parse back to a root with FieldCount == 0.
        /// </summary>
        [Test]
        public void RoundTrip_EmptyRootObject_FromStorage()
        {
            var storage = new Storage();
            var root = storage.Root;

            Assert.AreEqual(0, root.FieldCount, "New Storage.Root should start empty.");

            var json = storage.ToJson().ToString();
            Log(json);
            storage.Dispose();

            var parsed = Parse(json);
            var root2 = parsed.Root;

            Assert.AreEqual(0, root2.FieldCount, "Empty root should round-trip without injecting fields.");
            parsed.Dispose();
        }

        /// <summary>
        /// Round-trip a mix of primitive scalar fields: bool, int, long, float, string.
        /// Ensures that different primitive value kinds all survive ToJson + Parse.
        /// </summary>
        [Test]
        public void RoundTrip_MixedPrimitiveFields_FromStorage()
        {
            var storage = new Storage();
            var root = storage.Root;

            root.Write("Alive", true);
            root.Write("Level", 7);
            root.Write("Score", 1234567890123L);
            root.Write("Speed", 3.5f);
            root.WriteString("Title", "Knight");

            var json = storage.ToJson().ToString();
            Log(json);
            storage.Dispose();

            var parsed = Parse(json);
            var root2 = parsed.Root;

            Assert.AreEqual(true, root2.Read<bool>("Alive"));
            Assert.AreEqual(7, root2.Read<int>("Level"));
            Assert.AreEqual(1234567890123L, root2.Read<long>("Score"));
            Assert.AreEqual(3.5f, root2.Read<float>("Speed"));
            Assert.AreEqual("Knight", root2.ReadString("Title"));

            parsed.Dispose();
        }

        /// <summary>
        /// Round-trip string fields that contain JSON escape sequences and Unicode.
        /// Ensures that quotes, backslashes, newlines, and non-ASCII characters are
        /// preserved across ToJson + Parse.
        /// </summary>
        [Test]
        public void RoundTrip_StringEscapes_AndUnicode()
        {
            var storage = new Storage();
            var root = storage.Root;

            string escaped = "He said: \"Hello\\World\"\nNext line\tTabbed";
            string unicode = "雪夜の図 ❄️😊";

            root.WriteString("Escaped", escaped);
            root.WriteString("Unicode", unicode);

            var json = storage.ToJson().ToString();
            Log(json);
            storage.Dispose();

            var parsed = Parse(json);
            var root2 = parsed.Root;

            Assert.AreEqual(escaped, root2.ReadString("Escaped"), "Escaped string did not round-trip correctly.");
            Assert.AreEqual(unicode, root2.ReadString("Unicode"), "Unicode string did not round-trip correctly.");

            parsed.Dispose();
        }

        /// <summary>
        /// Round-trip a root that mixes scalar fields with an inline numeric array.
        /// Verifies that arrays do not interfere with sibling scalar fields.
        /// </summary>
        [Test]
        public void RoundTrip_Scalars_WithInlineArray()
        {
            var storage = new Storage();
            var root = storage.Root;

            root.Write("Id", 1001);
            root.WriteString("Name", "Hero");
            int[] stats = { 10, 20, 30 };
            root.WriteArray<int>("Stats".AsSpan(), stats.AsSpan());

            var json = storage.ToJson().ToString();
            Log(json);
            storage.Dispose();

            var parsed = Parse(json);
            var root2 = parsed.Root;

            Assert.AreEqual(1001, root2.Read<int>("Id"));
            Assert.AreEqual("Hero", root2.ReadString("Name"));

            var stats2 = root2.ReadArray<int>("Stats".AsSpan());
            CollectionAssert.AreEqual(stats, stats2, "Inline array Stats did not round-trip correctly.");

            parsed.Dispose();
        }

        /// <summary>
        /// Round-trip a custom struct blob alongside regular scalar fields
        /// within the same object. Ensures mixed blob + scalar layout is stable
        /// across ToJson + Parse.
        /// </summary>
        [Test]
        public void RoundTrip_CustomStructBlob_WithScalarSiblings()
        {
            var state = new TestBlob
            {
                A = 9,
                B = -0.5f,
                C = 42
            };

            var storage = new Storage();
            var root = storage.Root;

            root.Write("Id", 123);
            root.WriteString("Name", "NodeWithBlob");
            root.Write("State", state);

            var json = storage.ToJson().ToString();
            Log(json);
            storage.Dispose();

            var parsed = Parse(json);
            var root2 = parsed.Root;
            Log(parsed.ToJson().ToString());

            Assert.AreEqual(123, root2.Read<int>("Id"));
            Assert.AreEqual("NodeWithBlob", root2.ReadString("Name"));

            var state2 = root2.Read<TestBlob>("State");
            Log(state2.A.ToString());
            Log(state2.B.ToString());
            Log(state2.C.ToString());
            Assert.AreEqual(state.A, state2.A, "Blob field A mismatch in mixed blob+scalar object.");
            Assert.AreEqual(state.B, state2.B, "Blob field B mismatch in mixed blob+scalar object.");
            Assert.AreEqual(state.C, state2.C, "Blob field C mismatch in mixed blob+scalar object.");

            parsed.Dispose();
        }

        /// <summary>
        /// Round-trip a more realistic "save-game"-style object that mixes
        /// nested objects and arrays in a single tree:
        /// Root:
        ///   Player (object)
        ///   Inventory (inline numeric array)
        ///   Settings (object)
        /// </summary>
        [Test]
        public void RoundTrip_ComplexMixedTree_FromStorage()
        {
            var storage = new Storage();
            var root = storage.Root;

            // Player object with a few primitive fields
            var player = root.GetObject("Player");
            player.WriteString("Name", "Hero");
            player.Write("Level", 5);
            player.Write("Hp", 87.5f);

            // Inventory as an inline array of item IDs
            int[] inventory = { 101, 102, 103, 2001 };
            root.WriteArray<int>("Inventory".AsSpan(), inventory.AsSpan());

            // Settings as a child object with mixed primitives
            var settings = root.GetObject("Settings");
            settings.Write("MusicVolume", 80);
            settings.Write("SfxVolume", 60);
            settings.Write("FullScreen", true);

            var json = storage.ToJson().ToString();
            Log(json);
            storage.Dispose();

            var parsed = Parse(json);
            var root2 = parsed.Root;

            var player2 = root2.GetObject("Player");
            Assert.AreEqual("Hero", player2.ReadString("Name"));
            Assert.AreEqual(5, player2.Read<int>("Level"));
            Assert.AreEqual(87.5f, player2.Read<float>("Hp"));

            var inventory2 = root2.ReadArray<int>("Inventory".AsSpan());
            CollectionAssert.AreEqual(inventory, inventory2);

            var settings2 = root2.GetObject("Settings");
            Assert.AreEqual(80, settings2.Read<int>("MusicVolume"));
            Assert.AreEqual(60, settings2.Read<int>("SfxVolume"));
            Assert.AreEqual(true, settings2.Read<bool>("FullScreen"));

            parsed.Dispose();
        }








        private void Log(string v)
        {
#if UNITY_EDITOR
            UnityEngine.Debug.Log(v);
#endif
        }
    }
}
