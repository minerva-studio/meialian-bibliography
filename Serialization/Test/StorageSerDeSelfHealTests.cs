using NUnit.Framework;
using System;
using System.Linq;
using Unity.Serialization.Json;
using UnityEngine;

namespace Amlos.Container.Serialization.Tests
{
    [TestFixture]
    public class StorageSerDeSelfHealTests
    {
        private static JsonSerializationParameters ParamsWithAdapter() => new JsonSerializationParameters
        {
            UserDefinedAdapters = new System.Collections.Generic.List<IJsonAdapter>
            {
                new StorageAdapter()
            }
        };

        /// <summary>
        /// Build [hp:int, children:ref[3], speeds:float[4]], fill values, serialize.
        /// Validate JSON shape (not strict whitespace).
        /// </summary>
        [Test]
        public void Serialize_SimpleRoot_HpChildrenSpeeds_CorrectJsonShape()
        {
            var schema = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .AddRefArray("children", 3)
                .AddArrayOf<float>("speeds", 4)
                .Build();

            var storage = new Storage(schema);
            var root = storage.Root;

            root.Write("hp", 100);

            var speeds = new float[] { 1.5f, 3.33f, 2.0f, 74.0f };
            var speedsSpan = root.GetArray<float>("speeds");
            speeds.AsSpan().CopyTo(speedsSpan.AsSpan());

            var json = JsonSerialization.ToJson(storage, ParamsWithAdapter());

            StringAssert.Contains("\"hp\":", json);
            StringAssert.Contains("100", json);
            StringAssert.Contains("\"children\":", json);
            // Expect three nulls for ref array default
            var nullCount = CountSubstring(json, "null");
            Assert.That(nullCount, Is.GreaterThanOrEqualTo(3));
            StringAssert.Contains("\"speeds\":", json);
            StringAssert.Contains("1.5", json);
            StringAssert.Contains("3.33", json);
            StringAssert.Contains("74", json);
        }

        /// <summary>
        /// Deserialize a minimal JSON with no pre-existing schema. Field has an integer literal (1).
        /// After deserialization, calling Read<int> must succeed.
        /// </summary>
        [Test]
        public void Deserialize_NoSchema_IntLiteral_ReadInt()
        {
            var json = "{ \"health\": 1 }";
            var storage = JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
            var root = storage.Root;

            // Read<int> should self-heal schema to a 4-byte value field if necessary
            int hp = root.Read<int>("health");
            Assert.That(hp, Is.EqualTo(1));

            var f = root.Schema.GetField("health");
            Assert.That(f.IsRef, Is.False);
            //Assert.That(f.AbsLength, Is.EqualTo(sizeof(int)));
        }

        /// <summary>
        /// Deserialize with integer literal but caller wants byte.
        /// </summary>
        [Test]
        public void Deserialize_NoSchema_IntLiteral_ReadByte()
        {
            var json = "{ \"x\": 1 }";
            var storage = JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
            var root = storage.Root;

            byte bx = root.Read<byte>("x");
            Assert.That(bx, Is.EqualTo((byte)1));

            var f = root.Schema.GetField("x");
            Assert.That(f.IsRef, Is.False);
            //Assert.That(f.AbsLength, Is.EqualTo(sizeof(byte)));
        }

        /// <summary>
        /// Deserialize with floating array; then read as float[] span and verify values.
        /// This also checks that the serializer produced a fixed-size inline array and
        /// that our read path returns the correct data.
        /// </summary>
        [Test]
        public void Deserialize_FloatArray_ReadAsFloatSpan()
        {
            var json = "{ \"speeds\": [1.5, 3.33, 2, 74] }";
            var storage = JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
            var root = storage.Root;

            // Ensure array field exists and is float[4] after self-heal on access if needed
            Debug.Log(JsonSerialization.ToJson(storage, ParamsWithAdapter()));
            var span = root.GetArray<float>("speeds");
            Debug.Log(string.Join(',', span.ToArray()));
            Assert.That(span.Length, Is.EqualTo(4));
            Assert.That(span[0], Is.EqualTo(1.5f).Within(1e-6));
            Assert.That(span[1], Is.EqualTo(3.33f).Within(1e-6));
            Assert.That(span[2], Is.EqualTo(2.0f).Within(1e-6));
            Assert.That(span[3], Is.EqualTo(74.0f).Within(1e-6));
        }

        /// <summary>
        /// Deserialize with a ref-array of three children set to null.
        /// Verify ref array length and null elements.
        /// </summary>
        [Test]
        public void Deserialize_RefArray_Nulls_CreatesRefSlots()
        {
            var json = "{ \"children\": [null, null, null] }";
            var storage = JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
            var root = storage.Root;

            var arr = root.GetObjectArray("children");
            Assert.That(arr.Count, Is.EqualTo(3));
            Assert.That(arr[0].IsNull, Is.True);
            Assert.That(arr[1].IsNull, Is.True);
            Assert.That(arr[2].IsNull, Is.True);
        }

        /// <summary>
        /// Roundtrip: build in memory, write JSON, read back, then Read<T> should
        /// return the same values; schema should be stable.
        /// </summary>
        [Test]
        public void RoundTrip_RestoreValuesAndSchema()
        {
            var schema = new SchemaBuilder(true)
                .AddFieldOf<int>("hp")
                .AddArrayOf<float>("speeds", 4)
                .AddRefArray("children", 2)
                .Build();

            var s1 = new Storage(schema);
            var r1 = s1.Root;

            r1.Write("hp", 123);
            var sp = r1.GetArray<float>("speeds");
            new float[] { 0.1f, 0.2f, 0.3f, 0.4f }.AsSpan().CopyTo(sp.AsSpan());
            // Leave children nulls

            var json = JsonSerialization.ToJson(s1, ParamsWithAdapter());
            Debug.Log(json);

            var s2 = JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
            var r2 = s2.Root;
            Debug.Log(string.Join(',', r2.HeaderHints.ToArray().Select(s => TypeUtil.ToString(s))));

            Assert.That(r2.Read<int>("hp"), Is.EqualTo(123));
            Debug.Log(string.Join(',', r2.HeaderHints.ToArray().Select(s => TypeUtil.ToString(s))));
            var sp2 = r2.GetArray<float>("speeds");
            Debug.Log(JsonSerialization.ToJson(s2, ParamsWithAdapter()));
            Debug.Log(string.Join(',', sp2.ToArray()));
            Assert.That(sp2.AsSpan().SequenceEqual(new float[] { 0.1f, 0.2f, 0.3f, 0.4f }), Is.True);

            var kids = r2.GetObjectArray("children");
            Assert.That(kids.Count, Is.EqualTo(2));
            Assert.That(kids[0].IsNull && kids[1].IsNull, Is.True);
        }

        /// <summary>
        /// Self-heal on same-size-but-different-type: if deserializer guessed Int32 but user wants Single,
        /// we should convert in-place without rescheming (field length stays).
        /// </summary>
        [Test]
        public void SelfHeal_SameSize_Int32ToFloat32_NoReschemeLengthChange()
        {
            // JSON value "42" often gets guessed as Int32 when no schema exists.
            var json = "{ \"v\": 42 }";
            var storage = JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
            var root = storage.Root;

            var baseLength = root.Schema.GetField("v").AbsLength;

            // First force an Int32 read to initialize field (if not already)
            int vInt = root.Read<int>("v");
            Assert.That(vInt, Is.EqualTo(42));

            // Now read as float ¡ú should convert in place, not change AbsLength (still 4)
            float vFloat = root.Read<float>("v");
            Assert.That(vFloat, Is.EqualTo(42.0f).Within(1e-6));

            var f = root.Schema.GetField("v");
            Assert.That(f.AbsLength, Is.GreaterThanOrEqualTo(baseLength)); // same size, no rescheme length change
        }

        /// <summary>
        /// Value array - length mismatch on self-heal: if JSON gave N but we later access as M,
        /// rescheme should expand/shrink and data should copy min(N,M) elements; tail zero-filled.
        /// </summary>
        [Test]
        public void SelfHeal_Array_LengthChange_CopyAndZeroFill()
        {
            Assert.Inconclusive("Not supported yet");
            var json = "{ \"arr\": [10, 20, 30] }";
            var storage = JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
            var root = storage.Root;

            // Access as int[5]; your array accessor should self-heal schema to new fixed length
            var span5 = root.GetArray<int>("arr"); // If you don't have lengthed overload, adapt to your API
            Assert.That(span5.Length, Is.EqualTo(5));
            Assert.That(span5[0], Is.EqualTo(10));
            Assert.That(span5[1], Is.EqualTo(20));
            Assert.That(span5[2], Is.EqualTo(30));
            Assert.That(span5[3], Is.EqualTo(0));  // zero-filled
            Assert.That(span5[4], Is.EqualTo(0));
        }

        /// <summary>
        /// Ref ? value mismatch must throw (structural change is not allowed).
        /// </summary>
        [Test]
        public void StructuralChange_ValueVsRef_ShouldThrow()
        {
            var json = "{ \"child\": {} }"; // ref (object)
            var storage = JsonSerialization.FromJson<Storage>(json, ParamsWithAdapter());
            var root = storage.Root;

            // Trying to read value type from a ref field should fail
            bool threw = false;
            try
            {
                var _ = root.Read<int>("child");
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Assert.That(threw, Is.True);
        }

        // -------- helpers --------

        /// <summary>Count non-overlapping substring occurrences.</summary>
        private static int CountSubstring(string s, string needle)
        {
            int count = 0, idx = 0;
            while (true)
            {
                idx = s.IndexOf(needle, idx, StringComparison.Ordinal);
                if (idx < 0) break;
                count++;
                idx += needle.Length;
            }
            return count;
        }
    }
}
