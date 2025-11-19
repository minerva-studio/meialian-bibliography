using Minerva.DataStorage.Serialization;
using NUnit.Framework;
using System;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class BinarySerializationTests
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
            root.WriteString("Name", "Hero".AsSpan());

            var bytes = storage.ToBinary().ToArray();
            storage.Dispose();   // Dispose original tree after serialization

            var deserialized = BinarySerialization.Parse(bytes);
            var root2 = deserialized.Root;

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
            var bytes = storage.ToBinary().ToArray();
            storage.Dispose();

            var deserialized = BinarySerialization.Parse(bytes);
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

            var bytes = storage.ToBinary().ToArray();
            storage.Dispose();

            var deserialized = BinarySerialization.Parse(bytes);
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

            var bytes = storage.ToBinary().ToArray();
            storage.Dispose();

            var deserialized = BinarySerialization.Parse(bytes);
            var root2 = deserialized.Root;

            var values2 = root2.ReadArray<int>("Values".AsSpan());
            CollectionAssert.AreEqual(values, values2);

            deserialized.Dispose();
        }

        /// <summary>
        /// Verifies that the three Parse overloads produce logically identical data:
        /// - Parse(ReadOnlySpan&lt;byte&gt;)
        /// - Parse(Memory&lt;byte&gt;, allocate: true)
        /// - Parse(Memory&lt;byte&gt;, allocate: false)
        /// </summary>
        [Test]
        public void Parse_Overloads_ProduceSameLogicalData()
        {
            var storage = new Storage();
            var root = storage.Root;

            root.Write("Health", 123);
            root.GetObject("Child").Write("Value", 456);

            var bytes = storage.ToBinary().ToArray();
            storage.Dispose();

            var sSpan = BinarySerialization.Parse(bytes);
            var sAlloc = BinarySerialization.Parse(new Memory<byte>(bytes), allocate: true);
            var sAlias = BinarySerialization.Parse(new Memory<byte>(bytes), allocate: false);

            Assert.That(sSpan.Root.Read<int>("Health"), Is.EqualTo(123));
            Assert.That(sAlloc.Root.Read<int>("Health"), Is.EqualTo(123));
            Assert.That(sAlias.Root.Read<int>("Health"), Is.EqualTo(123));

            Assert.That(sSpan.Root.GetObject("Child").Read<int>("Value"), Is.EqualTo(456));
            Assert.That(sAlloc.Root.GetObject("Child").Read<int>("Value"), Is.EqualTo(456));
            Assert.That(sAlias.Root.GetObject("Child").Read<int>("Value"), Is.EqualTo(456));

            sSpan.Dispose();
            sAlloc.Dispose();
            sAlias.Dispose();
        }
    }
}
