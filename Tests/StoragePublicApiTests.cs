using NUnit.Framework;
using System;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class StoragePublicApiTests
    {
        // root: int hp; ref child; float[4] speeds
        private Schema _rootSchema;

        // leaf: int hp;
        private Schema _leafSchema;

        [SetUp]
        public void Setup()
        {
            _rootSchema = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .AddRef("child")
                .AddFieldFixed("speeds", sizeof(float) * 4)
                .Build();

            _leafSchema = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .Build();
        }

        [TearDown]
        public void Teardown()
        {
            // No-op; individual tests dispose Storage when needed.
        }

        // 1) Storage lifetime: root is usable; Dispose() recursively unregisters.
        [Test]
        public void Storage_Root_IsUsable_And_Dispose_Unregisters()
        {
            var storage = new Storage(_rootSchema);
            var root = storage.Root; // ref struct view

            // Write/read a value field
            root.Write<int>("hp", 123);
            Assert.That(root.Read<int>("hp"), Is.EqualTo(123));

            // Dispose should recursively unregister the whole tree
            storage.Dispose();

            // After Dispose, accessing the ref struct should throw
            var threw = false;
            try
            {
                root.Read<int>("hp");
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Assert.That(threw, Is.True, "Expected InvalidOperationException was not thrown.");
        }

        // 2) StorageField: value-field read/write + array/span views
        [Test]
        public void StorageField_ValueField_ReadWrite_And_ArraySpan()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // Value field via StorageField
            var hp = root.GetField("hp");
            hp.Write(999);
            Assert.That(hp.Read<int>(), Is.EqualTo(999));

            // Float array (packed in bytes): speeds has 4 elements
            var speeds = root.GetField("speeds").AsArray<float>();
            Assert.That(speeds.Count, Is.EqualTo(4));
            for (int i = 0; i < speeds.Count; i++)
                speeds[i] = i * 0.5f;

            // Read back via read-only span
            var speedsRo = root.GetField("speeds").AsReadOnlySpan<float>();
            CollectionAssert.AreEqual(new[] { 0f, 0.5f, 1.0f, 1.5f }, speedsRo.ToArray());
        }

        // 3) StorageField: single-slot ref field null semantics and Try pattern
        [Test]
        public void StorageField_Ref_SingleSlot_NullSemantics_And_TryGet()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            var childField = root.GetField("child"); // reference field

            // Initially null: GetObject() returns default (IsNull == true); TryGetObject returns false
            var maybeChild = childField.GetObject();
            Assert.That(maybeChild.IsNull, Is.True);

            Assert.That(childField.TryGetObject(out var childSO), Is.False);

            // Create a child via the field API and bind its ID into the slot
            childField.CreateObject(_leafSchema);

            // Now the object should be non-null
            maybeChild = childField.GetObject();
            Assert.That(maybeChild.IsNull, Is.False);

            // Write/read on the child
            maybeChild.Write<int>("hp", 42);
            Assert.That(maybeChild.Read<int>("hp"), Is.EqualTo(42));
        }

        // 4) StorageObject: strong navigation throws if child is null
        [Test]
        public void StorageObject_GetObject_Return_Null_When_Child_Is_Null()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // child is null; GetObject(fieldName) should throw
            var threw = false;
            StorageObject child = default;
            try
            {
                child = root.GetObject("child");
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Assert.That(threw, Is.False, "Expected InvalidOperationException was thrown.");
            Assert.That(child.IsNull, "Expected null was returned.");
        }

        // 5) Dispose should recursively unregister children; IDs should no longer resolve
        [Test]
        public void Storage_Dispose_Recursively_Unregisters_Children()
        {
            var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // Create a child and write a value
            var childField = root.GetField("child");
            childField.CreateObject(_leafSchema);
            var child = childField.GetObject();
            child.Write<int>("hp", 7);
            var childId = child.ID;

            // Dispose storage: should unregister root and its subtree
            storage.Dispose();

            // Registry should not resolve the child ID anymore
            var reg = Container.Registry.Shared;
            Assert.That(reg.GetContainer(childId), Is.Null);
        }

        [Test]
        public void StorageField_GetObjectOrNew_Should_Create_When_Null()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            var childField = root.GetField("child");

            // Expected: when slot is empty, GetObjectOrNew creates a new child and returns a non-null object.
            var obj = childField.GetObjectOrNew();
            Assert.That(obj.IsNull, Is.False, "Expected GetObjectOrNew to create a new child when slot is empty.");
        }
    }
}
