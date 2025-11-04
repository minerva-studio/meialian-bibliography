using NUnit.Framework;
using System;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class StorageObjectArrayTests
    {
        // root: int hp; ref-array children[3]; float[4] speeds
        private Schema _rootSchema;

        // child: int hp
        private Schema _childSchema;

        [SetUp]
        public void Setup()
        {
            _rootSchema = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .AddRefArray("children", 3)
                .AddFieldFixed("speeds", sizeof(float) * 4)
                .Build();

            _childSchema = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .Build();

            _rootSchema = SchemaPool.Shared.Intern(_rootSchema);
            _childSchema = SchemaPool.Shared.Intern(_childSchema);
        }

        [Test]
        public void ObjectArray_Basic_Create_Read_Write()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // Get the ref-array field via StorageObject
            var arr = root.GetObjectArray("children");
            Assert.That(arr.Count, Is.EqualTo(3));

            // Initially all null/empty
            for (int i = 0; i < arr.Count; i++)
            {
                var ok = arr.TryGet(i, out var child);
                Assert.That(ok, Is.False);
                Assert.That(child.IsNull, Is.True);
            }

            // Create three children via element API (AsObjectOrNew)
            for (int i = 0; i < arr.Count; i++)
            {
                var child = arr[i].AsObjectOrNew(_childSchema);
                Assert.That(child.IsNull, Is.False);

                // Write a distinct hp
                child.Write<int>("hp", (i + 1) * 10);
            }

            // Read back
            for (int i = 0; i < arr.Count; i++)
            {
                var ok = arr.TryGet(i, out var child);
                Assert.That(ok, Is.True);
                Assert.That(child.IsNull, Is.False);
                Assert.That(child.Read<int>("hp"), Is.EqualTo((i + 1) * 10));
            }
        }

        [Test]
        public void ObjectArray_AsField_View_Matches_Object_View()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // Create via StorageField view
            var field = root.GetField("children");
            var arrField = field.AsObjectArray();
            Assert.That(arrField.Count, Is.EqualTo(3));

            for (int i = 0; i < arrField.Count; i++)
            {
                var child = arrField[i].AsObjectOrNew(_childSchema);
                Assert.That(child.IsNull, Is.False);
                child.Write<int>("hp", 100 + i);
            }

            // Cross-check via StorageObject.GetObjectArray
            var arr = root.GetObjectArray("children");
            for (int i = 0; i < arr.Count; i++)
            {
                var c = arr.Get(i);
                Assert.That(c.Read<int>("hp"), Is.EqualTo(100 + i));
            }
        }

        [Test]
        public void ObjectArray_ClearAt_And_ClearAll()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            var arr = root.GetObjectArray("children");

            // Create at 0,1,2
            for (int i = 0; i < arr.Count; i++)
                arr[i].AsObjectOrNew(_childSchema);

            // Clear a single slot
            arr.ClearAt(1);

            // Check: slot 0,2 present; slot 1 empty
            {
                var ok0 = arr.TryGet(0, out var c0);
                var ok1 = arr.TryGet(1, out var c1);
                var ok2 = arr.TryGet(2, out var c2);

                Assert.That(ok0, Is.True); Assert.That(c0.IsNull, Is.False);
                Assert.That(ok1, Is.False); Assert.That(c1.IsNull, Is.True);
                Assert.That(ok2, Is.True); Assert.That(c2.IsNull, Is.False);
            }

            // Clear all
            arr.ClearAll();

            for (int i = 0; i < arr.Count; i++)
            {
                var ok = arr.TryGet(i, out var child);
                Assert.That(ok, Is.False);
                Assert.That(child.IsNull, Is.True);
            }
        }

        [Test]
        public void ObjectArray_AsObject_ReturnsExisting_NotRecreate()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            var arr = root.GetObjectArray("children");

            // Create once
            var c0 = arr[0].AsObjectOrNew(_childSchema);
            var id0 = c0.ID;

            // Ask AsObject (get only) should return same ID
            var c0b = arr[0].AsObject();
            Assert.That(c0b.IsNull, Is.False);
            Assert.That(c0b.ID, Is.EqualTo(id0));

            // Ask AsObjectOrNew again should still return the same existing child
            var c0c = arr[0].AsObjectOrNew(_childSchema);
            Assert.That(c0c.ID, Is.EqualTo(id0));
        }

        [Test]
        public void ObjectArray_Index_OutOfRange_Throws()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            var arr = root.GetObjectArray("children");

            bool threw = false;
            try
            {
                // The indexer builds a StorageObjectArrayElement; the exception will be thrown
                // when the element attempts to access the Span at invalid index
                // via AsObject/AsObjectOrNew (below).
                var el = arr[arr.Count]; // create element
                el.AsObject();           // force access -> should throw
            }
            catch (IndexOutOfRangeException)
            {
                threw = true;
            }
            Assert.That(threw, Is.True, "Expected IndexOutOfRangeException was not thrown.");
        }

        [Test]
        public void Storage_Dispose_Unregisters_All_ObjectArray_Children()
        {
            var storage = new Storage(_rootSchema);
            var root = storage.Root;

            var arr = root.GetObjectArray("children");
            var ids = new ulong[arr.Count];

            for (int i = 0; i < arr.Count; i++)
            {
                var child = arr[i].AsObjectOrNew(_childSchema);
                child.Write<int>("hp", 5 + i);
                ids[i] = child.ID;
            }

            storage.Dispose();

            var reg = Container.ContainerRegistry.Shared;
            for (int i = 0; i < ids.Length; i++)
                Assert.That(reg.GetContainer(ids[i]), Is.Null, $"Child {i} should be unregistered.");
        }

        [Test]
        public void StorageField_ValueArray_And_ObjectArray_Api_Contract()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // Get value array and write values
            var speeds = root.GetArray<float>("speeds");
            Assert.That(speeds.Count, Is.EqualTo(4));
            for (int i = 0; i < 4; i++) speeds[i] = i * 1.25f;

            var roSpeeds = root.GetField("speeds").AsReadOnlySpan<float>();
            CollectionAssert.AreEqual(new[] { 0f, 1.25f, 2.5f, 3.75f }, roSpeeds.ToArray());

            // Trying to treat a ref-array as value array should throw
            var childField = root.GetField("children");
            bool threw = false;
            try
            {
                childField.AsArray<int>(); // children is ref-array, not value field
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Assert.That(threw, Is.True, "Expected InvalidOperationException was not thrown.");
        }

        [Test]
        public void StorageObject_TryGetField_Works_And_Equality_Semantics()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // TryGetField success
            var ok = root.TryGetField("hp", out var hpField);
            Assert.That(ok, Is.True);

            // TryGetField fail
            var ok2 = root.TryGetField("missing", out var missing);
            Assert.That(ok2, Is.False);
            Assert.That(missing == default, Is.True);

            // Equality on StorageField compares (container, descriptor)
            var hpField2 = root.GetField("hp");
            Assert.That(hpField == hpField2, Is.True);
            Assert.That(hpField != hpField2, Is.False);
        }
    }
}
