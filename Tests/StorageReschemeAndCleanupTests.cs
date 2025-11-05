using NUnit.Framework;
using System.Collections.Generic;
using UnityEngine;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class StorageReschemeAndCleanupTests
    {
        // root: int hp; ref child; ref-array children[2]; float[4] speeds
        private Schema _rootSchema;

        // child: int hp;
        private Schema _childSchema;

        [SetUp]
        public void Setup()
        {
            _rootSchema = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .AddRef("child")
                .AddRefArray("children", 2)
                .AddFieldFixed("speeds", sizeof(float) * 4)
                .Build();

            _childSchema = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .Build();
        }

        // --- Complex: create subtree in object array; replace one element; ensure old child unregistered ---
        [Test]
        public void ObjectArray_Replace_Element_Unregisters_Previous()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;
            var arr = root.GetObjectArray("children");

            var c0 = arr[0].GetObject(_childSchema);
            var c1 = arr[1].GetObject(_childSchema);
            var id0 = c0.ID;
            var id1 = c1.ID;

            // Replace slot 0 with a new child
            var c0b = arr[0].GetObject(_childSchema);
            var id0b = c0b.ID;

            // If AsObjectOrNew returns existing when present, do an explicit Clear + New to force replacement
            if (id0b == id0)
            {
                arr.ClearAt(0);
                c0b = arr[0].GetObject(_childSchema);
                id0b = c0b.ID;
            }

            Assert.That(id0b, Is.Not.EqualTo(id0));
            Assert.That(Container.Registry.Shared.GetContainer(id0), Is.Null, "Old element should be unregistered.");
            Assert.That(Container.Registry.Shared.GetContainer(id0b), Is.Not.Null);
            Assert.That(Container.Registry.Shared.GetContainer(id1), Is.Not.Null);
        }

        // --- Storage cleanup: after Dispose, no child remains reachable in the registry ---
        [Test]
        public void Storage_Dispose_Unregisters_All_Subtree()
        {
            var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // Fill child and children[0..1]
            var direct = root.GetObject("child", false, _childSchema);
            var arr = root.GetObjectArray("children");
            var a0 = arr[0].GetObject(_childSchema);
            var a1 = arr[1].GetObject(_childSchema);

            var ids = new List<ulong> { direct.ID, a0.ID, a1.ID };

            storage.Dispose();

            foreach (var id in ids)
                Assert.That(Container.Registry.Shared.GetContainer(id), Is.Null,
                    $"Container id {id} should be unregistered after storage dispose.");
        }

        // --- Rescheme(Add): write a new field name via Write<T> adds the field and stores the value ---
        [Test]
        public void Rescheme_AddField_Via_Write_Auto()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            Debug.Log(root.ToString());
            // hp exists; score does not exist
            root.Write<int>("hp", 10);                    // existing value write  :contentReference[oaicite:8]{index=8}

            Debug.Log(root.ToString());
            root.Write<float>("score", 3.5f);            // nonexistent -> auto add via Rescheme+AddFieldOf<T>  :contentReference[oaicite:9]{index=9}

            Debug.Log(root.ToString());
            Assert.That(root.Read<int>("hp"), Is.EqualTo(10));
            Assert.That(root.Read<float>("score"), Is.EqualTo(3.5f));
        }

        // --- Rescheme(Delete): delete a value field; data of remaining fields preserved ---
        [Test]
        public void Rescheme_Delete_ValueField_Preserves_Others()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            root.Write<int>("hp", 77);
            var speeds = root.GetArray<float>("speeds");     // 4 floats
            for (int i = 0; i < speeds.Length; i++) speeds[i] = i + 0.25f;

            // Delete "speeds"
            var deleted = root.Delete("speeds");             //  :contentReference[oaicite:10]{index=10}
            Assert.That(deleted, Is.True);
            Assert.That(root.Read<int>("hp"), Is.EqualTo(77));

            // Accessing 'speeds' as field should now fail (TryGetField returns false)
            var ok = root.HasField("speeds");
            Assert.That(ok, Is.False);
        }

        // --- Rescheme(Delete): delete a ref field => its subtree is unregistered during migration ---
        [Test]
        public void Rescheme_Delete_RefField_Unregisters_Subtree()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // Create direct child and children
            var direct = root.GetObject("child", false, _childSchema);
            var arr = root.GetObjectArray("children");
            var a0 = arr[0].GetObject(_childSchema);
            var a1 = arr[1].GetObject(_childSchema);

            var ids = new[] { direct.ID, a0.ID, a1.ID };

            // Delete "child" ref field ¡ª migration should unregister that subtree
            var deleted = root.Delete("child");              // calls Container.Rescheme -> RebuildSchema  
            Assert.That(deleted, Is.True);
            Assert.That(Container.Registry.Shared.GetContainer(ids[0]), Is.Null, "Deleted ref field's subtree should be unregistered.");

            // children ref-array still present; its elements remain
            Assert.That(Container.Registry.Shared.GetContainer(ids[1]), Is.Not.Null);
            Assert.That(Container.Registry.Shared.GetContainer(ids[2]), Is.Not.Null);
        }

        // --- Rescheme(Delete multiple): delete both ref-array and value field; keep hp ---
        [Test]
        public void Rescheme_Delete_Multiple_Fields()
        {
            using var storage = new Storage(_rootSchema);
            var root = storage.Root;

            // Initialize tree
            root.Write<int>("hp", 5);
            var arr = root.GetObjectArray("children");
            var a0 = arr[0].GetObject(_childSchema);
            var a1 = arr[1].GetObject(_childSchema);
            var ids = new[] { a0.ID, a1.ID };

            // Delete children (ref-array) + speeds (value)
            var removed = root.Delete("children", "speeds");  //  :contentReference[oaicite:12]{index=12}
            Assert.That(removed, Is.EqualTo(2));

            // hp still present
            Assert.That(root.Read<int>("hp"), Is.EqualTo(5));

            // children elements should be unregistered as part of migration
            Assert.That(Container.Registry.Shared.GetContainer(ids[0]), Is.Null);
            Assert.That(Container.Registry.Shared.GetContainer(ids[1]), Is.Null);

            // Verify fields gone
            Assert.That(root.HasField("children"), Is.False);
            Assert.That(root.HasField("speeds"), Is.False);
        }

        // --- Cleanup safety: disposing an already-disposed Storage is safe ---
        [Test]
        public void Storage_Dispose_Is_Idempotent()
        {
            var storage = new Storage(_rootSchema);
            var root = storage.Root;
            var child = root.GetObject("child", false, _childSchema);
            var id = child.ID;

            storage.Dispose();
            storage.Dispose(); // second call should be a no-op

            Assert.That(Container.Registry.Shared.GetContainer(id), Is.Null);
        }
    }
}
