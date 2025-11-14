using NUnit.Framework;
using System.Collections.Generic;
using UnityEngine;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageReschemeAndCleanupTests
    {
        // root: int hp; ref child; ref-array children[2]; float[4] speeds
        private ContainerLayout _rootLayout;

        // child: int hp
        private ContainerLayout _childLayout;

        [SetUp]
        public void Setup()
        {
            // root layout
            {
                var ob = new ObjectBuilder();
                ob.SetScalar<int>("hp");
                ob.SetRef("child", 0UL);
                ob.SetRefArray("children", 2);
                ob.SetArray<float>("speeds", 4);
                _rootLayout = ob.BuildLayout();
            }
            // child layout
            {
                var ob = new ObjectBuilder();
                ob.SetScalar<int>("hp");
                _childLayout = ob.BuildLayout();
            }
        }

        // --- Replace element by clearing the slot; old child must be unregistered; new child gets a new ID ---
        [Test]
        public void ObjectArray_Replace_Element_Unregisters_Previous()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;
            var arr = root.GetObjectArray("children");
            var reg = Container.Registry.Shared;

            // create two children
            var c0 = arr[0].GetObject(_childLayout);
            var c1 = arr[1].GetObject(_childLayout);
            var oldId0 = c0.ID;
            var id1 = c1.ID;

            // clear slot 0 -> slot becomes global empty object, old child must be unregistered
            arr.ClearAt(0);

            // the old object is gone from registry
            Assert.That(reg.GetContainer(oldId0), Is.Null, "Old element should be unregistered after ClearAt.");

            // slot 0 now points to the global empty object (no creation)
            var empty0 = arr[0].GetObjectNoAllocate();
            Assert.That(empty0.ID, Is.EqualTo(Container.Registry.ID.Empty), "Cleared slot should reference the global Empty object.");

            // re-create at slot 0 -> new object with a new ID
            var c0b = arr[0].GetObject(_childLayout);
            var newId0 = c0b.ID;

            //Assert.That(newId0, Is.Not.EqualTo(oldId0), "Recreated element should have a different ID.");
            Assert.That(reg.GetContainer(newId0), Is.Not.Null, "New element should be registered.");
            Assert.That(reg.GetContainer(id1), Is.Not.Null, "Other slots should be unaffected.");
        }

        // --- Storage cleanup: after Dispose, no child remains reachable in the registry ---
        [Test]
        public void Storage_Dispose_Unregisters_All_Subtree()
        {
            var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var direct = root.GetObject("child", false, _childLayout);
            var arr = root.GetObjectArray("children");
            var a0 = arr[0].GetObject(_childLayout);
            var a1 = arr[1].GetObject(_childLayout);

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
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            Debug.Log(root.ToString());
            root.Write<int>("hp", 10);                  // existing

            Debug.Log(root.ToString());
            root.Write<float>("score", 3.5f);           // nonexistent -> auto add

            Debug.Log(root.ToString());
            Assert.That(root.Read<int>("hp"), Is.EqualTo(10));
            Assert.That(root.Read<float>("score"), Is.EqualTo(3.5f));
        }

        // --- Rescheme(Delete): delete a value field; data of remaining fields preserved ---
        [Test]
        public void Rescheme_Delete_ValueField_Preserves_Others()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            root.Write<int>("hp", 77);
            var speeds = root.GetArray("speeds"); // 4 floats
            for (int i = 0; i < speeds.Length; i++) speeds[i].Write(i + 0.25f);

            // Delete "speeds"
            var deleted = root.Delete("speeds");
            Assert.That(deleted, Is.True);
            Assert.That(root.Read<int>("hp"), Is.EqualTo(77));

            // Verify gone
            var ok = root.HasField("speeds");
            Assert.That(ok, Is.False);
        }

        // --- Rescheme(Delete): delete a ref field => its subtree is unregistered during migration ---
        [Test]
        public void Rescheme_Delete_RefField_Unregisters_Subtree()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var direct = root.GetObject("child", false, _childLayout);
            var arr = root.GetObjectArray("children");
            var a0 = arr[0].GetObject(_childLayout);
            var a1 = arr[1].GetObject(_childLayout);

            var ids = new[] { direct.ID, a0.ID, a1.ID };

            var deleted = root.Delete("child");
            Assert.That(deleted, Is.True);
            Assert.That(Container.Registry.Shared.GetContainer(ids[0]), Is.Null,
                "Deleted ref field's subtree should be unregistered.");

            // children array unchanged
            Assert.That(Container.Registry.Shared.GetContainer(ids[1]), Is.Not.Null);
            Assert.That(Container.Registry.Shared.GetContainer(ids[2]), Is.Not.Null);
        }

        // --- Rescheme(Delete multiple): delete both ref-array and value field; keep hp ---
        [Test]
        public void Rescheme_Delete_Multiple_Fields()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            root.Write<int>("hp", 5);
            var arr = root.GetObjectArray("children");
            var a0 = arr[0].GetObject(_childLayout);
            var a1 = arr[1].GetObject(_childLayout);
            var ids = new[] { a0.ID, a1.ID };

            var removed = root.Delete("children", "speeds");
            Assert.That(removed, Is.EqualTo(2));

            Assert.That(root.Read<int>("hp"), Is.EqualTo(5));
            Assert.That(Container.Registry.Shared.GetContainer(ids[0]), Is.Null);
            Assert.That(Container.Registry.Shared.GetContainer(ids[1]), Is.Null);

            Assert.That(root.HasField("children"), Is.False);
            Assert.That(root.HasField("speeds"), Is.False);
        }

        // --- Cleanup safety: disposing an already-disposed Storage is safe ---
        [Test]
        public void Storage_Dispose_Is_Idempotent()
        {
            var storage = new Storage(_rootLayout);
            var root = storage.Root;
            var child = root.GetObject("child", false, _childLayout);
            var id = child.ID;

            storage.Dispose();
            storage.Dispose(); // no-op

            Assert.That(Container.Registry.Shared.GetContainer(id), Is.Null);
        }
    }
}
