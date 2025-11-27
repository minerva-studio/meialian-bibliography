using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageMoveTests
    {
        [Test]
        public void Move_Raises_Rename_Event_And_Field_Subscriber_Not_Migrated()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // create field
            root.Write("src", 1);

            var events = new List<StorageEventArgs>();

            using var fieldSub = root.Subscribe("src", (in StorageEventArgs a) => events.Add(a));

            // perform move
            root.Move("src", "dst");

            // rename event should be delivered to field subscriber (old name)
            Assert.That(events.Count, Is.EqualTo(1));
            Assert.That(events[0].Event, Is.EqualTo(StorageEvent.Rename));
            Assert.That(events[0].Path, Is.EqualTo("dst"));

            events.Clear();

            // subsequent writes to new name should NOT notify the old field subscriber (with migration)
            root.Write("dst", 2);
            Assert.That(events.Count, Is.EqualTo(1));

            // container-level subscribers still receive writes
            var containerEvents = new List<StorageEventArgs>();
            using var containerSub = root.Subscribe((in StorageEventArgs a) => containerEvents.Add(a));
            root.Write("dst", 3);
            Assert.That(containerEvents.Count, Is.GreaterThanOrEqualTo(1));
            Assert.That(containerEvents[^1].Path, Is.EqualTo("dst"));
        }

        [Test]
        public void TryMove_Fails_When_Destination_Exists_And_No_Events_Fired()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("src", 1);
            root.Write("dst", 5);

            var events = new List<StorageEventArgs>();
            using var sub = root.Subscribe((in StorageEventArgs a) => events.Add(a));

            bool ok = root.TryMove("src", "dst");
            Assert.That(ok, Is.False);

            // no rename event should be fired
            Assert.That(events.Count, Is.EqualTo(0));

            // both fields still readable
            Assert.That(root.ReadPath<int>("src"), Is.EqualTo(1));
            Assert.That(root.ReadPath<int>("dst"), Is.EqualTo(5));
        }

        [Test]
        public void TryMove_Succeeds_And_Raises_Rename_Event()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("src", 10);

            var events = new List<StorageEventArgs>();
            using var sub = root.Subscribe((in StorageEventArgs a) => events.Add(a));

            bool ok = root.TryMove("src", "dst");
            Assert.That(ok, Is.True);

            // rename event should be present
            Assert.That(events.Count, Is.GreaterThanOrEqualTo(1));
            Assert.That(events[0].Event, Is.EqualTo(StorageEvent.Rename));
            Assert.That(events[0].Path, Is.EqualTo("dst"));

            // old name should no longer exist, new name should hold the value
            Assert.Throws<InvalidOperationException>(() => root.ReadPath<int>("src"));
            Assert.That(root.ReadPath<int>("dst"), Is.EqualTo(10));
        }

        [Test]
        public void Move_Scalar_Field_Preserves_Type_Value_And_Index_Order()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("a", 1);
            root.Write("b", 2);
            root.Write("c", 3);

            // Move middle field to a new name; verify ordering and reindex correctness
            root.Move("b", "x");

            Assert.That(root.ReadPath<int>("a"), Is.EqualTo(1));
            Assert.Throws<InvalidOperationException>(() => root.ReadPath<int>("b"));
            Assert.That(root.ReadPath<int>("c"), Is.EqualTo(3));
            Assert.That(root.ReadPath<int>("x"), Is.EqualTo(2));

            // Move to a lexicographically earlier name to trigger header resort
            root.Move("x", "aa");
            Assert.That(root.ReadPath<int>("aa"), Is.EqualTo(2));
            Assert.That(root.ReadPath<int>("a"), Is.EqualTo(1));
            Assert.That(root.ReadPath<int>("c"), Is.EqualTo(3));
        }

        [Test]
        public void Move_String_Field_Preserves_Content_And_Length()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Strings are stored via array/object backing
            root.WritePath("s", "HelloWorld");

            root.Move("s", "t");

            Assert.Throws<ArgumentException>(() => root.ReadStringPath("s"));
            Assert.That(root.ReadStringPath("t"), Is.EqualTo("HelloWorld"));

            // Continue write/read on new name
            root.WritePath("t", "Bye");
            Assert.That(root.ReadStringPath("t"), Is.EqualTo("Bye"));
        }

        [Test]
        public void Move_Inline_Array_Field_Preserves_Length_And_Elements()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArrayPath("arr", new[] { 1, 2, 3, 4 });

            root.Move("arr", "arr2");

            Assert.Throws<ArgumentException>(() => root.ReadArrayPath<int>("arr"));
            var arr2 = root.ReadArrayPath<int>("arr2");
            Assert.That(arr2, Is.EqualTo(new[] { 1, 2, 3, 4 }));

            // Rewrite with different length then recheck
            root.WriteArrayPath("arr2", new[] { 5, 6, 7, 8, 9 });
            Assert.That(root.ReadArrayPath<int>("arr2"), Is.EqualTo(new[] { 5, 6, 7, 8, 9 }));
        }

        [Test]
        public void Move_Object_Field_Preserves_Child_References_And_Data()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Create object field and write subfields
            var player = root.GetObject("player");
            player.Write("hp", 100);
            player.Write("name", "Alice");

            // Move object field to a new name
            root.Move("player", "entity");

            // Old name should be missing; new name retains data
            Assert.That(root.TryGetObject("player", out _), Is.False);
            var entity = root.GetObject("entity", reschemeIfMissing: false, layout: null);
            Assert.That(entity.Read<int>("hp"), Is.EqualTo(100));
            Assert.That(entity.ReadString("name"), Is.EqualTo("Alice"));

            // Continue writing under new name
            entity.Write("hp", 250);
            Assert.That(entity.Read<int>("hp"), Is.EqualTo(250));
        }

        [Test]
        public void Move_Object_Array_Field_Preserves_Elements_And_IDs()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Create object array (ref inline array)
            var arr = root.GetArray("items", TypeData.Ref, createIfMissing: true, reschemeOnTypeMismatch: true, overrideExisting: true);
            arr.EnsureLength(3);
            arr.GetObject(0).Write("val", 10);
            arr.GetObject(1).Write("val", 20);
            arr.GetObject(2).Write("val", 30);

            // Capture element IDs to compare
            var idsBefore = new ulong[3];
            for (int i = 0; i < 3; i++)
                idsBefore[i] = arr.GetObject(i).ID;

            // Move array field
            root.Move("items", "entries");

            // New field read and ID consistency
            var entries = root.GetArray("entries");
            Assert.That(entries.Length, Is.EqualTo(3));
            for (int i = 0; i < 3; i++)
            {
                var obj = entries.GetObject(i);
                Assert.That(obj.Read<int>("val"), Is.EqualTo((i + 1) * 10));
                Assert.That(obj.ID, Is.EqualTo(idsBefore[i]));
            }

            // Old name should not be available
            Assert.That(root.TryGetArray("items", out _), Is.False);
        }

        [Test]
        public void Move_To_Same_Name_No_Op_No_Event()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("score", 99);

            var events = new List<StorageEventArgs>();
            using var sub = root.Subscribe((in StorageEventArgs a) => events.Add(a));

            // Moving to same name should be a no-op
            root.Move("score", "score");

            // No rename event; data intact
            Assert.That(events.TrueForAll(e => e.Event != StorageEvent.Rename), Is.True);
            Assert.That(root.Read<int>("score"), Is.EqualTo(99));
        }

        [Test]
        public void TryMove_Source_Not_Exist_False_Destination_Free()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var events = new List<StorageEventArgs>();
            using var sub = root.Subscribe((in StorageEventArgs a) => events.Add(a));

            bool ok = root.TryMove("missing", "dst");
            Assert.That(ok, Is.False);
            Assert.That(events.Count, Is.EqualTo(0));
            Assert.Throws<InvalidOperationException>(() => root.ReadPath<int>("dst"));
        }

        [Test]
        public void Move_Raises_Rename_Event_And_New_Field_Writes_Notify_New_Subscribers()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("old", 1);

            var oldFieldEvents = new List<StorageEventArgs>();
            using var oldFieldSub = root.Subscribe("old", (in StorageEventArgs a) => oldFieldEvents.Add(a));


            root.Move("old", "new");

            // Rename event received by old field subscriber; path should be new name
            Assert.That(oldFieldEvents.Count, Is.GreaterThanOrEqualTo(1));
            Assert.That(oldFieldEvents[0].Event, Is.EqualTo(StorageEvent.Rename));
            Assert.That(oldFieldEvents[0].Path, Is.EqualTo("new"));

            var newFieldEvents = new List<StorageEventArgs>();
            using var newFieldSub = root.Subscribe("new", (in StorageEventArgs a) => newFieldEvents.Add(a));

            // Subsequent writes should only notify new field subscriber
            oldFieldEvents.Clear();
            newFieldEvents.Clear();

            root.Write("new", 42);

            Assert.That(oldFieldEvents.Count, Is.EqualTo(1));
            Assert.That(newFieldEvents.Count, Is.EqualTo(1));
            Assert.That(newFieldEvents[^1].Event, Is.EqualTo(StorageEvent.Write));
            Assert.That(newFieldEvents[^1].Path, Is.EqualTo("new"));
        }

        [Test]
        public void Move_Multiple_Fields_Stress_Order_And_Data_Integrity()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Prepare multiple fields of various types
            root.Write("a", 1);
            root.Write("b", 2L);
            root.Write("c", 3.0f);
            root.Write("d", true);
            root.WriteArray("arr", new[] { 7, 8, 9 });
            root.GetObject("obj").Write("x", 11);

            // Perform consecutive moves to exercise header resort and length changes
            root.Move("a", "aa");
            root.Move("b", "z");
            root.Move("arr", "arr2");
            root.Move("obj", "entity");
            root.Move("d", "flag");

            // Verify data intact and types preserved
            Assert.That(root.Read<int>("aa"), Is.EqualTo(1));
            Assert.That(root.Read<long>("z"), Is.EqualTo(2L));
            Assert.That(root.Read<float>("c"), Is.EqualTo(3.0f));
            Assert.That(root.Read<bool>("flag"), Is.True);

            Assert.That(root.ReadArray<int>("arr2"), Is.EqualTo(new[] { 7, 8, 9 }));

            var entity = root.GetObject("entity");
            Assert.That(entity.Read<int>("x"), Is.EqualTo(11));

            // Old names should not exist
            Assert.That(root.HasField("a"), Is.False);
            Assert.That(root.HasField("b"), Is.False);
            Assert.That(root.HasField("arr"), Is.False);
            Assert.That(root.HasField("obj"), Is.False);
            Assert.That(root.HasField("d"), Is.False);
        }
    }
}
