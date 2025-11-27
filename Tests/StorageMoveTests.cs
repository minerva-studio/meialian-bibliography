using System;
using System.Collections.Generic;
using NUnit.Framework;

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
    }
}
