using Minerva.DataStorage.Serialization;
using NUnit.Framework;
using System.Collections.Generic;
namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class SerializationEventTests
    {
        [Test]
        public void Events_Fire_After_Binary_Snapshot_Restore()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var obj = root.GetObject("obj");

            obj.Write("a", 1);

            var bytes = storage.ToBinary();

            using Storage newStorage = BinarySerialization.Parse(bytes);
            var restored = newStorage.Root;

            var events = new List<StorageEventArgs>();
            using var sub = restored.Subscribe((in StorageEventArgs e) => events.Add(e));

            restored.Write("a", 2);
            Assert.That(events.Count, Is.GreaterThanOrEqualTo(1));
            Assert.That(events[^1].Event, Is.EqualTo(StorageEvent.Write));
            Assert.That(events[^1].Path, Is.EqualTo("a"));

            restored.Move("a", "b");
            Assert.That(events.Count, Is.GreaterThanOrEqualTo(2));
            Assert.That(events[^1].Event, Is.EqualTo(StorageEvent.Rename));
            Assert.That(events[^1].Path, Is.EqualTo("b"));

            restored.Delete("b");
            Assert.That(events.Count, Is.GreaterThanOrEqualTo(3));
            Assert.That(events[^1].Event, Is.EqualTo(StorageEvent.Delete));
            Assert.That(events[^1].Path, Is.EqualTo("b"));
        }
    }
}
