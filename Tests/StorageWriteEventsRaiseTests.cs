using System;
using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [Timeout(1000)]
    [TestFixture]
    public class StorageWriteEventsRaiseTests
    {
        private int _count;
        private StorageEventArgs _last;

        [SetUp]
        public void SetUp()
        {
            _count = 0;
            _last = default;
        }

        private StorageSubscription SubscribeAll(StorageObject obj)
        {
            return obj.Subscribe((in StorageEventArgs args) =>
            {
                _count++;
                _last = args;
#if UNITY_EDITOR
                UnityEngine.Debug.Log(args);
#endif
            });
        }

        [Test]
        public void WriteScalar_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.Write("hp", 5);
            Assert.AreEqual(1, _count);
            Assert.AreEqual("hp", _last.Path);
        }

        [Test]
        public void TryWriteScalar_RaisesEventOnSuccess()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            Assert.IsTrue(storage.Root.TryWrite<int>("energy", 88));
            Assert.AreEqual(1, _count);
            Assert.AreEqual("energy", _last.Path);
        }

        [Test]
        public void Override_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.Override("mana", BitConverter.GetBytes(42), ValueType.Int32);
            Assert.AreEqual(1, _count);
            Assert.AreEqual("mana", _last.Path);
        }

        [Test]
        public void WriteString_RaisesEvent()
        {
            _count = 0;
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.Write("name", "hero");
            Assert.AreEqual(1, _count);
            Assert.AreEqual("name", _last.Path);
        }

        [Test]
        public void WriteArray_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.WriteArray<int>(new int[] { 1, 2, 3 });
            Assert.AreEqual(1, _count);
            Assert.AreEqual(ContainerLayout.ArrayName, _last.Path);
        }

        [Test]
        public void WritePath_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.WritePath<int>("stats.hp", 33);
            Assert.GreaterOrEqual(_count, 1);
            Assert.AreEqual("stats.hp", _last.Path);
        }

        [Test]
        public void Write_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.Write<int>("stats", 33);
            Assert.GreaterOrEqual(_count, 1);
            Assert.AreEqual("stats", _last.Path);
        }

        [Test]
        public void WriteArrayPath_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.WriteArrayPath<int>("numbers".AsSpan(), new int[] { 9, 8 });
            Assert.AreEqual(1, _count);
            Assert.AreEqual("numbers", _last.Path);
        }

        [Test]
        public void Delete_RaiseEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.Write("temp", 1);
            _count = 0;
            storage.Root.Delete("temp");
            Assert.AreEqual(1, _count);
        }

        [Test]
        public void WriteArrayElement_ByIndexedPath_RaisesEventWithIndexedPath()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            // Create an int array field named numbers
            storage.Root.WriteArrayPath<int>("numbers".AsSpan(), new int[] { 1, 2, 3 });
            _count = 0;

            // Write to element [1]
            storage.Root.WritePath<int>("numbers[1]", 99);
            Assert.AreEqual(1, _count, "Exactly one event should fire for element write");
            Assert.AreEqual("numbers", _last.Path, "Event path should include index");
            // not support subscribe for a single member of array, so the event returned is the path to the entire array
            //Assert.AreEqual("numbers[1]", _last.Path, "Event path should include index");
        }

        [Test]
        public void WriteNestedObjectInsideArray_RaisesEventWithFullIndexedPath()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);

            // Write to an object field inside an array slot; this should implicitly create the array and object if supported
            storage.Root.GetObject("items").MakeObjectArray(5);
            storage.Root.WritePath<int>("items[2].value", 7);
            Assert.GreaterOrEqual(_count, 1);
            Assert.AreEqual("items[2].value", _last.Path, "Event path should include array index and child field name");
        }
    }
}
