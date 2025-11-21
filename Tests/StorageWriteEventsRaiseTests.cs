using System;
using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageWriteEventsRaiseTests
    {
        private int _count;
        private StorageFieldWriteEventArgs _last;

        [SetUp]
        public void SetUp()
        {
            _count = 0;
            _last = default;
        }

        private StorageWriteSubscription SubscribeAll(StorageObject obj)
        {
            return obj.Subscribe((in StorageFieldWriteEventArgs args) => { _count++; _last = args; });
        }

        [Test]
        public void WriteScalar_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.Write("hp", 5);
            Assert.AreEqual(1, _count);
            Assert.AreEqual("hp", _last.FieldName);
        }

        [Test]
        public void TryWriteScalar_RaisesEventOnSuccess()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            Assert.IsTrue(storage.Root.TryWrite<int>("energy", 88));
            Assert.AreEqual(1, _count);
            Assert.AreEqual("energy", _last.FieldName);
        }

        [Test]
        public void Override_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.Override("mana", BitConverter.GetBytes(42), ValueType.Int32);
            Assert.AreEqual(1, _count);
            Assert.AreEqual("mana", _last.FieldName);
        }

        [Test]
        public void WriteString_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.Write("name", "hero");
            Assert.AreEqual(1, _count);
            Assert.AreEqual("name", _last.FieldName);
        }

        [Test]
        public void WriteArray_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.WriteArray<int>(new int[] { 1, 2, 3 });
            Assert.AreEqual(1, _count);
            Assert.AreEqual(ContainerLayout.ArrayName, _last.FieldName);
        }

        [Test]
        public void WritePath_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.WritePath<int>("stats.hp".AsSpan(), 33);
            Assert.GreaterOrEqual(_count, 1);
            Assert.AreEqual("hp", _last.FieldName);
        }

        [Test]
        public void WriteArrayPath_RaisesEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.WriteArrayPath<int>("numbers".AsSpan(), new int[] { 9, 8 });
            Assert.AreEqual(1, _count);
            Assert.AreEqual("numbers", _last.FieldName);
        }

        [Test]
        public void Delete_DoesNotRaiseEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var sub = SubscribeAll(storage.Root);
            storage.Root.Write("temp", 1);
            _count = 0;
            storage.Root.Delete("temp");
            Assert.AreEqual(0, _count);
        }
    }
}
