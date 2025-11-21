using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageIndexerTests
    {
        [Test]
        public void RootIndexer_ReturnsMember()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            storage.Root.Write("health", 10);
            var m = storage["health"];
            Assert.AreEqual(ValueType.Int32, m.ValueType);
            Assert.AreEqual(10, m.Read<int>());
        }

        [Test]
        public void MemberIndexer_NestedPath()
        {
            var storage = new Storage(ContainerLayout.Empty);
            storage.Root.Write("a", 1);
            storage.Root.GetObject("b").Write("c", 123);
            var b = storage["b"];
            var c = b["c"];
            Assert.AreEqual(123, c.Read<int>());
        }

        [Test]
        public void TryGetMember_PathFound()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            storage.Root.Write("x", 7);
            Assert.IsTrue(storage.TryGetMember("x", out var member));
            Assert.AreEqual(7, member.Read<int>());
        }

        [Test]
        public void TryGetMember_PathMissing()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            Assert.IsFalse(storage.TryGetMember("nope", out var _));
        }
    }
}
