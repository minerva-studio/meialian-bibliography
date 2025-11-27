using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class VersioningTests
    {
        [Test]
        public void EachObjectManagesItsOwnVersion()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var player = root.GetObject("player");
            var stats = player.GetObject("stats");

            player.Version = 1;
            stats.Version = 3;

            Assert.AreEqual(1, player.Version, "player.Version should be independent");
            Assert.AreEqual(3, stats.Version, "stats.Version should be independent");
        }

        [Test]
        public void ReadWriteDoesNotChangeVersion()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var obj = root.GetObject("obj");

            obj.Version = 10;
            obj.Write("x", 123);
            Assert.AreEqual(10, obj.Version, "Write should not change Version");

            var value = obj.Read<int>("x");
            Assert.AreEqual(123, value);
            Assert.AreEqual(10, obj.Version, "Read should not change Version");

            bool ok = obj.TryRead<int>("x", out var v2);
            Assert.IsTrue(ok);
            Assert.AreEqual(123, v2);
            Assert.AreEqual(10, obj.Version, "TryRead should not change Version");
        }

        [Test]
        public void MoveRenameDeleteDoesNotChangeVersion()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var obj = root.GetObject("obj");
            obj.Version = 2;

            obj.Write("name", 1);
            obj.Move("name", "displayName");
            Assert.AreEqual(2, obj.Version, "Move should not change Version");
            Assert.AreEqual(1, obj.Read<int>("displayName"));

            obj.Write("temp", 5);
            int before = obj.Version;
            bool deleted = obj.Delete("temp");
            Assert.IsTrue(deleted);
            Assert.AreEqual(before, obj.Version, "Delete should not change Version");
        }

        [Test]
        public void PathApisAffectOnlyTargetObject()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var player = root.GetObject("player");
            var stats = player.GetObject("stats");

            player.Version = 1;
            stats.Version = 0;

            // WritePath to stats
            player.WritePath<int>("stats.hp", 100);
            Assert.AreEqual(1, player.Version, "parent Version unchanged");
            Assert.AreEqual(0, stats.Version, "child Version unchanged by write");

            // ReadPath from stats
            var hp = player.ReadPath<int>("stats.hp");
            Assert.AreEqual(100, hp);
            Assert.AreEqual(1, player.Version);
            Assert.AreEqual(0, stats.Version);
        }
    }
}
