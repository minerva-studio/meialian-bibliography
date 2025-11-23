using System;
using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageComplexPathTests
    {
        private Storage Create() => new Storage(ContainerLayout.Empty);

        [Test]
        public void WriteAndRead_Scalar_DeepPath()
        {
            using var s = Create();
            s.Root.WritePath<int>("a.b.c", 42);
            Assert.AreEqual(42, s.Root.ReadPath<int>("a.b.c"));
        }

        [Test]
        public void WriteAndRead_String_DeepPath()
        {
            using var s = Create();
            s.Root.WritePath("profile.info.name", "Minerva");
            Assert.AreEqual("Minerva", s.Root.ReadStringPath("profile.info.name"));
        }

        [Test]
        public void WriteAndRead_Array_DeepPath()
        {
            using var s = Create();
            int[] arr = { 1, 2, 3, 4 };
            s.Root.WriteArrayPath<int>("stats.levels", arr);
            var read = s.Root.ReadArrayPath<int>("stats.levels");
            CollectionAssert.AreEqual(arr, read);
        }

        [Test]
        public void Mixed_CreateIntermediateObjects()
        {
            using var s = Create();
            s.Root.WritePath<int>("settings.audio.volume", 75);
            s.Root.WritePath("settings.audio.device", "Speaker");
            s.Root.WriteArrayPath<int>("settings.video.resolutions", new[] { 720, 1080, 1440 });
            Assert.AreEqual(75, s.Root.ReadPath<int>("settings.audio.volume"));
            Assert.AreEqual("Speaker", s.Root.ReadStringPath("settings.audio.device"));
            CollectionAssert.AreEqual(new[] { 720, 1080, 1440 }, s.Root.ReadArrayPath<int>("settings.video.resolutions"));
        }

        [Test]
        public void IndexedArrayPath_ReadWrite_ScalarElements()
        {
            using var s = Create();
            s.Root.WriteArrayPath<int>("inventory.counts", new[] { 5, 6, 7 });
            s.Root.WritePath<int>("inventory.meta.version", 3);
            var counts = s.Root.ReadArrayPath<int>("inventory.counts");
            CollectionAssert.AreEqual(new[] { 5, 6, 7 }, counts);
            Assert.AreEqual(3, s.Root.ReadPath<int>("inventory.meta.version"));
        }

        [Test]
        public void ArrayOfObjects_ManualIndexAccess()
        {
            using var s = Create();
            // create an object array container under "list"
            var listContainer = s.Root.GetObject("list");
            var arrayField = listContainer.MakeObjectArray(3); // assume extension exists: creates ref array of 3 child objects
            for (int i = 0; i < 3; i++)
            {
                var child = arrayField.GetObject(i);
                child.Write("value", i + 10);
            }
            var mid = s.Root.GetMember("list");
            Assert.IsTrue(mid.IsArray);
            var readArr = mid.AsArray();
            Assert.AreEqual(3, readArr.Length);
            Assert.AreEqual(11, readArr.GetObject(1).Read<int>("value"));
        }

        [Test]
        public void BlobWrite()
        {
            using var s = Create();
            byte[] payload = { 0xDE, 0xAD, 0xBE, 0xEF };
            s.Root.GetObjectByPath("data.blob").Override("bin", payload, ValueType.Blob);
            var member = s.Root.GetMember("data.blob.bin");
            var view = member.AsScalar();
            Assert.AreEqual(ValueType.Blob, view.Type);
            // since no direct ReadBytes API, reconstruct via span copy
            Span<byte> buffer = stackalloc byte[view.Bytes.Length];
            view.Bytes.CopyTo(buffer);
            CollectionAssert.AreEqual(payload, buffer.ToArray());
        }

        [Test]
        public void Complex_MixedTypes_Chain()
        {
            using var s = Create();
            s.Root.WritePath<int>("world.players.count", 8);
            s.Root.WritePath("world.players.title", "Adventurer");
            s.Root.WriteArrayPath<int>("world.players.ids", new[] { 100, 101 });
            s.Root.WritePath<double>("world.metrics.fps", 59.9);
            s.Root.WritePath<float>("world.metrics.delta", 0.016f);
            Assert.AreEqual(8, s.Root.ReadPath<int>("world.players.count"));
            Assert.AreEqual("Adventurer", s.Root.ReadStringPath("world.players.title"));
            CollectionAssert.AreEqual(new[] { 100, 101 }, s.Root.ReadArrayPath<int>("world.players.ids"));
            Assert.AreEqual(59.9, s.Root.ReadPath<double>("world.metrics.fps"), 0.0001);
            Assert.AreEqual(0.016f, s.Root.ReadPath<float>("world.metrics.delta"), 0.000001f);
        }

        [Test]
        public void PathWithIndex_ObjectArray_ElementField_WriteRead()
        {
            using var s = Create();
            // Prepare a.b as object holding an array container
            var a = s.Root.GetObject("a");
            var b = a.GetObject("b");
            // Turn b into an object array of length 3
            b.MakeArray(TypeData.Ref, 3);
            // Allocate element 1 container
            var arr = b.AsArray();
            arr.GetObject(1); // create container at index 1

            // Write scalar through indexed path
            s.Root.WritePath<int>("a.b[1].c", 314);
            Assert.AreEqual(314, s.Root.ReadPath<int>("a.b[1].c"));
        }

        [Test]
        public void PathWithIndex_ObjectArray_ElementString_WriteRead()
        {
            using var s = Create();
            var a = s.Root.GetObject("a");
            var b = a.GetObject("b");
            b.MakeArray(TypeData.Ref, 2);
            var arr = b.AsArray();
            arr.GetObject(1);

            s.Root.WritePath("a.b[1].name", "Node");
            Assert.AreEqual("Node", s.Root.ReadStringPath("a.b[1].name"));
        }

        [Test]
        public void PathWithIndex_MissingElement_WillNotThrow()
        {
            using var s = Create();
            var a = s.Root.GetObject("a");
            var b = a.GetObject("b");
            b.MakeArray(TypeData.Ref, 2);
            // auto allocate element 1
            Assert.DoesNotThrow(() => s.Root.WritePath<int>("a.b[1].value", 7));
        }
    }
}
