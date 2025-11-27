using System;
using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageObjectPathTests
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
            var view = member.AsScalar().ToValueView();
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

        [Test]
        public void Storage_Path_SingleField_Write_And_Read()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("hp", 42);

            Assert.That(root.Read<int>("hp"), Is.EqualTo(42));
            Assert.That(root.ReadPath<int>("hp"), Is.EqualTo(42));
        }

        [Test]
        public void Storage_Path_Nested_Object_Write_And_Read()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("persistent.entity.mamaRhombear.killed", 5);

            var persistent = root.GetObject("persistent");
            var entity = persistent.GetObject("entity");
            var mama = entity.GetObject("mamaRhombear");

            Assert.That(mama.Read<int>("killed"), Is.EqualTo(5));

            var value = root.ReadPath<int>("persistent.entity.mamaRhombear.killed");
            Assert.That(value, Is.EqualTo(5));
        }

        [Test]
        public void Storage_Path_Read_Missing_Segment_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.Throws<ArgumentException>(() => root.ReadPath<int>("missing.hp"));
        }

        [Test]
        public void Storage_Path_String_Write_And_Read()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var big = new string('x', 148);
            root.WritePath("persistent.entity.mamaRhombear.greetings.helloMessage", big);

            var result = root.ReadStringPath("persistent.entity.mamaRhombear.greetings.helloMessage");
            Assert.That(result, Is.EqualTo(big));
        }

        [Test]
        public void Storage_Path_Array_Write_And_Read()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var speeds = new[] { 1.0f, 2.5f, 3.75f };
            root.WriteArrayPath("stats.speeds", speeds);

            var back = root.ReadArrayPath<float>("stats.speeds");
            CollectionAssert.AreEqual(speeds, back);
        }

        [Test]
        public void Storage_Path_Write_Read_Different_Scalar_Types()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Test various scalar types
            root.WritePath("player.stats.hp", 100);
            root.WritePath("player.stats.mana", 50.5f);
            root.WritePath("player.stats.experience", 12345.678);
            root.WritePath("player.stats.isAlive", true);
            root.WritePath("player.stats.level", (byte)42);
            root.WritePath("player.stats.armor", (short)15);
            root.WritePath("player.stats.gold", 999999L);

            Assert.That(root.ReadPath<int>("player.stats.hp"), Is.EqualTo(100));
            Assert.That(root.ReadPath<float>("player.stats.mana"), Is.EqualTo(50.5f));
            Assert.That(root.ReadPath<double>("player.stats.experience"), Is.EqualTo(12345.678));
            Assert.That(root.ReadPath<bool>("player.stats.isAlive"), Is.True);
            Assert.That(root.ReadPath<byte>("player.stats.level"), Is.EqualTo((byte)42));
            Assert.That(root.ReadPath<short>("player.stats.armor"), Is.EqualTo((short)15));
            Assert.That(root.ReadPath<long>("player.stats.gold"), Is.EqualTo(999999L));
        }

        [Test]
        public void Storage_Path_Array_Different_Types()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var intArray = new[] { 1, 2, 3, 4, 5 };
            var floatArray = new[] { 1.1f, 2.2f, 3.3f };
            var doubleArray = new[] { 10.5, 20.5, 30.5, 40.5 };
            var boolArray = new[] { true, false, true, false, true };
            var byteArray = new byte[] { 0x01, 0x02, 0x03, 0xFF };

            root.WriteArrayPath("data.integers", intArray);
            root.WriteArrayPath("data.floats", floatArray);
            root.WriteArrayPath("data.doubles", doubleArray);
            root.WriteArrayPath("data.booleans", boolArray);
            root.WriteArrayPath("data.bytes", byteArray);

            CollectionAssert.AreEqual(intArray, root.ReadArrayPath<int>("data.integers"));
            CollectionAssert.AreEqual(floatArray, root.ReadArrayPath<float>("data.floats"));
            CollectionAssert.AreEqual(doubleArray, root.ReadArrayPath<double>("data.doubles"));
            CollectionAssert.AreEqual(boolArray, root.ReadArrayPath<bool>("data.booleans"));
            CollectionAssert.AreEqual(byteArray, root.ReadArrayPath<byte>("data.bytes"));
        }

        [Test]
        public void Storage_Path_Custom_Separator()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("a/b/c/value", 42, '/');
            root.WriteArrayPath<float>("x/y/z/array", new[] { 1.0f, 2.0f, 3.0f }, '/');
            root.WritePath("path/to/string", "hello", '/');

            Assert.That(root.ReadPath<int>("a/b/c/value", '/'), Is.EqualTo(42));
            CollectionAssert.AreEqual(new[] { 1.0f, 2.0f, 3.0f }, root.ReadArrayPath<float>("x/y/z/array", '/'));
            Assert.That(root.ReadStringPath("path/to/string", '/'), Is.EqualTo("hello"));
        }

        [Test]
        public void Storage_Path_TryReadByPath_Success()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("level1.level2.value", 99);

            Assert.That(root.TryReadPath<int>("level1.level2.value", out var value), Is.True);
            Assert.That(value, Is.EqualTo(99));
        }

        [Test]
        public void Storage_Path_TryReadByPath_Missing_Returns_False()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.That(root.TryReadPath<int>("missing.path.value", out var value), Is.False);
            Assert.That(value, Is.EqualTo(0)); // default value
        }

        [Test]
        public void Storage_Path_Overwrite_Value()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("config.setting", 10);
            Assert.That(root.ReadPath<int>("config.setting"), Is.EqualTo(10));

            root.WritePath("config.setting", 20);
            Assert.That(root.ReadPath<int>("config.setting"), Is.EqualTo(20));
        }

        [Test]
        public void Storage_Path_Overwrite_Array()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var array1 = new[] { 1, 2, 3 };
            var array2 = new[] { 4, 5, 6, 7, 8 };

            root.WriteArrayPath("data.numbers", array1);
            CollectionAssert.AreEqual(array1, root.ReadArrayPath<int>("data.numbers"));

            root.WriteArrayPath("data.numbers", array2);
            CollectionAssert.AreEqual(array2, root.ReadArrayPath<int>("data.numbers"));
        }

        [Test]
        public void Storage_Path_Empty_Array()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var emptyArray = new int[0];
            root.WriteArrayPath("data.empty", emptyArray);

            var back = root.ReadArrayPath<int>("data.empty");
            Assert.That(back.Length, Is.EqualTo(0));
        }

        [Test]
        public void Storage_Path_Large_Array()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var largeArray = new float[1000];
            for (int i = 0; i < largeArray.Length; i++)
            {
                largeArray[i] = i * 0.5f;
            }

            root.WriteArrayPath("data.large", largeArray);
            var back = root.ReadArrayPath<float>("data.large");

            Assert.That(back.Length, Is.EqualTo(1000));
            CollectionAssert.AreEqual(largeArray, back);
        }

        [Test]
        public void Storage_Path_Very_Deep_Nesting()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var deepPath = "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.value";
            root.WritePath(deepPath, 777);

            Assert.That(root.ReadPath<int>(deepPath), Is.EqualTo(777));
        }

        [Test]
        public void Storage_Path_Mixed_Operations_Same_Path()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Write scalar, then array, then string to different fields in same path
            root.WritePath("entity.stats.hp", 100);
            root.WriteArrayPath("entity.stats.buffs", new[] { 1.0f, 2.0f });
            root.WritePath("entity.stats.name", "Player1");

            Assert.That(root.ReadPath<int>("entity.stats.hp"), Is.EqualTo(100));
            CollectionAssert.AreEqual(new[] { 1.0f, 2.0f }, root.ReadArrayPath<float>("entity.stats.buffs"));
            Assert.That(root.ReadStringPath("entity.stats.name"), Is.EqualTo("Player1"));
        }

        [Test]
        public void Storage_Path_GetObjectByPath()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("level1.level2.value", 42);

            var level1 = root.GetObjectByPath("level1");
            Assert.That(level1.IsNull, Is.False);

            var level2 = root.GetObjectByPath("level1.level2");
            Assert.That(level2.IsNull, Is.False);
            Assert.That(level2.Read<int>("value"), Is.EqualTo(42));
        }

        [Test]
        public void Storage_Path_Read_Missing_Field_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("level1.level2", 10); // Creates level1.level2 as a scalar, not an object

            // When trying to navigate through a scalar field, it throws ArgumentException
            Assert.Throws<ArgumentException>(() => root.ReadPath<int>("level1.level2.missing"));
        }

        [Test]
        public void Storage_Path_Read_Array_Missing_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.Throws<ArgumentException>(() => root.ReadArrayPath<int>("missing.path.array"));
        }

        [Test]
        public void Storage_Path_Read_String_Missing_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.Throws<ArgumentException>(() => root.ReadStringPath("missing.path.string"));
        }

        [Test]
        public void Storage_Path_Empty_Path_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.Throws<ArgumentException>(() => root.ReadPath<int>(""));
            Assert.Throws<ArgumentException>(() => root.WritePath("", 10));
        }

        [Test]
        public void Storage_Path_Null_Path_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.Throws<ArgumentNullException>(() => root.ReadPath<int>(null));
            Assert.Throws<ArgumentNullException>(() => root.WritePath<int>(null, 10));
            Assert.Throws<ArgumentNullException>(() => root.WritePath(null, "test"));
        }

        [Test]
        public void Storage_Path_Multiple_Reads_Same_Path()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("shared.value", 123);

            for (int i = 0; i < 10; i++)
            {
                Assert.That(root.ReadPath<int>("shared.value"), Is.EqualTo(123));
            }
        }

        [Test]
        public void Storage_Path_Complex_Nested_Structure()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Create a complex nested structure
            root.WritePath("game.player.stats.hp", 100);
            root.WritePath("game.player.stats.mana", 50);
            root.WriteArrayPath("game.player.stats.inventory", new[] { 1, 2, 3, 4, 5 });
            root.WritePath("game.player.name", "Hero");
            root.WritePath("game.enemy.stats.hp", 50);
            root.WriteArrayPath("game.enemy.stats.abilities", new[] { 10.0f, 20.0f, 30.0f });
            root.WritePath("game.level", 5);

            // Verify all values
            Assert.That(root.ReadPath<int>("game.player.stats.hp"), Is.EqualTo(100));
            Assert.That(root.ReadPath<int>("game.player.stats.mana"), Is.EqualTo(50));
            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4, 5 }, root.ReadArrayPath<int>("game.player.stats.inventory"));
            Assert.That(root.ReadStringPath("game.player.name"), Is.EqualTo("Hero"));
            Assert.That(root.ReadPath<int>("game.enemy.stats.hp"), Is.EqualTo(50));
            CollectionAssert.AreEqual(new[] { 10.0f, 20.0f, 30.0f }, root.ReadArrayPath<float>("game.enemy.stats.abilities"));
            Assert.That(root.ReadPath<int>("game.level"), Is.EqualTo(5));
        }

        [Test]
        public void Storage_Path_Array_Size_Change()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Write small array
            root.WriteArrayPath("data.items", new[] { 1, 2 });
            Assert.That(root.ReadArrayPath<int>("data.items").Length, Is.EqualTo(2));

            // Overwrite with larger array
            root.WriteArrayPath("data.items", new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
            var back = root.ReadArrayPath<int>("data.items");
            Assert.That(back.Length, Is.EqualTo(10));
            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, back);

            // Overwrite with smaller array
            root.WriteArrayPath("data.items", new[] { 100 });
            Assert.That(root.ReadArrayPath<int>("data.items").Length, Is.EqualTo(1));
            Assert.That(root.ReadArrayPath<int>("data.items")[0], Is.EqualTo(100));
        }

        [Test]
        public void Storage_Path_String_Array_Write_Read()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var text = "Hello, World! This is a test string.";
            root.WritePath("messages.greeting", text);

            var back = root.ReadStringPath("messages.greeting");
            Assert.That(back, Is.EqualTo(text));
        }

        [Test]
        public void Storage_Path_Unicode_String()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var unicode = "Hello 世界 🌍 测试";
            root.WritePath("text.unicode", unicode);

            var back = root.ReadStringPath("text.unicode");
            Assert.That(back, Is.EqualTo(unicode));
        }

        [Test]
        public void Storage_Path_Read_Write_All_Numeric_Types()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("types.sbyte", (sbyte)-50);
            root.WritePath("types.byte", (byte)200);
            root.WritePath("types.short", (short)-1000);
            root.WritePath("types.ushort", (ushort)5000);
            root.WritePath("types.int", -100000);
            root.WritePath("types.uint", 200000U);
            root.WritePath("types.long", -1000000L);
            root.WritePath("types.ulong", 2000000UL);
            root.WritePath("types.float", 3.14f);
            root.WritePath("types.double", 2.71828);

            Assert.That(root.ReadPath<sbyte>("types.sbyte"), Is.EqualTo((sbyte)-50));
            Assert.That(root.ReadPath<byte>("types.byte"), Is.EqualTo((byte)200));
            Assert.That(root.ReadPath<short>("types.short"), Is.EqualTo((short)-1000));
            Assert.That(root.ReadPath<ushort>("types.ushort"), Is.EqualTo((ushort)5000));
            Assert.That(root.ReadPath<int>("types.int"), Is.EqualTo(-100000));
            Assert.That(root.ReadPath<uint>("types.uint"), Is.EqualTo(200000U));
            Assert.That(root.ReadPath<long>("types.long"), Is.EqualTo(-1000000L));
            Assert.That(root.ReadPath<ulong>("types.ulong"), Is.EqualTo(2000000UL));
            Assert.That(root.ReadPath<float>("types.float"), Is.EqualTo(3.14f));
            Assert.That(root.ReadPath<double>("types.double"), Is.EqualTo(2.71828));
        }
    }
}
