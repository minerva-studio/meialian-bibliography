using NUnit.Framework;
using System;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StoragePublicApiTests
    {
        // root: int hp; ref child; float[4] speeds
        private ContainerLayout _rootLayout;

        // leaf: int hp
        private ContainerLayout _leafLayout;

        [SetUp]
        public void Setup()
        {
            // root
            {
                var ob = new ObjectBuilder();
                ob.SetScalar<int>("hp");
                ob.SetRef("child", 0UL);
                ob.SetArray<float>("speeds", 4);
                _rootLayout = ob.BuildLayout();
            }

            // leaf
            {
                var ob = new ObjectBuilder();
                ob.SetScalar<int>("hp");
                _leafLayout = ob.BuildLayout();
            }
        }

        [TearDown]
        public void Teardown() { }

        // 1) Storage lifetime: root is usable; Dispose() recursively unregisters.
        [Test]
        public void Storage_Root_IsUsable_And_Dispose_Unregisters()
        {
            var storage = new Storage(_rootLayout);
            var root = storage.Root; // ref struct view

            root.Write<int>("hp", 123);
            Assert.That(root.Read<int>("hp"), Is.EqualTo(123));

            storage.Dispose();

            var threw = false;
            try { root.Read<int>("hp"); }
            catch (InvalidOperationException) { threw = true; }
            Assert.That(threw, Is.True, "Expected InvalidOperationException was not thrown.");
        }

        // 4) StorageObject: GetObjectNoAllocate returns null view when child is null
        [Test]
        public void StorageObject_GetObject_Return_Null_When_Child_Is_Null()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var threw = false;
            StorageObject child = default;
            try
            {
                child = root.GetObjectNoAllocate("child");
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Assert.That(threw, Is.False, "Unexpected InvalidOperationException.");
            Assert.That(child.IsNull, "Expected null was returned.");
        }

        // 5) Dispose should recursively unregister children; IDs should no longer resolve
        [Test]
        public void Storage_Dispose_Recursively_Unregisters_Children()
        {
            var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var child = root.GetObject("child", false, _leafLayout);
            child.Write<int>("hp", 7);
            var childId = child.ID;

            storage.Dispose();

            var reg = Container.Registry.Shared;
            Assert.That(reg.GetContainer(childId), Is.Null);
        }

        [Test]
        public void Storage_FieldAutoExpand()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            root.Write<byte>("v", 1);
            Assert.That(root.GetField("v").Length, Is.EqualTo(1));

            root.Write<int>("v", 1);
            Assert.That(root.GetField("v").Length, Is.EqualTo(4));
        }

        [Test]
        public void Storage_FieldTwoWay()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            const string f1 = "f1";
            root.Write<int>(f1, 1);
            Assert.That(root.GetField(f1).Length, Is.EqualTo(4));
            root.Write<float>(f1, 1f);
            Assert.That(root.GetField(f1).Length, Is.EqualTo(4));
            Assert.That(root.GetValueView(f1).Type, Is.EqualTo(ValueType.Float32)); // same size, change type

            const string f2 = "f2";
            root.Write<float>(f2, 1f);
            Assert.That(root.GetField(f2).Length, Is.EqualTo(4));
            root.Write<int>(f2, 1);
            Assert.That(root.GetField(f2).Length, Is.EqualTo(4));
            Assert.That(root.GetValueView(f2).Type, Is.EqualTo(ValueType.Float32)); // int can implicit to float
        }

        [Test]
        public void Storage_WriteNoRescheme_FieldNoExpand_WillThrow()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            root.Write<byte>("v", 1);
            Assert.That(root.GetField("v").Length, Is.EqualTo(1));

            bool threw = false;
            try { root.WriteNoRescheme<int>("v", 1); }
            catch (IndexOutOfRangeException) { threw = true; }
            Assert.That(threw, Is.True);

            root.Write<float>("v", 1000.0f);
            Assert.That(root.GetField("v").Length, Is.EqualTo(4));

            threw = false;
            try { root.WriteNoRescheme<double>("v", 1000.0); }
            catch (IndexOutOfRangeException) { threw = true; }
            Assert.That(threw, Is.True);
        }

        [Test]
        public void Storage_WriteNoRescheme_FieldNoExpand_FieldTypeConstant()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            root.Write<double>("v", 1000.0);
            Assert.That(root.GetField("v").Length, Is.EqualTo(8));
            var threw = false;
            try { root.WriteNoRescheme<int>("v", 1000); }
            catch (ArgumentException) { threw = true; }
            Assert.That(threw, Is.False);
            Assert.That(root.GetField("v").Length, Is.EqualTo(8));
            var view = root.GetValueView("v");
            Assert.That(view.Type, Is.EqualTo(ValueType.Float64));
        }

        [Test]
        public void Storage_WriteNoRescheme_FieldNoExpand_Safe()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;
            root.Write<int>("v", 1);
            Assert.That(root.GetField("v").Length, Is.EqualTo(4));

            bool threw = false;
            try { root.WriteNoRescheme<byte>("v", 1); }
            catch (ArgumentException) { threw = true; }
            Assert.That(threw, Is.False);
        }

        [Test]
        public void Storage_String_RoundTrip_ExternalLeaf_OK()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var big = new string('g', 276);
            root.Write("e", big);
            Assert.That(root.GetField("e").IsRef, Is.True);
            Assert.That(root.GetObject("e").IsArray, Is.True);
            Assert.That(root.GetObject("e").IsString, Is.True);
            Assert.That(root.GetObject("e").GetField(ContainerLayout.ArrayName).Length, Is.EqualTo(big.Length * sizeof(char)));
            var back = root.ReadString("e");

            Assert.That(back.Length, Is.EqualTo(big.Length));
            Assert.That(back, Is.EqualTo(big));

            var big2 = new string('a', 100);
            root.Write("e", big2);

            Assert.That(root.GetField("e").IsRef, Is.True);
            Assert.That(root.GetObject("e").IsArray, Is.True);
            Assert.That(root.GetObject("e").IsString, Is.True);
            Assert.That(root.GetObject("e").GetField(ContainerLayout.ArrayName).Length, Is.EqualTo(big2.Length * sizeof(char)));
            var back2 = root.ReadString("e");

            Assert.That(back2.Length, Is.EqualTo(big2.Length));
            Assert.That(back2, Is.EqualTo(big2));
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

        [Test]
        public void Storage_FieldWrite_Subscription_Fires_On_Write()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            int invoked = 0;

            root.Write("score", 0);
            using var subscription = root.Subscribe("score", (in StorageFieldWriteEventArgs args) =>
            {
                invoked++;
                Assert.That(args.FieldName, Is.EqualTo("score"));
                Assert.That(args.FieldType, Is.EqualTo(ValueType.Int32));
                Assert.That(args.Target.Read<int>("score"), Is.EqualTo(123));
            });

            root.Write("score", 123);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_Unsubscribes()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            int invoked = 0;

            root.Write("hp", 0);
            var subscription = root.Subscribe("hp", (in StorageFieldWriteEventArgs _) => invoked++);

            root.Write("hp", 10);
            Assert.That(invoked, Is.EqualTo(1));

            subscription.Dispose();
            root.Write("hp", 20);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Missing_Container_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.Throws<ArgumentException>(() => root.Subscribe("player", (in StorageFieldWriteEventArgs _) => { }));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_TryWriteFailure_DoesNotNotify()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write<byte>("small", 1);
            int invoked = 0;

            using var subscription = root.Subscribe("small", (in StorageFieldWriteEventArgs _) => invoked++);

            root.TryWrite<int>("small", 99, allowRescheme: false);
            Assert.That(invoked, Is.EqualTo(0));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_MultipleHandlers_AllInvoked()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            int a = 0;
            int b = 0;

            root.Write("score", 0);
            using var subA = root.Subscribe("score", (in StorageFieldWriteEventArgs _) => a++);
            using var subB = root.Subscribe("score", (in StorageFieldWriteEventArgs _) => b++);

            root.Write("score", 10);

            Assert.That(a, Is.EqualTo(1));
            Assert.That(b, Is.EqualTo(1));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_String_And_Array_Writes_Notify()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int stringInvoked = 0;
            root.Write("playerName", string.Empty);
            using var stringSub = root.Subscribe("playerName", (in StorageFieldWriteEventArgs args) =>
            {
                stringInvoked++;
                Assert.That(args.Target.ReadString(), Is.EqualTo("Hero"));
            });

            var stats = root.GetObject("stats");
            stats.WriteArray("speeds", Array.Empty<float>());
            int arrayInvoked = 0;
            using var arraySub = stats.Subscribe("speeds", (in StorageFieldWriteEventArgs args) =>
            {
                arrayInvoked++;
                CollectionAssert.AreEqual(new[] { 1.0f, 2.5f }, args.Target.ReadArray<float>());
            });

            root.Write("playerName", "Hero");
            stats.WriteArray("speeds", new[] { 1.0f, 2.5f });

            Assert.That(stringInvoked, Is.EqualTo(1));
            Assert.That(arrayInvoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_Only_Target_Fires()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);
            root.Write("hp", 0);

            int scoreInvoked = 0;
            int hpInvoked = 0;

            using var scoreSub = root.Subscribe("score", (in StorageFieldWriteEventArgs _) => scoreInvoked++);
            using var hpSub = root.Subscribe("hp", (in StorageFieldWriteEventArgs _) => hpInvoked++);

            root.Write("score", 10);
            root.Write("hp", 5);

            Assert.That(scoreInvoked, Is.EqualTo(1));
            Assert.That(hpInvoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_Custom_Separator()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var stats = root.GetObject("player").GetObject("stats");
            stats.Write("hp", 0);
            int invoked = 0;
            using var sub = root.Subscribe("player/stats/hp", (in StorageFieldWriteEventArgs _) => invoked++, '/');

            stats.Write("hp", 9);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_Missing_Intermediate_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.GetObject("player"); // but no stats

            Assert.Throws<ArgumentException>(() =>
                root.Subscribe("player.stats.hp", (in StorageFieldWriteEventArgs _) => { }));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_OnChildObject()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;
            var child = root.GetObject("child", layout: _leafLayout);
            int invoked = 0;

            using var sub = child.Subscribe("hp", (in StorageFieldWriteEventArgs args) =>
            {
                invoked++;
                Assert.That(args.Target.ID, Is.EqualTo(child.ID));
                Assert.That(args.Target.Read<int>("hp"), Is.EqualTo(55));
            });

            child.Write("hp", 55);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Name_Equals_Child_Subscribe()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.GetObject("entity");
            int viaRoot = 0;
            using var rootSub = root.Subscribe("entity", (in StorageFieldWriteEventArgs _) => viaRoot++);

            var entity = root.GetObject("entity");
            int viaChild = 0;
            using var childSub = entity.Subscribe((in StorageFieldWriteEventArgs _) => viaChild++);

            entity.Write("hp", 10);

            Assert.That(viaRoot, Is.EqualTo(1));
            Assert.That(viaChild, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_EmptyString_Targets_Current_Container()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int invoked = 0;
            using var sub = root.Subscribe("", (in StorageFieldWriteEventArgs _) => invoked++);

            root.Write("hp", 5);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_Equals_Nested_Subscribe()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var persistent = root.GetObject("persistent");
            persistent.GetObject("entity");
            int viaPath = 0;
            using var pathSub = root.Subscribe("persistent.entity", (in StorageFieldWriteEventArgs _) => viaPath++);

            var nested = root.GetObject("persistent");
            int viaNested = 0;
            using var nestedSub = nested.Subscribe("entity", (in StorageFieldWriteEventArgs _) => viaNested++);

            nested.GetObject("entity").Write("hp", 9);

            Assert.That(viaPath, Is.EqualTo(1));
            Assert.That(viaNested, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_Must_Exist()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.Throws<ArgumentException>(() =>
                root.Subscribe("missing", (in StorageFieldWriteEventArgs _) => { }));

            root.Write("existing", 1);
            int invoked = 0;
            using var sub = root.Subscribe("existing", (in StorageFieldWriteEventArgs _) => invoked++);

            root.Write("existing", 2);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_Navigates_To_Child()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.GetObject("entity").GetObject("child");
            int invoked = 0;
            using var sub = root.Subscribe("entity.child", (in StorageFieldWriteEventArgs _) => invoked++);

            var child = root.GetObject("entity").GetObject("child");
            child.Write("hp", 3);

            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_To_Field()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var stats = root.GetObject("player").GetObject("stats");
            stats.Write("hp", 0);

            int invoked = 0;
            using var sub = root.Subscribe("player.stats.hp", (in StorageFieldWriteEventArgs args) =>
            {
                invoked++;
                Assert.That(args.FieldName, Is.EqualTo("hp"));
                Assert.That(args.Target.Read<int>("hp"), Is.EqualTo(42));
            });

            stats.Write("hp", 42);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Container_Subscription_Fires_For_All_Fields()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int invoked = 0;
            using var sub = root.Subscribe((in StorageFieldWriteEventArgs args) =>
            {
                invoked++;
                Assert.That(args.Target.ID, Is.EqualTo(root.ID));
            });

            root.Write("a", 1);
            root.Write("b", 2);

            Assert.That(invoked, Is.EqualTo(2));
        }

        [Test]
        public void Storage_Container_Subscription_Dispose_Stops_Notifications()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int invoked = 0;
            var sub = root.Subscribe((in StorageFieldWriteEventArgs _) => invoked++);

            root.Write("hp", 10);
            Assert.That(invoked, Is.EqualTo(1));

            sub.Dispose();
            root.Write("hp", 11);
            Assert.That(invoked, Is.EqualTo(1));
        }
    }

    public static class StorageApiTestExt
    {
        internal static void WriteNoRescheme<T>(this StorageObject obj, string fieldName, in T value) where T : unmanaged
        {
            obj.Write(fieldName, value, allowRescheme: false);
        }
    }
}
