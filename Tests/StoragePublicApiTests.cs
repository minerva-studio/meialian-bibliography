using NUnit.Framework;
using System;

namespace Amlos.Container.Tests
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
            catch (ArgumentException) { threw = true; }
            Assert.That(threw, Is.True);

            root.Write<float>("v", 1000.0f);
            Assert.That(root.GetField("v").Length, Is.EqualTo(4));

            threw = false;
            try { root.WriteNoRescheme<double>("v", 1000.0); }
            catch (ArgumentException) { threw = true; }
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
    }

    public static class StorageApiTestExt
    {
        internal static void WriteNoRescheme<T>(this StorageObject obj, string fieldName, in T value) where T : unmanaged
        {
            obj.Write(fieldName, value, allowRescheme: false);
        }
    }
}
