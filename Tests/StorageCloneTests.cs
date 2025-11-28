using System;
using System.Collections.Generic;
using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageCloneTests
    {
        private static Storage BuildComplex()
        {
            // Build a reasonably complex tree with mixed types
            var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            // Scalars
            root.Write("int_value", 123);
            root.Write("long_value", 1234567890123L);
            root.Write("float_value", 3.5f);
            root.Write("double_value", 2.71828);
            root.Write("bool_value", true);

            // String
            root.Write("title", "Hello World");

            // Inline arrays
            root.WriteArray("int_arr", new[] { 1, 2, 3, 4 });
            root.WriteArray("float_arr", new[] { 1.5f, 2.5f, 3.5f });

            // Object with subfields
            var player = root.GetObject("player");
            player.Write("hp", 100);
            player.Write("name", "Alice");

            // Object array and elements values
            var list = root.GetObject("list");
            var arr = list.MakeObjectArray(3);
            arr.GetObject(0).Write("val", 10);
            arr.GetObject(1).Write("val", 20);
            arr.GetObject(2).Write("val", 30);

            // Deep path
            root.WritePath("a.b.c", 42);
            root.WriteArrayPath("m.n.arr", new[] { 7, 8, 9 });

            return s;
        }

        [Test]
        public void Clone_Empty_Then_Use_All_Basics()
        {
            using var s = new Storage(ContainerLayout.Empty);

            // Clone empty storage
            using var clone = Storage.Clone(s);

            var root = clone.Root;

            // Scalar read/write
            root.Write("x", 1);
            Assert.That(root.Read<int>("x"), Is.EqualTo(1));

            // Path API
            root.WritePath("a.b.c", 99);
            Assert.That(root.ReadPath<int>("a.b.c"), Is.EqualTo(99));

            // String
            root.Write("title", "ok");
            Assert.That(root.ReadString("title"), Is.EqualTo("ok"));

            // Inline array
            root.WriteArray("ints", new[] { 3, 4, 5 });
            CollectionAssert.AreEqual(new[] { 3, 4, 5 }, root.ReadArray<int>("ints"));

            // Object + child
            var child = root.GetObject("child");
            child.Write("hp", 5);
            Assert.That(child.Read<int>("hp"), Is.EqualTo(5));

            // Object array
            var arr = root.GetObject("bag").MakeObjectArray(2);
            arr.GetObject(1).Write("v", 123);
            Assert.That(arr.GetObject(1).Read<int>("v"), Is.EqualTo(123));
        }

        [Test]
        public void Clone_Root_FullTree_Preserves_Data_And_Shape()
        {
            using var s = BuildComplex();
            var orig = s.Root;

            // Clone from root storage
            using var clone = Storage.Clone(s);
            var root = clone.Root;

            // Scalars
            Assert.That(root.Read<int>("int_value"), Is.EqualTo(123));
            Assert.That(root.Read<long>("long_value"), Is.EqualTo(1234567890123L));
            Assert.That(root.Read<float>("float_value"), Is.InRange(3.4999f, 3.5001f));
            Assert.That(root.Read<double>("double_value"), Is.InRange(2.71827, 2.71829));
            Assert.That(root.Read<bool>("bool_value"), Is.True);

            // String
            Assert.That(root.ReadString("title"), Is.EqualTo("Hello World"));

            // Inline arrays
            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4 }, root.ReadArray<int>("int_arr"));
            CollectionAssert.AreEqual(new[] { 1.5f, 2.5f, 3.5f }, root.ReadArray<float>("float_arr"));

            // Object child
            var player = root.GetObject("player");
            Assert.That(player.Read<int>("hp"), Is.EqualTo(100));
            Assert.That(player.ReadString("name"), Is.EqualTo("Alice"));

            // Object array
            var list = root.GetObject("list");
            var arr = list.AsArray();
            Assert.That(arr.Length, Is.EqualTo(3));
            Assert.That(arr.GetObject(0).Read<int>("val"), Is.EqualTo(10));
            Assert.That(arr.GetObject(1).Read<int>("val"), Is.EqualTo(20));
            Assert.That(arr.GetObject(2).Read<int>("val"), Is.EqualTo(30));

            // Deep path
            Assert.That(root.ReadPath<int>("a.b.c"), Is.EqualTo(42));
            CollectionAssert.AreEqual(new[] { 7, 8, 9 }, root.ReadArrayPath<int>("m.n.arr"));

            // Independence: IDs should not be the same as original (deep copy)
            Assert.That(root.ID, Is.Not.EqualTo(orig.ID));
        }

        [Test]
        public void Clone_From_Subtree_Produces_Independent_Storage_SubtreeOnly()
        {
            using var s = BuildComplex();
            var subtree = s.Root.GetObject("player");

            // Clone from StorageObject sub-tree
            using var clone = Storage.Clone(subtree);
            var root = clone.Root;

            // Only subtree content should exist
            Assert.That(root.HasField("hp"), Is.True);
            Assert.That(root.HasField("name"), Is.True);
            Assert.That(root.Read<int>("hp"), Is.EqualTo(100));
            Assert.That(root.ReadString("name"), Is.EqualTo("Alice"));

            // Fields from original root should not exist here
            Assert.That(root.HasField("int_value"), Is.False);
            Assert.That(root.HasField("list"), Is.False);
        }

        [Test]
        public void Clone_Is_DeepCopy_Mutations_Do_Not_Affect_Original()
        {
            using var s = BuildComplex();
            using var clone = Storage.Clone(s);

            var r0 = s.Root;
            var r1 = clone.Root;

            // Change clone
            r1.Write("int_value", 999);
            r1.Write("title", "changed");
            r1.WriteArray("int_arr", new[] { 8, 8, 8 });
            r1.GetObject("player").Write("hp", 555);
            r1.WritePath("a.b.c", -1);

            // Original remains unchanged
            Assert.That(r0.Read<int>("int_value"), Is.EqualTo(123));
            Assert.That(r0.ReadString("title"), Is.EqualTo("Hello World"));
            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4 }, r0.ReadArray<int>("int_arr"));
            Assert.That(r0.GetObject("player").Read<int>("hp"), Is.EqualTo(100));
            Assert.That(r0.ReadPath<int>("a.b.c"), Is.EqualTo(42));
        }

        [Test]
        public void Clone_Subscription_Works_On_Clone()
        {
            using var s = BuildComplex();
            using var clone = Storage.Clone(s);

            TestFor(s.Root);
            TestFor(clone.Root);

            static void TestFor(StorageObject root)
            {
                var events = new List<StorageEventArgs>();
                using var sub = root.Subscribe((in StorageEventArgs a) => events.Add(a));

                // Write scalar
                root.Write("score", 7);
                // Write path
                root.WritePath("p.q.r", 11);
                // Modify string
                root.Write("title", "new");

                Assert.That(events.Exists(e => e.Path == "score" && e.Event == StorageEvent.Write), Is.True);
                Assert.That(events.Exists(e => e.Path == "p.q.r" && e.Event == StorageEvent.Write), Is.True);
                Assert.That(events.Exists(e => e.Path == "title" && e.Event == StorageEvent.Write), Is.True);
                Assert.That(events.Count, Is.GreaterThanOrEqualTo(3));
            }
        }


        [Test]
        public void Clone_Move_Delete_And_Arrays_Work_On_Clone()
        {
            using var s = BuildComplex();
            using var clone = Storage.Clone(s);
            var root = clone.Root;

            // Move scalar field
            root.Move("int_value", "int_value2");
            Assert.That(root.HasField("int_value"), Is.False);
            Assert.That(root.Read<int>("int_value2"), Is.EqualTo(123));

            // Delete a field
            Assert.That(root.Delete("bool_value"), Is.True);
            Assert.That(root.HasField("bool_value"), Is.False);

            // Arrays: resize/override/read
            root.WriteArray("float_arr", new[] { 9.1f, 9.2f });
            CollectionAssert.AreEqual(new[] { 9.1f, 9.2f }, root.ReadArray<float>("float_arr"));

            // String
            root.Write("title", "updated");
            Assert.That(root.ReadString("title"), Is.EqualTo("updated"));

            // Object array append (ensure larger length)
            var list = root.GetObject("list");
            var arr = list.AsArray();
            arr.EnsureLength(5);
            arr.GetObject(4).Write("val", 77);
            Assert.That(arr.Length, Is.EqualTo(5));
            Assert.That(arr.GetObject(4).Read<int>("val"), Is.EqualTo(77));
        }

        [Test]
        public void Clone_Dispose_Is_Independent()
        {
            using var s = BuildComplex();
            using var clone = Storage.Clone(s);

            var r0 = s.Root;
            var r1 = clone.Root;

            // Dispose original
            s.Dispose();

            // Clone still alive
            Assert.That(r1.IsNull, Is.False);
            r1.Write("survive", 1);
            Assert.That(r1.Read<int>("survive"), Is.EqualTo(1));

            // Dispose clone, original already disposed
            clone.Dispose();
            Assert.That(r1.IsDisposed, Is.True);
        }

        [Test]
        public void Clone_ObjectArray_Ids_Are_Distinct_And_Data_Preserved()
        {
            using var s = BuildComplex();
            var orig = s.Root.GetObject("list").AsArray();
            var idsBefore = new ulong[orig.Length];
            for (int i = 0; i < orig.Length; i++) idsBefore[i] = orig.GetObject(i).ID;

            using var clone = Storage.Clone(s);
            var arr = clone.Root.GetObject("list").AsArray();

            // Same length and values preserved
            Assert.That(arr.Length, Is.EqualTo(orig.Length));
            for (int i = 0; i < arr.Length; i++)
            {
                Assert.That(arr.GetObject(i).Read<int>("val"), Is.EqualTo((i + 1) * 10));
                // IDs should typically differ due to re-registration on parse
                Assert.That(arr.GetObject(i).ID, Is.Not.EqualTo(idsBefore[i]));
            }
        }

        [Test]
        public void Clone_Path_Apis_Work_And_Errors_Are_Consistent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            s.Root.WritePath("x.y.z", 5);

            using var clone = Storage.Clone(s);
            var root = clone.Root;

            // Read existing path
            Assert.That(root.ReadPath<int>("x.y.z"), Is.EqualTo(5));

            // TryRead missing
            Assert.That(root.TryReadPath<int>("missing.path", out var value), Is.False);
            Assert.That(value, Is.EqualTo(default(int)));

            // Read missing throws
            Assert.Throws<ArgumentException>(() => root.ReadPath<int>("missing.path"));
        }
    }
}