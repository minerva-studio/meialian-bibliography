using NUnit.Framework;
using System;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageObjectArrayTests
    {
        // root: int hp; ref-array children[3]; float[4] speeds
        private ContainerLayout _rootLayout;

        // child: int hp
        private ContainerLayout _childLayout;

        [SetUp]
        public void Setup()
        {
            // root layout
            {
                var ob = new ObjectBuilder();
                ob.SetScalar<int>("hp");
                ob.SetRefArray("children", 3);
                ob.SetArray<float>("speeds", 4);
                _rootLayout = ob.BuildLayout();
            }

            // child layout
            {
                var ob = new ObjectBuilder();
                ob.SetScalar<int>("hp");
                _childLayout = ob.BuildLayout();
            }
        }

        [Test]
        public void ObjectArray_Basic_Create_Read_Write()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var arr = root.GetArray("children");
            Assert.That(arr.Length, Is.EqualTo(3));

            // Initially all null/empty
            for (int i = 0; i < arr.Length; i++)
            {
                var ok = arr.TryGetObject(i, out var child);
                Assert.That(ok, Is.False);
                Assert.That(child.IsNull, Is.True);
            }

            // Create three children
            for (int i = 0; i < arr.Length; i++)
            {
                var child = arr.GetObject(i, _childLayout);
                Assert.That(child.IsNull, Is.False);
                child.Write<int>("hp", (i + 1) * 10);
            }

            // Read back
            for (int i = 0; i < arr.Length; i++)
            {
                var ok = arr.TryGetObject(i, out var child);
                Assert.That(ok, Is.True);
                Assert.That(child.IsNull, Is.False);
                Assert.That(child.Read<int>("hp"), Is.EqualTo((i + 1) * 10));
            }
        }

        [Test]
        public void ObjectArray_AsField_View_Matches_Object_View()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            // Create via StorageField view
            var arrField = root.GetArray("children");
            Assert.That(arrField.Length, Is.EqualTo(3));

            for (int i = 0; i < arrField.Length; i++)
            {
                var child = arrField.GetObject(i, _childLayout);
                Assert.That(child.IsNull, Is.False);
                child.Write<int>("hp", 100 + i);
            }

            // Cross-check via StorageObject.GetArray
            var arr = root.GetArray("children");
            for (int i = 0; i < arr.Length; i++)
            {
                var c = arr.GetObject(i);
                Assert.That(c.Read<int>("hp"), Is.EqualTo(100 + i));
            }
        }

        [Test]
        public void ObjectArray_ClearAt_And_ClearAll()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var arr = root.GetArray("children");

            // Create at 0,1,2
            for (int i = 0; i < arr.Length; i++)
            {
                var obj = arr.GetObject(i, _childLayout);
                Assert.That(obj.IsNull, Is.False);
            }

            {
                var ok0 = arr.TryGetObject(0, out var c0);
                var ok1 = arr.TryGetObject(1, out var c1);
                var ok2 = arr.TryGetObject(2, out var c2);

                Assert.That(ok0, Is.True); Assert.That(c0.IsNull, Is.False);
                Assert.That(ok1, Is.True); Assert.That(c1.IsNull, Is.False);
                Assert.That(ok2, Is.True); Assert.That(c2.IsNull, Is.False);
            }

            // Clear a single slot
            arr.ClearAt(1);

            // Check: slot 0,2 present; slot 1 empty
            {
                var ok0 = arr.TryGetObject(0, out var c0);
                var ok1 = arr.TryGetObject(1, out var c1);
                var ok2 = arr.TryGetObject(2, out var c2);

                Assert.That(ok0, Is.True); Assert.That(c0.IsNull, Is.False);
                Assert.That(ok1, Is.False); Assert.That(c1.IsNull, Is.True);
                Assert.That(ok2, Is.True); Assert.That(c2.IsNull, Is.False);
            }

            // Clear all
            arr.Clear();

            for (int i = 0; i < arr.Length; i++)
            {
                var ok = arr.TryGetObject(i, out var child);
                Assert.That(ok, Is.False);
                Assert.That(child.IsNull, Is.True);
            }
        }

        [Test]
        public void ObjectArray_AsObject_ReturnsExisting_NotRecreate()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var arr = root.GetArray("children");

            // Create once
            var c0 = arr.GetObject(0, _childLayout);
            var id0 = c0.ID;

            // Get existing without allocation
            var c0b = arr.GetObjectNoAllocate(0);
            Assert.That(c0b.IsNull, Is.False);
            Assert.That(c0b.ID, Is.EqualTo(id0));

            // GetObject again should still return the same
            var c0c = arr.GetObject(0, _childLayout);
            Assert.That(c0c.ID, Is.EqualTo(id0));
        }

        [Test]
        public void ObjectArray_Index_OutOfRange_Throws()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var arr = root.GetArray("children");

            bool threw = false;
            try
            {
                //var el = arr[arr.Length]; // create element
                //el.GetObjectNoAllocate(); // force access -> should throw
                arr.GetObjectNoAllocate(arr.Length);
            }
            catch (IndexOutOfRangeException)
            {
                threw = true;
            }
            Assert.That(threw, Is.True, "Expected IndexOutOfRangeException was not thrown.");
        }

        [Test]
        public void Storage_Dispose_Unregisters_All_ObjectArray_Children()
        {
            var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var arr = root.GetArray("children");
            var ids = new ulong[arr.Length];

            for (int i = 0; i < arr.Length; i++)
            {
                var child = arr.GetObject(i, _childLayout);
                child.Write<int>("hp", 5 + i);
                ids[i] = child.ID;
            }

            storage.Dispose();

            var reg = Container.Registry.Shared;
            for (int i = 0; i < ids.Length; i++)
                Assert.That(reg.GetContainer(ids[i]), Is.Null, $"Child {i} should be unregistered.");
        }

        // --- Object Array: index out of range throws IndexOutOfRangeException ---
        [Test]
        public void ObjectArray_Index_OutOfRange_Throws_IndexOutOfRangeException()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var arr = root.GetArray("children");
            bool threw = false;
            try
            {
                arr.GetObjectNoAllocate(arr.Length); // triggers span index
            }
            catch (IndexOutOfRangeException)
            {
                threw = true;
            }
            Assert.That(threw, Is.True, "Expected IndexOutOfRangeException was not thrown.");
        }

        // --- Object Array: AsField view == Object view (consistency) ---
        [Test]
        public void ObjectArray_FieldView_And_ObjectView_Stay_Consistent()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;

            var arrField = root.GetArray("children");
            Assert.That(arrField.Length, Is.EqualTo(3));

            for (int i = 0; i < arrField.Length; i++)
            {
                var child = arrField.GetObject(i, _childLayout);
                child.Write<int>("hp", 100 + i);
            }

            var arr = root.GetArray("children");
            for (int i = 0; i < arr.Length; i++)
                Assert.That(arr.GetObject(i).Read<int>("hp"), Is.EqualTo(100 + i));
        }
    }
}
