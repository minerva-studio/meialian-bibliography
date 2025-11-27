using NUnit.Framework;
using System;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageObjectArrayApiTests
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

        [Test]
        public void ValueArray_Create_Read_Resize()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("numbers", new[] { 1, 2, 3 });
            Assert.That(root.TryGetArray<int>("numbers".AsSpan(), out var arr), Is.True);

            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.False);

            arr.EnsureLength(6);
            var back = root.ReadArray<int>("numbers");
            Assert.That(back.Length, Is.EqualTo(6));
            CollectionAssert.AreEqual(new[] { 1, 2, 3, 0, 0, 0 }, back);
        }

        [Test]
        public void ValueArray_Resize_Shrink_PreservesPrefix()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("floats", new[] { 1f, 2f, 3f, 4f, 5f });
            Assert.That(root.TryGetArray<float>("floats".AsSpan(), out var arr), Is.True);
            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.False);

            arr.Resize(2);
            var back = root.ReadArray<float>("floats");
            CollectionAssert.AreEqual(new[] { 1f, 2f }, back);
        }

        [Test]
        public void ValueArray_WriteViaStorageArrayWrite()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("vals", new[] { 10f, 20f, 30f });
            Assert.That(root.TryGetArray<float>("vals".AsSpan(), out var arr), Is.True);
            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.False);

            arr.Write(1, MemoryMarshal.AsBytes(new float[1] { 99 }.AsSpan()), ValueType.Float32);

            var back = root.ReadArray<float>("vals");
            CollectionAssert.AreEqual(new[] { 10f, 99f, 30f }, back);
        }

        [Test]
        public void ValueArray_TryGetArray_Generic_TypeMismatch()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("speeds", new[] { 1f, 2f });
            Assert.That(root.TryGetArray<int>("speeds".AsSpan(), out var wrong), Is.False);
            Assert.That(root.TryGetArray<float>("speeds".AsSpan(), out var ok), Is.True);
            Assert.That(ok.IsExternalArray, Is.True);
            Assert.That(ok.IsObjectArray, Is.False);
        }

        [Test]
        public void ValueArray_TryGetArrayByPath_Success_And_Failure()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArrayPath("stats.values", new[] { 5, 6, 7 });
            Assert.That(root.TryGetArrayByPath<int>("stats.values".AsSpan(), out var arr), Is.True);
            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.False);

            Assert.That(root.TryGetArrayByPath<int>("stats.missing".AsSpan(), out var missingArr), Is.False);
        }

        [Test]
        public void ObjectArray_Create_Access_Element()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var objArr = root.GetArrayByPath("world.entities".AsSpan(), TypeData.Ref, true);
            objArr.EnsureLength(3);
            Assert.That(objArr.Length, Is.GreaterThanOrEqualTo(3));
            Assert.That(objArr.IsExternalArray, Is.True);
            Assert.That(objArr.IsObjectArray, Is.True);

            root.WritePath("world.entities[1].hp", 15);
            int hp = root.ReadPath<int>("world.entities[1].hp");
            Assert.That(hp, Is.EqualTo(15));
        }

        [Test]
        public void ObjectArray_TryGetArray_Generic_Ref()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var arrEnsure = root.GetArrayByPath("actors".AsSpan(), TypeData.Ref, true);
            arrEnsure.EnsureLength(2);

            Assert.That(root.TryGetArray("actors".AsSpan(), TypeData.Ref, out var arr), Is.True);
            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.True);
        }

        [Test]
        public void ObjectArray_EnsureLength_Expands()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var arr = root.GetArrayByPath("entities".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(2);
            Assert.That(arr.Length, Is.EqualTo(2));
            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.True);

            arr.EnsureLength(5);
            Assert.That(arr.Length, Is.EqualTo(5));
            Assert.That(arr.TryGetObject(4, out var obj4), Is.False);
        }

        [Test]
        public void ObjectArray_TryGetObject_False_On_Empty_Slot()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var arr = root.GetArrayByPath("entities".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(4);
            Assert.That(arr.TryGetObject(3, out var obj), Is.False);
        }

        [Test]
        public void ObjectArray_GetObjectNoAllocate_ReturnsNullView()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var arr = root.GetArrayByPath("entities".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(3);
            var obj2 = arr.GetObjectNoAllocate(2);
            Assert.That(obj2.IsNull, Is.True);
        }

        [Test]
        public void ObjectArray_WriteInsideElementThenRead()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var arr = root.GetArrayByPath("units".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(2);

            root.WritePath("units[0].name", "Alpha");
            root.WritePath("units[1].hp", 33);

            Assert.That(root.ReadStringPath("units[0].name"), Is.EqualTo("Alpha"));
            Assert.That(root.ReadPath<int>("units[1].hp"), Is.EqualTo(33));
        }

        [Test]
        public void ObjectArray_Index_OutOfRange_TryGetObject_Fails()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var arr = root.GetArrayByPath("entities".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(2);
            Assert.That(arr.TryGetObject(5, out var over), Is.False);
        }

        [Test]
        public void AsArray_On_ObjectArrayContainer()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var arr = root.GetArrayByPath("entities".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(1);

            var holder = root.GetObject("entities");
            var view = holder.AsArray();
            Assert.That(view.IsExternalArray, Is.True);
            Assert.That(view.IsObjectArray, Is.True);
            Assert.That(view.Length, Is.EqualTo(1));
        }

        [Test]
        public void StringArray_AsString_RoundTrip()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var txt = "Hello Arrays!";
            root.WritePath("dialog.line", txt);
            Assert.That(root.TryGetArrayByPath("dialog.line".AsSpan(), TypeData.Of<char>(), out var arr), Is.True);

            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.False);
            Assert.That(arr.Type, Is.EqualTo(ValueType.Char16));

            var back = arr.AsString();
            Assert.That(back, Is.EqualTo(txt));
        }

        [Test]
        public void TryGetArrayByPath_ObjectArray_Success()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var created = root.GetArrayByPath("world.entities".AsSpan(), TypeData.Ref, true);
            created.EnsureLength(2);

            Assert.That(root.TryGetArrayByPath("world.entities".AsSpan(), TypeData.Ref, out var arr), Is.True);
            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.True);
        }

        [Test]
        public void TryGetArrayByPath_ObjectArray_Failure_On_ValueArray()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArrayPath("data.values", new[] { 1, 2, 3 });
            Assert.That(root.TryGetArrayByPath("data.values".AsSpan(), TypeData.Ref, out var arr), Is.False);
        }

        [Test]
        public void ValueArray_Path_Read_Write_Mix()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArrayPath("stats.speeds", new[] { 1f, 2f });
            Assert.That(root.TryGetArrayByPath<float>("stats.speeds".AsSpan(), out var arr), Is.True);
            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.False);

            arr.EnsureLength(4);
            var back = root.ReadArrayPath<float>("stats.speeds");
            Assert.That(back.Length, Is.EqualTo(4));
            CollectionAssert.AreEqual(new[] { 1f, 2f, 0f, 0f }, back);
        }

        [Test]
        public void ObjectArray_Path_Write_Element_Field()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var arr = root.GetArrayByPath("scene.actors".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(2);
            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.True);

            root.WritePath("scene.actors[1].hp", 88);
            Assert.That(root.ReadPath<int>("scene.actors[1].hp"), Is.EqualTo(88));
        }

        [Test]
        public void StringArray_Write_Expand_And_Shrink()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.Write("caption", "Hi");
            Assert.That(root.TryGetArray("caption".AsSpan(), TypeData.Of<char>(), out var arr), Is.True);
            Assert.That(arr.IsString, Is.True);
            Assert.That(arr.AsString(), Is.EqualTo("Hi"));

            arr.Write("HelloWorld");
            Assert.That(arr.AsString(), Is.EqualTo("HelloWorld"));

            arr.Write("X");
            Assert.That(arr.AsString(), Is.EqualTo("X"));
        }

        [Test]
        public void StringArray_Write_Via_Path()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WritePath("ui.title.text", "Title");
            Assert.That(root.TryGetArrayByPath("ui.title.text".AsSpan(), TypeData.Of<char>(), out var arr), Is.True);
            Assert.That(arr.IsString, Is.True);
            Assert.That(arr.AsString(), Is.EqualTo("Title"));

            arr.Write("NewTitle");
            Assert.That(root.ReadStringPath("ui.title.text"), Is.EqualTo("NewTitle"));
        }

        [Test]
        public void Ensure_Creates_CharArray_Then_Write_String()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var ensured = root.Make("dialog.line").Array<char>(minLength: 0, allowOverride: true);
            Assert.That(ensured.IsString, Is.True);

            ensured.Write("First");
            Assert.That(root.ReadStringPath("dialog.line"), Is.EqualTo("First"));

            ensured.Write("SecondLine");
            Assert.That(root.ReadStringPath("dialog.line"), Is.EqualTo("SecondLine"));
        }

        [Test]
        public void Override_IntArray_To_FloatArray()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("values", new[] { 1, 2, 3 });
            Assert.That(root.TryGetArray<int>("values".AsSpan(), out var original), Is.True);
            Assert.That(original.Type, Is.EqualTo(ValueType.Int32));

            var floats = new float[] { 3.5f, 4.5f };
            root.Override("values", floats);

            Assert.That(root.TryGetArray<float>("values".AsSpan(), out var floatArr), Is.True);
            Assert.That(floatArr.Type, Is.EqualTo(ValueType.Float32));
            CollectionAssert.AreEqual(floats, root.ReadArray<float>("values"));
        }

        [Test]
        public void Override_IntArray_To_DoubleArray_Different_Length()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("nums", new[] { 10, 20, 30, 40 });
            Assert.That(root.TryGetArray<int>("nums".AsSpan(), out var intArr), Is.True);
            Assert.That(intArr.Type, Is.EqualTo(ValueType.Int32));

            var doubles = new double[] { 1.25, 2.5 };
            root.Override("nums", doubles);

            Assert.That(root.TryGetArray<double>("nums".AsSpan(), out var dblArr), Is.True);
            Assert.That(dblArr.Type, Is.EqualTo(ValueType.Float64));
            CollectionAssert.AreEqual(doubles, root.ReadArray<double>("nums"));
        }

        [Test]
        public void Override_IntArray_To_StringArray()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("mixed", new[] { 1, 2, 3 });
            Assert.That(root.TryGetArray<int>("mixed".AsSpan(), out var arrInt), Is.True);
            Assert.That(arrInt.Type, Is.EqualTo(ValueType.Int32));

            var chars = "Alpha".ToCharArray();
            root.Override("mixed", chars);

            Assert.That(root.TryGetArray("mixed".AsSpan(), TypeData.Of<char>(), out var arrChars), Is.True);
            Assert.That(arrChars.Type, Is.EqualTo(ValueType.Char16));
            Assert.That(arrChars.IsString, Is.True);
            Assert.That(root.ReadString("mixed"), Is.EqualTo("Alpha"));

            arrChars.Write("Z");
            Assert.That(root.ReadString("mixed"), Is.EqualTo("Z"));
        }

        [Test]
        public void StorageArray_CopyFrom_SameLength_NoResize_Allowed()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("data", new[] { 1, 2, 3 });
            var arr = root.GetObject("data").AsArray();
            Assert.That(arr.Type, Is.EqualTo(ValueType.Int32));
            Assert.That(arr.Length, Is.EqualTo(3));

            arr.CopyFrom(new ReadOnlySpan<int>(new[] { 9, 8, 7 }), allowResize: false);
            CollectionAssert.AreEqual(new[] { 9, 8, 7 }, root.ReadArray<int>("data"));
            Assert.That(arr.Length, Is.EqualTo(3));
        }

        [Test]
        public void StorageArray_CopyFrom_DifferentLength_Resize_Disallowed()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("nums", new[] { 1, 2, 3, 4 });
            var arr = root.GetObject("nums").AsArray();
            Assert.That(arr.Length, Is.EqualTo(4));

            var newSpan = new ReadOnlySpan<int>(new[] { 5, 6 });
            arr.CopyFrom(newSpan, allowResize: false);

            var back = root.ReadArray<int>("nums");
            Assert.That(back.Length, Is.EqualTo(4));
            CollectionAssert.AreEqual(new[] { 5, 6, 3, 4 }, back);
        }

        [Test]
        public void StorageArray_Override_DifferentLength_Resize_Allowed()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("nums", new[] { 1, 2, 3, 4 });
            var arr = root.GetObject("nums").AsArray();
            Assert.That(arr.Length, Is.EqualTo(4));

            arr.Override(new ReadOnlySpan<int>(new[] { 9, 9, 9 }));
            var back = root.ReadArray<int>("nums");
            Assert.That(back.Length, Is.EqualTo(3));
            CollectionAssert.AreEqual(new[] { 9, 9, 9 }, back);
        }

        [Test]
        public void StorageArray_Write_String_On_Array_Obtained_By_Path()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WritePath("meta.info.name", "A");
            Assert.That(root.TryGetArrayByPath("meta.info.name".AsSpan(), TypeData.Of<char>(), out var arr), Is.True);
            arr.Write("BiggerNameSegment");
            Assert.That(root.ReadStringPath("meta.info.name"), Is.EqualTo("BiggerNameSegment"));
        }

        [Test]
        public void StorageArray_RawOverride_Then_String_Write()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var ints = new[] { 100, 200 };
            root.Override("raw", MemoryMarshal.AsBytes<int>(ints), ValueType.Int32, inlineArrayLength: ints.Length);
            Assert.That(root.TryGetArray<int>("raw".AsSpan(), out var intArr), Is.True);
            CollectionAssert.AreEqual(ints, root.ReadArray<int>("raw"));

            var chars = "Hello".ToCharArray();
            root.Override("raw", chars);
            Assert.That(root.TryGetArray("raw".AsSpan(), TypeData.Of<char>(), out var strArr), Is.True);
            Assert.That(strArr.Type, Is.EqualTo(ValueType.Char16));
            Assert.That(root.ReadString("raw"), Is.EqualTo("Hello"));

            strArr.Write("X");
            Assert.That(root.ReadString("raw"), Is.EqualTo("X"));
        }
    }
}