using System;
using System.Runtime.InteropServices;
using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageArrayApiTests
    {
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

            var ensured = root.Ensure("dialog.line").IsArray<char>(minLength: 0, allowOverride: true);
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