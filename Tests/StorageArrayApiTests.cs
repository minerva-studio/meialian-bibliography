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
    }
}