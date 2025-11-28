using NUnit.Framework;
using System;
using System.Collections.Generic;

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
                child = root.GetObject("child", false, null);
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
            try { root.Write<int>("v", 1, allowRescheme: false); }
            catch (ArgumentException) { threw = true; }
            Assert.That(threw, Is.True);

            root.Write<float>("v", 1000.0f);
            Assert.That(root.GetField("v").Length, Is.EqualTo(4));

            threw = false;
            try { root.Write<double>("v", 1000.0, allowRescheme: false); }
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
            try { root.Write<int>("v", 1000, allowRescheme: false); }
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
            try { root.Write<byte>("v", 1, allowRescheme: false); }
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
            Assert.That(root.GetObject("e").IsArray(), Is.True);
            Assert.That(root.GetObject("e").IsString, Is.True);
            Assert.That(root.GetObject("e").GetField(ContainerLayout.ArrayName).Length, Is.EqualTo(big.Length * sizeof(char)));
            var back = root.ReadString("e");

            Assert.That(back.Length, Is.EqualTo(big.Length));
            Assert.That(back, Is.EqualTo(big));

            var big2 = new string('a', 100);
            root.Write("e", big2);

            Assert.That(root.GetField("e").IsRef, Is.True);
            Assert.That(root.GetObject("e").IsArray(), Is.True);
            Assert.That(root.GetObject("e").IsString, Is.True);
            Assert.That(root.GetObject("e").GetField(ContainerLayout.ArrayName).Length, Is.EqualTo(big2.Length * sizeof(char)));
            var back2 = root.ReadString("e");

            Assert.That(back2.Length, Is.EqualTo(big2.Length));
            Assert.That(back2, Is.EqualTo(big2));
        }


        #region Array

        [Test]
        public void StorageObject_GetArray_ValueArray_ReadWrite_RoundTrip()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("ints", new[] { 1, 2, 3 });
            var arr = root.GetArray("ints");
            Assert.That(arr.IsExternalArray, Is.True);
            Assert.That(arr.IsObjectArray, Is.False);
            Assert.That(arr.Length, Is.EqualTo(3));

            arr.Write(1, 99);
            CollectionAssert.AreEqual(new[] { 1, 99, 3 }, root.ReadArray<int>("ints"));
        }

        [Test]
        public void StorageObject_TryGetArray_Generic_Success_And_TypeMismatch()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("floats", new[] { 1f, 2f });
            Assert.That(root.TryGetArray<float>("floats".AsSpan(), out var ok), Is.True);
            Assert.That(ok.Length, Is.EqualTo(2));
            Assert.That(root.TryGetArray<int>("floats".AsSpan(), out var bad), Is.False);
        }

        [Test]
        public void StorageObject_TryGetArray_TypeData_Filter_Fails_On_TypeMismatch()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("nums", new[] { 10, 20 });
            Assert.That(root.TryGetArray("nums".AsSpan(), TypeData.Of<int>(), out var arr2), Is.True);
            Assert.That(root.TryGetArray("nums".AsSpan(), TypeData.Of<float>(), out var arr), Is.True);
            Assert.That(arr2.Length, Is.EqualTo(2));
            root.WriteArray("numss", new[] { 10f, 20f });
            Assert.That(root.TryGetArray("numss".AsSpan(), TypeData.Of<int>(), out var arr3), Is.False);
            Assert.That(root.TryGetArray("numss".AsSpan(), TypeData.Of<float>(), out var arr4), Is.True);
            Assert.That(arr4.Length, Is.EqualTo(2));
        }

        [Test]
        public void StorageObject_GetArrayByPath_Creates_ValueArray_IfMissing()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var arr = root.GetArrayByPath<int>("stats.values".AsSpan(), true);
            Assert.That(arr.Length, Is.EqualTo(0));

            arr.EnsureLength(3);
            arr.Write(0, 5);
            arr.Write(1, 6);
            arr.Write(2, 7);

            CollectionAssert.AreEqual(new[] { 5, 6, 7 }, root.ReadArrayPath<int>("stats.values"));
        }

        [Test]
        public void StorageObject_GetArrayByPath_ObjectArray_CreateAndWriteElement()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var objArr = root.GetArrayByPath("world.entities".AsSpan(), TypeData.Ref, true);
            objArr.EnsureLength(2);
            objArr.GetObject(1).Write("hp", 33);

            Assert.That(root.ReadPath<int>("world.entities[1].hp"), Is.EqualTo(33));
        }

        [Test]
        public void StorageObject_TryGetArrayByPath_ObjectArray_Fails_On_ValueArray()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArrayPath("data.values", new[] { 1, 2, 3 });
            Assert.That(root.TryGetArrayByPath("data.values".AsSpan(), TypeData.Ref, out var arr), Is.False);
        }

        [Test]
        public void StorageObject_IsArray_Overloads_Work_For_Value_And_Object()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("nums", new[] { 1, 2 });
            Assert.That(root.IsArray("nums"), Is.True);

            var objArr = root.GetArrayByPath("actors".AsSpan(), TypeData.Ref, true);
            objArr.EnsureLength(1);
            Assert.That(root.IsArray("actors"), Is.True);

            Assert.That(root.IsArray(), Is.False, "Root object itself is not an array container.");
            Assert.That(root.GetObject("actors").IsArray(), Is.True, "Object array holder should report IsArray().");
        }

        [Test]
        public void StorageObject_AsArray_On_ArrayContainer_ValueArray()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var holder = root.GetObject("holder");
            holder.WriteArray(new[] { 10, 11, 12 });

            Assert.That(holder.IsArray(), Is.True);
            var arr = holder.AsArray();
            Assert.That(arr.Length, Is.EqualTo(3));
            CollectionAssert.AreEqual(new[] { 10, 11, 12 }, holder.ReadArray<int>());
        }

        [Test]
        public void StorageObject_AsArray_On_ArrayContainer_ObjectArray()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var objArr = root.GetArrayByPath("scene.units".AsSpan(), TypeData.Ref, true);
            objArr.EnsureLength(2);
            objArr.GetObject(0).Write("name", "Hero");

            var holder = root.GetObject("scene").GetObject("units");
            Assert.That(holder.IsArray(), Is.True);
            var arrView = holder.AsArray();
            Assert.That(arrView.IsObjectArray, Is.True);
            Assert.That(arrView.Length, Is.EqualTo(2));
            Assert.That(root.ReadStringPath("scene.units[0].name"), Is.EqualTo("Hero"));
        }

        [Test]
        public void StorageObject_TryGetArray_Generic_On_ObjectArray_Fails()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var objArr = root.GetArrayByPath("objs".AsSpan(), TypeData.Ref, true);
            objArr.EnsureLength(1);

            Assert.That(root.TryGetArray<int>("objs".AsSpan(), out var typed), Is.False);
            Assert.That(root.TryGetArray("objs".AsSpan(), TypeData.Ref, out var untyped), Is.True);
            Assert.That(untyped.IsObjectArray, Is.True);
        }

        [Test]
        public void StorageObject_ObjectArray_Element_Write_Read()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var arr = root.GetArrayByPath("items".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(3);

            arr.GetObject(2).Write("quality", 88);
            Assert.That(root.ReadPath<int>("items[2].quality"), Is.EqualTo(88));
        }

        [Test]
        public void StorageObject_Array_Element_ClearAt_SetsRefZero()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var arr = root.GetArrayByPath("objects".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(2);
            arr.GetObject(0).Write("hp", 5);
            Assert.That(root.ReadPath<int>("objects[0].hp"), Is.EqualTo(5));

            arr.ClearAt(0);
            Assert.That(arr.TryGetObject(0, out var cleared), Is.False);
        }

        [Test]
        public void StorageObject_Array_Clear_Resets_All_Slots()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var arr = root.GetArrayByPath("objects".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(3);
            arr.GetObject(0).Write("id", 1);
            arr.GetObject(1).Write("id", 2);

            arr.Clear();
            Assert.That(arr.TryGetObject(0, out var a0), Is.False);
            Assert.That(arr.TryGetObject(1, out var a1), Is.False);
        }

        [Test]
        public void StorageObject_Array_EnsureLength_Expands_ValueArray()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("nums", new[] { 1, 2 });
            var arr = root.GetArray("nums");
            arr.EnsureLength(5);
            Assert.That(arr.Length, Is.EqualTo(5));
            CollectionAssert.AreEqual(new[] { 1, 2, 0, 0, 0 }, root.ReadArray<int>("nums"));
        }

        [Test]
        public void StorageObject_Array_Resize_Shrinks_ValueArray()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("nums", new[] { 1, 2, 3, 4 });
            var arr = root.GetArray("nums");
            arr.Resize(2);
            CollectionAssert.AreEqual(new[] { 1, 2 }, root.ReadArray<int>("nums"));
        }

        [Test]
        public void StorageObject_GetOrCreateArray_OverrideExisting_Type()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("values", new float[] { 1f, 2f, 3f });
            var original = root.GetArray("values");
            Assert.That(original.Type, Is.EqualTo(ValueType.Float32));

            var replaced = root.GetArray("values".AsSpan(), TypeData.Of<int>(), reschemeOnTypeMismatch: false, overrideExisting: true);
            Assert.That(replaced.Type, Is.EqualTo(ValueType.Int32));
            replaced.EnsureLength(3);
            replaced.Write(0, 1f);
            replaced.Write(1, 2f);
            replaced.Write(2, 3f);

            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, root.ReadArray<int>("values"));
            CollectionAssert.AreEqual(new[] { 1f, 2f, 3f }, root.ReadArray<float>("values"));
        }

        [Test]
        public void StorageObject_GetOrCreateArray_Respects_Existing_When_Not_Override()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("floats", new[] { 5f, 6f });
            var arr1 = root.GetArray("floats", TypeData.Of<float>(), overrideExisting: false);
            Assert.That(arr1.Type, Is.EqualTo(ValueType.Float32));
            Assert.Throws<ArgumentException>(() =>
                root.GetArray("floats", TypeData.Of<int>(), reschemeOnTypeMismatch: false, overrideExisting: false));
        }

        [Test]
        public void StorageObject_Array_IsConvertibleTo_Works_For_ImplicitConversions()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArray("ints", new[] { 1, 2 });
            var arr = root.GetArray("ints");
            Assert.That(arr.IsConvertibleTo<float>(), Is.True); // int->float implicit read
            Assert.That(arr.IsConvertibleTo(TypeData.Of<float>()), Is.True);
            Assert.That(arr.IsConvertibleTo(ValueType.Float32), Is.True);
        }

        [Test]
        public void StorageObject_ReadArray_FromChildArrayContainer()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var holder = root.GetObject("holder");
            holder.WriteArray(new[] { 9, 8, 7 });
            CollectionAssert.AreEqual(new[] { 9, 8, 7 }, holder.ReadArray<int>());
        }

        [Test]
        public void StorageObject_TryGetArray_ValueArray_Fails_On_ObjectArray()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var objArr = root.GetArrayByPath("refs".AsSpan(), TypeData.Ref, true);
            objArr.EnsureLength(1);
            Assert.That(root.TryGetArray<int>("refs".AsSpan(), out var valueArr), Is.False);
            Assert.That(root.TryGetArray("refs".AsSpan(), TypeData.Ref, out var refArr), Is.True);
            Assert.That(refArr.IsObjectArray, Is.True);
        }

        [Test]
        public void StorageObject_Array_Path_Index_Write_Read()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.GetArrayByPath("units".AsSpan(), TypeData.Ref, true).EnsureLength(2);
            root.WritePath("units[1].hp", 55);
            Assert.That(root.ReadPath<int>("units[1].hp"), Is.EqualTo(55));
        }

        [Test]
        public void StorageObject_Array_String_AsArray_RoundTrip()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("title", "Minerva");
            var arr = root.GetArray("title");
            Assert.That(arr.Type, Is.EqualTo(ValueType.Char16));
            var back = arr.AsString();
            Assert.That(back, Is.EqualTo("Minerva"));
        }

        #endregion

        #region Indexer

        [Test]
        public void RootIndexer_ReturnsMember()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            storage.Root.Write("health", 10);
            var m = storage["health"];
            Assert.AreEqual(ValueType.Int32, m.ValueType);
            Assert.AreEqual(10, m.Read<int>());
        }

        [Test]
        public void MemberIndexer_NestedPath()
        {
            var storage = new Storage(ContainerLayout.Empty);
            storage.Root.Write("a", 1);
            storage.Root.GetObject("b").Write("c", 123);
            var b = storage["b"];
            var c = b["c"];
            Assert.AreEqual(123, c.Read<int>());
        }

        [Test]
        public void TryGetMember_PathFound()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            storage.Root.Write("x", 7);
            Assert.IsTrue(storage.TryGetMember("x", out var member));
            Assert.AreEqual(7, member.Read<int>());
        }

        [Test]
        public void TryGetMember_PathMissing()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            Assert.IsFalse(storage.TryGetMember("nope", out var _));
        }

        #region StorageObject scalar & type behavior

        [Test]
        public void Storage_FieldTwoWay_TypeEvolution()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;
            root.Write<int>("f1", 1);
            root.Write<float>("f1", 1f);
            Assert.AreEqual(ValueType.Float32, root.GetValueView("f1").Type);
            root.Write<float>("f2", 1f);
            root.Write<int>("f2", 2);
            Assert.AreEqual(ValueType.Float32, root.GetValueView("f2").Type);
        }

        [Test]
        public void Storage_WriteNoRescheme_WillThrow_OnExpandBlock()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;
            root.Write<byte>("v", 1);
            Assert.Throws<ArgumentException>(() => root.Write<int>("v", 1, allowRescheme: false));
            root.Write<float>("v", 1f);
            Assert.Throws<ArgumentException>(() => root.Write<double>("v", 1.0, allowRescheme: false));
        }

        [Test]
        public void Storage_WriteNoRescheme_FieldTypeConstant()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;
            root.Write<double>("v", 1.0);
            Assert.DoesNotThrow(() => root.Write<int>("v", 5, allowRescheme: false));
            Assert.AreEqual(ValueType.Float64, root.GetValueView("v").Type);
        }

        [Test]
        public void Storage_String_RoundTrip()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.Write("name", "Minerva");
            Assert.AreEqual("Minerva", root.ReadString("name"));
            root.Write("name", "Core");
            Assert.AreEqual("Core", root.ReadString("name"));
        }

        #endregion

        #region Path-based accessors

        [Test]
        public void StorageObject_Path_Write_Read_Scalar()
        {
            using var s = new Storage();
            var root = s.Root;
            root.WritePath("stats.hp", 55);
            Assert.AreEqual(55, root.ReadPath<int>("stats.hp"));
        }

        [Test]
        public void StorageObject_Path_Write_Read_String()
        {
            using var s = new Storage();
            var root = s.Root;
            root.WritePath("profile.name", "Alice");
            Assert.AreEqual("Alice", root.ReadStringPath("profile.name"));
        }

        [Test]
        public void StorageObject_Path_Write_Read_Array()
        {
            using var s = new Storage();
            var root = s.Root;
            root.WriteArrayPath("stats.values", new[] { 3, 6, 9 });
            var arr = root.ReadArrayPath<int>("stats.values");
            CollectionAssert.AreEqual(new[] { 3, 6, 9 }, arr);
        }

        [Test]
        public void StorageObject_Path_ObjectArray_Element()
        {
            using var s = new Storage();
            var root = s.Root;
            root.GetArrayByPath("actors".AsSpan(), TypeData.Ref, true).EnsureLength(2);
            root.WritePath("actors[1].hp", 88);
            Assert.AreEqual(88, root.ReadPath<int>("actors[1].hp"));
        }

        #endregion

        #region Array API

        [Test]
        public void StorageObject_ValueArray_Roundtrip()
        {
            using var s = new Storage();
            var root = s.Root;
            root.WriteArray("ints", new[] { 1, 2, 3 });
            var arr = root.GetArray("ints");
            arr.Write(1, 99);
            CollectionAssert.AreEqual(new[] { 1, 99, 3 }, root.ReadArray<int>("ints"));
        }

        [Test]
        public void StorageObject_ObjectArray_Create_Read()
        {
            using var s = new Storage();
            var root = s.Root;
            var objArr = root.GetArrayByPath("items".AsSpan(), TypeData.Ref, true);
            objArr.EnsureLength(2);
            objArr.GetObject(0).Write("quality", 7);
            objArr.GetObject(1).Write("quality", 9);
            Assert.AreEqual(7, root.ReadPath<int>("items[0].quality"));
            Assert.AreEqual(9, root.ReadPath<int>("items[1].quality"));
        }

        [Test]
        public void StorageObject_Array_Clear_And_ClearAt()
        {
            using var s = new Storage();
            var root = s.Root;
            var arr = root.GetArrayByPath("objs".AsSpan(), TypeData.Ref, true);
            arr.EnsureLength(2);
            arr.GetObject(0).Write("id", 1);
            arr.GetObject(1).Write("id", 2);
            arr.ClearAt(0);
            Assert.IsFalse(arr.TryGetObject(0, out _));
            arr.Clear();
            Assert.IsFalse(arr.TryGetObject(1, out _));
        }

        [Test]
        public void StorageObject_ValueArray_EnsureLength_Resize()
        {
            using var s = new Storage();
            var root = s.Root;
            root.WriteArray("nums", new[] { 1, 2 });
            var arr = root.GetArray("nums");
            arr.EnsureLength(5);
            Assert.AreEqual(5, arr.Length);
            arr.Resize(2);
            CollectionAssert.AreEqual(new[] { 1, 2 }, root.ReadArray<int>("nums"));
        }

        #endregion

        #region Indexer & TryGetMember

        [Test]
        public void Storage_RootIndexer_ReturnsMember()
        {
            using var s = new Storage();
            s.Root.Write("hp", 22);
            var m = s["hp"];
            Assert.AreEqual(22, m.Read<int>());
        }

        [Test]
        public void Storage_MemberIndexer_Nested()
        {
            using var s = new Storage();
            s.Root.GetObject("player").Write("level", 9);
            var player = s["player"];
            Assert.AreEqual(9, player["level"].Read<int>());
        }

        [Test]
        public void Storage_TryGetMember_PathFound_And_Missing()
        {
            using var s = new Storage();
            s.Root.Write("x", 5);
            Assert.IsTrue(s.TryGetMember("x", out var found));
            Assert.AreEqual(5, found.Read<int>());
            Assert.IsFalse(s.TryGetMember("missing", out _));
        }

        [Test]
        public void IntAccessor_ReadWrite_Scalar_Field()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Write via accessor
            root.Int["score"] = 123;

            // Read via accessor
            int v = root.Int["score"];
            Assert.That(v, Is.EqualTo(123));

            // Read via generic API
            Assert.That(root.Read<int>("score"), Is.EqualTo(123));
        }

        [Test]
        public void LongAccessor_ReadWrite_Scalar_Field_And_Implicit_From_Int()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Write int then read long (implicit conversion at read time)
            root.Int["counter"] = 42;
            long lv = root.Long["counter"];
            Assert.That(lv, Is.EqualTo(42L));

            // Overwrite via long accessor
            root.Long["counter"] = 1234567890123L;
            Assert.That(root.Long["counter"], Is.EqualTo(1234567890123L));
        }

        [Test]
        public void FloatAccessor_ReadWrite_Scalar_Field_And_Conversion_From_Double()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Write double then read float (explicit read conversion)
            root.Double["speed"] = 3.5d;
            float f = root.Float["speed"];
            Assert.That(f, Is.InRange(3.4999f, 3.5001f));

            // Overwrite via float accessor
            root.Float["speed"] = 1.25f;
            Assert.That(root.Double["speed"], Is.InRange(1.2499d, 1.2501d));
        }

        [Test]
        public void DoubleAccessor_ReadWrite_Scalar_Field()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Double["ratio"] = 0.75;
            Assert.That(root.Double["ratio"], Is.InRange(0.7499d, 0.7501d));

            // Read via generic API
            Assert.That(root.Read<double>("ratio"), Is.InRange(0.7499d, 0.7501d));
        }

        [Test]
        public void StringAccessor_ReadWrite_String_Field()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Write via string accessor
            root.String["title"] = "Hello";
            Assert.That(root.String["title"], Is.EqualTo("Hello"));

            // Overwrite and read via ReadString API
            root.String["title"] = "World";
            Assert.That(root.ReadString("title"), Is.EqualTo("World"));
        }

        [Test]
        public void Accessors_Work_With_Path_WriteRead()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Use generic WritePath then access via accessors
            root.WritePath("stats.hp", 100);
            Assert.That(root.GetObject("stats").Int["hp"], Is.EqualTo(100)); // path-based read via Read<T>(string)

            // Update via accessor and read via ReadPath
            root.GetObject("stats").Int["hp"] = 250;
            Assert.That(root.ReadPath<int>("stats.hp"), Is.EqualTo(250));

            // String path
            root.GetObject("profile").String["name"] = "Alice";
            Assert.That(root.ReadStringPath("profile.name"), Is.EqualTo("Alice"));
        }

        [Test]
        public void Accessors_Work_With_Chain()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            using var storage2 = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var root2 = storage2.Root;

            root.WritePath("stats.a", 100);
            root.WritePath("stats.b", 200);
            root.WritePath("stats.c", 300);
            root2.WritePath("stats.c", 300);
            var stats = root.GetObject("stats");
            var stats2 = root2.GetObject("stats");
            stats.Int["a"] = stats.Int["b"] + 50;

            Assert.That(stats.Int["b"], Is.EqualTo(200));
            Assert.That(root.ReadPath<int>("stats.a"), Is.EqualTo(250));

            stats.Int["a"] = stats.Int["b"] + stats.Int["c"];

            Assert.That(stats.Int["c"], Is.EqualTo(300));
            Assert.That(stats.Int["b"], Is.EqualTo(200));
            Assert.That(root.ReadPath<int>("stats.a"), Is.EqualTo(500));

            stats.Int["a"] += stats.Int["b"] + stats.Int["c"];

            Assert.That(stats.Int["c"], Is.EqualTo(300));
            Assert.That(stats.Int["b"], Is.EqualTo(200));
            Assert.That(root.ReadPath<int>("stats.a"), Is.EqualTo(1000));



            stats.Int["a"] += stats.Int["b"] + stats2.Int["c"];

            Assert.That(stats.Int["c"], Is.EqualTo(300));
            Assert.That(stats.Int["b"], Is.EqualTo(200));
            Assert.That(root.ReadPath<int>("stats.a"), Is.EqualTo(1500));
        }

        [Test]
        public void Accessors_Raise_Write_Events_On_Update()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var events = new List<StorageEventArgs>();
            using var sub = root.Subscribe((in StorageEventArgs a) => events.Add(a));

            // Int write
            root.Int["points"] = 7;
            // Double write
            root.Double["ratio"] = 0.5;
            // String write
            root.String["desc"] = "ok";

            Assert.That(events.Count, Is.GreaterThanOrEqualTo(3));
            // Check last event path consistency
            Assert.That(events[^1].Event, Is.EqualTo(StorageEvent.Write));
            Assert.That(events[^1].Path, Is.EqualTo("desc"));
        }

        [Test]
        public void Accessors_TryRead_Default_Behavior_When_Missing()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Using TryRead<T> directly to verify default behavior when missing
            Assert.That(root.TryRead<int>("missing_int", out var iv), Is.False);
            Assert.That(iv, Is.EqualTo(default(int)));

            Assert.That(root.TryRead<long>("missing_long", out var lv), Is.False);
            Assert.That(lv, Is.EqualTo(default(long)));

            Assert.That(root.TryRead<float>("missing_float", out var fv), Is.False);
            Assert.That(fv, Is.EqualTo(default(float)));

            Assert.That(root.TryRead<double>("missing_double", out var dv), Is.False);
            Assert.That(dv, Is.EqualTo(default(double)));
        }
        #endregion

        #region StorageMember direct properties / metadata

        [Test]
        public void StorageMember_DirectProperties_Roundtrip()
        {
            using var s = new Storage();
            var root = s.Root;
            root.Write("vInt", 1);
            root.Write("vFloat", 1f);
            root.Write("vDouble", 2.0);
            root.Write("vLong", 3L);
            root.Write("vStr", "abc");

            var mi = root.GetMember("vInt");
            var mf = root.GetMember("vFloat");
            var md = root.GetMember("vDouble");
            var ml = root.GetMember("vLong");
            var ms = root.GetMember("vStr");

            mi.Int = 11;
            mf.Float = 1.5f;
            md.Double = 6.25;
            ml.Long = 999999999999L;
            ms.String = "xyz";

            Assert.AreEqual(11, mi.Int);
            Assert.AreEqual(1.5f, mf.Float);
            Assert.AreEqual(6.25, md.Double);
            Assert.AreEqual(999999999999L, ml.Long);
            Assert.AreEqual("xyz", ms.String);
        }

        [Test]
        public void StorageMember_Metadata_IsArray_IsArrayMember_ArrayIndex_ArrayLength()
        {
            using var s = new Storage();
            var root = s.Root;
            var arr = root.GetArrayByPath("nums".AsSpan(), TypeData.Of<int>(), true);
            arr.EnsureLength(3);
            arr.Write(0, 5);
            var m0 = new StorageMember(root, "nums", 0);
            Assert.IsTrue(root.GetMember("nums").IsArray);
            Assert.IsTrue(m0.IsArrayMember);
            Assert.AreEqual(0, m0.ArrayIndex);
            Assert.AreEqual(3, m0.ArrayLength);
        }

        [Test]
        public void StorageMember_ChangeFieldType_Works()
        {
            using var s = new Storage();
            var root = s.Root;
            root.Write("v", 5);
            var m = root.GetMember("v");
            m.ChangeFieldType(TypeData.Of<float>(), null);
            root.Write<float>("v", 3.5f);
            Assert.AreEqual(3.5f, root.Read<float>("v"));
        }

        #endregion

        #region StorageScalar & StorageScalar<T>

        [Test]
        public void StorageScalar_NonGeneric_ReadWrite()
        {
            using var s = new Storage();
            var root = s.Root;
            root.Write("counter", 10);
            var scalar = root.GetMember("counter").AsScalar();
            scalar.Write(42);
            Assert.AreEqual(42, scalar.Read<int>());
        }

        [Test]
        public void StorageScalar_Generic_ReadWrite()
        {
            using var s = new Storage();
            var root = s.Root;
            root.Write("score", 1);
            var g = root.GetMember("score").AsScalar<int>();
            g.Write(99);
            Assert.AreEqual(99, g.Read());
        }

        [Test]
        public void StorageScalar_ArrayElement_ReadWrite()
        {
            using var s = new Storage();
            var root = s.Root;
            var arr = root.GetArrayByPath("vals".AsSpan(), TypeData.Of<int>(), true);
            arr.EnsureLength(2);
            arr.Write(0, 7);
            arr.Write(1, 8);
            var m1 = new StorageMember(root, "vals", 1);
            m1.AsScalar().Write(66);
            Assert.AreEqual(66, m1.AsScalar().Read<int>());
        }

        [Test]
        public void StorageScalar_TypeMismatch_Explicit_Convert()
        {
            using var s = new Storage();
            var root = s.Root;
            root.Write("hp", 100);
            var m = root.GetMember("hp");
            // Explicit mismatch: reading double from int
            try
            {
                var d = m.AsScalar().Read<double>(true);
                Assert.AreEqual(100d, d);   // auto conversion works
            }
            catch (InvalidOperationException) { }
        }

        #endregion

        #region Error cases

        [Test]
        public void Error_NonExistingMember_ReadDefault_And_ExistFalse()
        {
            using var s = new Storage();
            var root = s.Root;
            var m = root.GetMember("no.field");
            Assert.IsFalse(m.Exist);
            try
            {
                m.Read<int>(true);
                Assert.Fail("Getting non-existing member should throw.");
            }
            catch (Exception) { }
        }

        [Test]
        public void Error_ArrayOutOfRange_DisposedMember()
        {
            using var s = new Storage();
            var root = s.Root;
            var arr = root.GetArrayByPath("nums".AsSpan(), TypeData.Of<int>(), true);
            arr.EnsureLength(1);
            var outMember = new StorageMember(root, "nums", 3);
            Assert.IsTrue(outMember.IsDisposed);
            try
            {
                outMember.AsScalar().Read<int>();
                Assert.Fail();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        [Test]
        public void Error_DisposedStorage_MemberAccessThrows()
        {
            var s = new Storage();
            s.Root.Write("alive", 1);
            var m = s.Root.GetMember("alive");
            s.Dispose();
            Assert.IsTrue(m.IsDisposed);
            try
            {
                _ = m.Int;
                Assert.Fail();
            }
            catch (ObjectDisposedException) { }
            try
            {
                m.Int = 2;
                Assert.Fail();
            }
            catch (ObjectDisposedException) { }
            try
            {
                _ = m.AsArray();
                Assert.Fail();
            }
            catch (ObjectDisposedException) { }
            try
            {
                _ = m.AsObject();
                Assert.Fail();
            }
            catch (ObjectDisposedException) { }
        }

        #endregion

        #region Query DSL minimal smoke

        [Test]
        public void StorageQuery_Chaining_ReadWrite_Finalize()
        {
            using var s = new Storage();
            var root = s.Root;
            root.Query().Location("player").Location("hp").Write(33);
            var hp = root.Query().Location("player").Location("hp").Read<int>();
            Assert.AreEqual(33, hp);
        }

        [Test]
        public void StorageQuery_Expect_Object_Scalar_Success()
        {
            using var s = new Storage();
            var root = s.Root;
            root.Query().Location("player").Make().Object();
            root.Query().Location("player").Location("hp").Write(10);
            var q = root.Query().Location("player").Expect().Object().Location("hp").Expect().Scalar<int>();
            Assert.IsTrue(q.Result.Success);
        }

        [Test]
        public void StorageQuery_Expect_Fail_Scalar_TypeMismatch()
        {
            using var s = new Storage();
            var root = s.Root;
            root.Query().Location("data").Location("value").Write(5);
            var q = root.Query().Location("data").Location("value").Expect().Scalar<float>();
            Assert.IsFalse(q.Result.Success);
        }

        #endregion
        #endregion


        #region Enumerate

        [Test]
        public void StorageObject_Enumerate_Members()
        {
            using var s = new Storage();
            var root = s.Root;

            root.Write("a", 1);
            root.Write("b", 2f);
            root.Write("c", "str");
            root.GetObject("child").Write("hp", 10);

            var names = new List<string>();
            foreach (var m in root)
            {
                Assert.IsFalse(m.IsArrayMember);
                names.Add(m.Name.ToString());
            }

            CollectionAssert.IsSubsetOf(new[] { "a", "b", "c", "child" }, names);
            Assert.That(names.Count, Is.GreaterThanOrEqualTo(4));
        }

        [Test]
        public void StorageArray_Enumerate_ValueArray_Members()
        {
            using var s = new Storage();
            var root = s.Root;
            root.WriteArray("nums", new[] { 5, 6, 7 });

            var arr = root.GetArray("nums");
            var values = new List<int>();
            var count = 0;

            foreach (var m in arr)
            {
                Assert.IsTrue(m.IsArrayMember);
                Assert.AreEqual(arr.Length, m.ArrayLength);
                values.Add(m.Read<int>());
                count++;
            }

            Assert.AreEqual(arr.Length, count);
            CollectionAssert.AreEqual(new[] { 5, 6, 7 }, values);
        }

        [Test]
        public void StorageArray_Enumerate_ObjectArray_Slots()
        {
            using var s = new Storage();
            var root = s.Root;

            var objArr = root.GetArrayByPath("entities".AsSpan(), TypeData.Ref, true);
            objArr.EnsureLength(3);
            objArr.GetObject(0).Write("hp", 10);
            objArr.GetObject(2).Write("hp", 30);

            var slotCount = 0;
            var hps = new List<int>();

            foreach (var slot in objArr)
            {
                Assert.IsTrue(slot.IsArrayMember);
                Assert.AreEqual(objArr.Length, slot.ArrayLength);
                slotCount++;

                // Only populated slots will succeed in reading nested field.
                var o = slot.AsObject();
                if (!o.IsNull && o.TryRead<int>("hp", out var hp))
                    hps.Add(hp);
            }

            Assert.AreEqual(objArr.Length, slotCount);
            CollectionAssert.AreEquivalent(new[] { 10, 30 }, hps);
        }

        [Test]
        public void StorageObject_Enumerate_After_TypeChanges()
        {
            using var s = new Storage();
            var root = s.Root;

            root.Write("v", 1);
            root.Write("v", 2.5f); // size-stable change
            root.Write("w", 9L);

            var members = new List<(string name, ValueType vt)>();
            foreach (var m in root)
                members.Add((m.Name.ToString(), m.ValueType));

            Assert.IsTrue(members.Exists(x => x.name == "v" && x.vt == ValueType.Float32));
            Assert.IsTrue(members.Exists(x => x.name == "w" && x.vt == ValueType.Int64));
        }

        [Test]
        public void StorageArray_Enumerate_ValueArray_ModifyDuringIteration()
        {
            using var s = new Storage();
            var root = s.Root;

            root.WriteArray("vals", new[] { 1, 2, 3, 4 });
            var arr = root.GetArray("vals");

            foreach (var m in arr)
            {
                var current = m.Read<int>();
                m.Write(current * 2);
            }

            CollectionAssert.AreEqual(new[] { 2, 4, 6, 8 }, root.ReadArray<int>("vals"));

            foreach (var m in arr)
            {
                m.Int *= 2;
            }

            CollectionAssert.AreEqual(new[] { 4, 8, 12, 16 }, root.ReadArray<int>("vals"));
        }

        #endregion
    }
}
