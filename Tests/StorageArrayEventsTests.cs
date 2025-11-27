using NUnit.Framework;
using System;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageArrayEventsTests
    {
        // NOTE
        // Subscribing root.Subscribe("numbers", ...) causes events whose Target is the array container itself.
        // Inside the handler never re-navigate by field name again (e.Target.ReadArray<int>("numbers")), just use:
        //   e.Target.ReadArray<int>() or e.Target.AsArray()
        // Always treat it as an ephemeral stack-only view.

        // =============== Element writes ===============

        [Test]
        public void ArrayElement_Write_Triggers_FieldEvent_Once()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("numbers", new[] { 1, 2, 3 });
            int writeEvents = 0;
            using var sub = root.Subscribe("numbers", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                // Target is the array container itself.
                Assert.That(e.Target.IsArray(), Is.True);
                writeEvents++;
            });

            Assert.That(root.TryGetArray<int>("numbers".AsSpan(), out var arr), Is.True);
            arr.Write(1, 99);

            Assert.That(writeEvents, Is.EqualTo(1));
            CollectionAssert.AreEqual(new[] { 1, 99, 3 }, root.ReadArray<int>("numbers"));
        }

        [Test]
        public void ArrayElement_TryWrite_OutOfRange_NoEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            root.WriteArray("values", new[] { 10, 20 });
            int writeEvents = 0;
            using var sub = root.Subscribe("values", (in StorageEventArgs e) => writeEvents++);

            Assert.That(root.TryGetArray<int>("values".AsSpan(), out var arr), Is.True);
            bool ok = arr.TryWrite(5, 123);
            Assert.That(ok, Is.False);
            Assert.That(writeEvents, Is.EqualTo(0));
        }

        // =============== Override via StorageArray (recommended) ===============

        [Test]
        public void Array_CopyFrom_TypeUnchange_By_StorageArray_Triggers_WriteEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("nums", new[] { 1, 2, 3 });

            int events = 0;
            ValueType observedType = default;
            float[] observed = null;

            using var sub = root.Subscribe("nums", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                events++;
                var arrayView = e.Target.AsArray();
                observedType = arrayView.Type;
                observed = e.Target.ReadArray<float>();
            });

            var arr = root.GetObject("nums").AsArray();
            arr.CopyFrom(new ReadOnlySpan<float>(new[] { 3.5f, 4.5f }), allowResize: true);

            Assert.That(events, Is.EqualTo(1));
            Assert.That(observedType, Is.EqualTo(ValueType.Int32));
            CollectionAssert.AreEqual(new[] { 3.0f, 4.0f, 3.0f }, observed); // float truncated
        }

        [Test]
        public void Array_CopyFrom_By_StorageArray_Triggers_WriteEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("nums", new[] { 1.5f, 2, 3 });

            int events = 0;
            ValueType observedType = default;
            int[] observed = null;

            using var sub = root.Subscribe("nums", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                events++;
                var arrayView = e.Target.AsArray();
                observedType = arrayView.Type;
                observed = e.Target.ReadArray<int>();
            });

            var arr = root.GetObject("nums").AsArray();
            arr.CopyFrom(new ReadOnlySpan<int>(new[] { 3, 4 }), allowResize: true);

            Assert.That(events, Is.EqualTo(1));
            Assert.That(observedType, Is.EqualTo(ValueType.Float32));
            CollectionAssert.AreEqual(new[] { 3, 4, 3 }, observed);
        }

        [Test]
        public void Array_Override_LengthResize_Triggers_WriteEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("payload", new[] { 1, 2, 3, 4 });

            int events = 0;
            int newLen = -1;
            using var sub = root.Subscribe("payload", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                events++;
                newLen = e.Target.ReadArray<int>().Length;
            });

            var arr = root.GetObject("payload").AsArray();
            arr.Override(new ReadOnlySpan<int>(new[] { 9, 8 }));

            Assert.That(events, Is.EqualTo(1));
            Assert.That(newLen, Is.EqualTo(2));
            CollectionAssert.AreEqual(new[] { 9, 8 }, root.ReadArray<int>("payload"));
        }

        [Test]
        public void Array_CopyFrom_LengthMismatch_Resize()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("nums", new[] { 1f, 2, 3 });

            int events = 0;
            using var sub = root.Subscribe("nums", (in StorageEventArgs _) => events++);

            var arr = root.GetObject("nums").AsArray();

            Assert.That(arr.CopyFrom(new ReadOnlySpan<int>(new[] { 7, 8, 9, 10 }), allowResize: false), Is.EqualTo(3));
            Assert.That(events, Is.EqualTo(1));
            CollectionAssert.AreEqual(new[] { 7f, 8f, 9f }, root.ReadArray<float>("nums"));
        }

        // =============== root.Override (inline array) unwanted Dispose event ===============

        [Test]
        public void RootOverride_Creates_InlineArray_DisposeEvent_TargetUnreadable()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("nums", new[] { 1, 2 });

            int disposeEvents = 0;
            bool targetDisposed = false;

            using var sub = root.Subscribe("nums", (in StorageEventArgs e) =>
            {
                if (e.Event == StorageEvent.Dispose)
                {
                    disposeEvents++;
                    targetDisposed = e.Target.IsNull;
                }
            });

            root.Override("nums", MemoryMarshal.AsBytes(new ReadOnlySpan<int>(new[] { 5, 6, 7 })), ValueType.Int32, inlineArrayLength: 3);

            Assert.That(disposeEvents, Is.EqualTo(1));
            Assert.That(targetDisposed, Is.True);
            CollectionAssert.AreEqual(new[] { 5, 6, 7 }, root.ReadArray<int>("nums"));
        }

        // =============== Resize & Clear ===============

        [Test]
        public void Array_Resize_Triggers_WriteEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("numbers", new[] { 1, 2 });

            int events = 0;
            int newLen = -1;
            using var sub = root.Subscribe("numbers", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                events++;
                newLen = e.Target.ReadArray<int>().Length;
            });

            var arr = root.GetObject("numbers").AsArray();
            arr.Resize(5);

            Assert.That(events, Is.EqualTo(1));
            Assert.That(newLen, Is.EqualTo(5));
        }

        [Test]
        public void ValueArray_ClearAt_Fires_WriteEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("values", new[] { 5, 6, 7 });

            int events = 0;
            using var sub = root.Subscribe("values", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                events++;
            });

            var arr = root.GetObject("values").AsArray();
            arr.ClearAt(1);

            Assert.That(events, Is.EqualTo(1));
            CollectionAssert.AreEqual(new[] { 5, 0, 7 }, root.ReadArray<int>("values"));
        }

        [Test]
        public void Array_Clear_Fires_WriteEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("values", new[] { 5, 6, 7 });

            int events = 0;
            using var sub = root.Subscribe("values", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                events++;
            });

            var arr = root.GetObject("values").AsArray();
            arr.Clear();

            Assert.That(events, Is.EqualTo(1));
            CollectionAssert.AreEqual(new[] { 0, 0, 0 }, root.ReadArray<int>("values"));
        }

        // =============== String array write ===============

        [Test]
        public void StringArray_WriteString_Triggers_WriteEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.Write("title", "A");

            int events = 0;
            string inside = null;
            using var sub = root.Subscribe("title", (in StorageEventArgs e) =>
            {
#if UNITY_EDITOR
                UnityEngine.Debug.Log(e);
#endif
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                // Target is the string array container
                inside = e.Target.ReadString();
                events++;
            });

            var arr = root.GetObject("title").AsArray();
            arr.Write("HelloWorld");

            Assert.That(events, Is.EqualTo(1));
            Assert.That(inside, Is.EqualTo("HelloWorld"));
        }

        [Test]
        public void StringArray_Reentrant_Write_DoubleEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.Write("name", "A");

            int events = 0;
            bool second = false;
            using var sub = root.Subscribe("name", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                events++;
                if (!second)
                {
                    second = true;
                    // Write new string on the same string array container
                    e.Target.WriteString("B");
                }
            });

            root.Write("name", "C");
            Assert.That(events, Is.EqualTo(2));
            Assert.That(root.ReadString("name"), Is.EqualTo("B"));
        }

        // =============== Raw & CopyFrom (silent) ===============

        [Test]
        public void Array_RawWriteView_NoEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("numbers", new[] { 1, 2, 3 });

            int events = 0;
            using var sub = root.Subscribe("numbers", (in StorageEventArgs _) => events++);

            var arr = root.GetObject("numbers").AsArray();
            var raw = arr.Raw;
            raw[1] = new ValueView(BitConverter.GetBytes(42), ValueType.Int32);
            Assert.That(events, Is.EqualTo(0));
            CollectionAssert.AreEqual(new[] { 1, 42, 3 }, root.ReadArray<int>("numbers"));
        }

        [Test]
        public void Array_CopyFrom_NoEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            root.WriteArray("values", new[] { 1, 2, 3, 4 });

            int events = 0;
            using var sub = root.Subscribe("values", (in StorageEventArgs _) => events++);

            var arr = root.GetObject("values").AsArray();
            int written = arr.CopyFrom(new ReadOnlySpan<int>(new[] { 9, 8 }));
            Assert.That(written, Is.EqualTo(2));
            Assert.That(events, Is.EqualTo(0));
            CollectionAssert.AreEqual(new[] { 9, 8, 3, 4 }, root.ReadArray<int>("values"));
        }

        // =============== Object array slot clear ===============

        [Test]
        public void ObjectArray_ClearAt_Unregisters_SingleChild_Fires_WriteEvent()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            var objArr = root.GetArrayByPath("entities".AsSpan(), TypeData.Ref, true);
            objArr.EnsureLength(2);
            root.WritePath("entities[0].hp", 10);

            int events = 0;
            using var sub = root.Subscribe("entities", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                events++;
            });

            objArr.ClearAt(0);

            Assert.That(events, Is.EqualTo(1));
            Assert.That(objArr.TryGetObject(0, out var removed), Is.False);
        }
    }
}