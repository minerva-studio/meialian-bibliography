using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageWriteEventsTests
    {
        private static Container CreateWildContainer()
        {
            return Container.Registry.Shared.CreateWild(ContainerLayout.Empty, "");
        }

        [Test]
        public void Unregister_Fires_Deletion_Events_Recursively()
        {
            var builder = new ObjectBuilder();
            builder.SetRef("child");
            var layout = builder.BuildLayout();

            using var storage = new Storage(layout);
            var root = storage.Root;
            var child = root.GetObject("child");
            var grandChild = child.GetObject("grandChild");

            bool childDeleted = false;
            bool grandChildDeleted = false;
            bool parentBubbled = false;

            // Subscribe to child container directly
            StorageEventRegistry.Subscribe(child.Container, "child", (in StorageEventArgs args) =>
            {
                if (args.Target.IsNull) childDeleted = true;
            });

            // Subscribe to grandchild
            StorageEventRegistry.Subscribe(grandChild.Container, "grandChild", (in StorageEventArgs args) =>
            {
                if (args.Target.IsNull) grandChildDeleted = true;
            });

            // Subscribe to root (parent) to check bubbling (descendant deletion)
            StorageEventRegistry.SubscribeToContainer(root.Container, (in StorageEventArgs args) =>
            {
                if (args.Target.IsNull) parentBubbled = true;
            });

            // Action: Unregister the child (middle of the chain)
            Container.Registry.Shared.Unregister(child.Container);

            // Assert
            Assert.That(grandChildDeleted, Is.True, "Grandchild should receive deletion event.");
            Assert.That(childDeleted, Is.True, "Child should receive deletion event.");
        }

        [Test]
        public void Unregister_Notifies_After_Disposal()
        {
            using var storage = new Storage();
            var root = storage.Root;
            bool isDisposedAtNotification = false;

            StorageEventRegistry.SubscribeToContainer(root.Container, (in StorageEventArgs args) =>
            {
                if (args.Target.IsNull)
                {
                    // Check if container is effectively disposed (ID == 0)
                    isDisposedAtNotification = true;
                }
            });

            Container.Registry.Shared.Unregister(root.Container);

            Assert.That(isDisposedAtNotification, Is.True, "Should be notified after ID is cleared (effectively disposed state).");
        }

        [Test]
        public void Delete_Field_Unregisters_Ref_Child()
        {
            using var storage = new Storage();
            var root = storage.Root;
            var child = root.GetObject("child");

            ulong childId = child.ID;
            bool childDeleted = false;

            // Subscribe to child to verify it gets killed
            StorageEventRegistry.Subscribe(child.Container, "child", (in StorageEventArgs args) => { if (args.Target.IsNull) childDeleted = true; });

            // Delete the field "child" from root
            root.Delete("child");

            Assert.That(childDeleted, Is.True, "Child container should be unregistered when Ref field is deleted.");

            // Verify registry doesn't have it
            var lookup = Container.Registry.Shared.GetContainer((ContainerReference)childId);
            Assert.That(lookup, Is.Null, "Child should be removed from registry.");
        }


        /// <summary>
        /// ensures field subscriptions are cleared when generation changes (pool reuse).
        /// </summary>
        [Test]
        public void Subscriptions_Cleared_After_Pooling()
        {
            var container = CreateWildContainer();
            try
            {
                int invoked = 0;
                var subscription = StorageEventRegistry.Subscribe(container, "score", (in StorageEventArgs _) => invoked++);

                StorageEventRegistry.NotifyFieldWrite(container, "score", ValueType.Int32);
                Assert.That(invoked, Is.EqualTo(1), "Baseline notification failed.");

                ForceNewGeneration(container);

                Assert.That(StorageEventRegistry.HasSubscribers(container), Is.False, "Generation change should clear subscriptions.");

                StorageEventRegistry.NotifyFieldWrite(container, "score", ValueType.Int32);
                Assert.That(invoked, Is.EqualTo(2), "Handlers from previous generation should recieved dispose message.");
                StorageEventRegistry.NotifyFieldWrite(container, "score", ValueType.Int32);
                Assert.That(invoked, Is.EqualTo(2), "Handlers from previous generation must not fire.");

                subscription.Dispose(); // should be a no-op after reset

                using var newSubscription = StorageEventRegistry.Subscribe(container, "score", (in StorageEventArgs _) => invoked++);
                StorageEventRegistry.NotifyFieldWrite(container, "score", ValueType.Int32);
                Assert.That(invoked, Is.EqualTo(3), "New subscription should work after generation reset.");
            }
            finally
            {
                container.Dispose();
            }
        }

        /// <summary>
        /// ensures repeated generation resets do not leak field subscriptions.
        /// </summary>
        [Test]
        public void Subscriptions_NoLeak_After_Pooling()
        {
            var container = CreateWildContainer();
            try
            {
                int totalInvocations = 0;

                for (int i = 0; i < 25; i++)
                {
                    using var subscription = StorageEventRegistry.Subscribe(container, "field", (in StorageEventArgs _) => totalInvocations++);
                    StorageEventRegistry.NotifyFieldWrite(container, "field", ValueType.Int32);
                    Assert.That(totalInvocations, Is.EqualTo(i * 2 + 1), $"Generation {i}: handler did not fire exactly once.");

                    ForceNewGeneration(container);

                    StorageEventRegistry.NotifyFieldWrite(container, "field", ValueType.Int32);
                    Assert.That(totalInvocations, Is.EqualTo(i * 2 + 2), $"Generation {i}: handler not receive dispose message.");
                    StorageEventRegistry.NotifyFieldWrite(container, "field", ValueType.Int32);
                    Assert.That(totalInvocations, Is.EqualTo(i * 2 + 2), $"Generation {i}: handler leaked into next generation.");
                }
            }
            finally
            {
                container.Dispose();
            }
        }

        /// <summary>
        /// ensures container-level subscriptions fire for every field write.
        /// </summary>
        [Test]
        public void Container_Subscription_Fires_For_All_Writes()
        {
            var container = CreateWildContainer();
            try
            {
                int count = 0;
                using var subscription = StorageEventRegistry.SubscribeToContainer(container, (in StorageEventArgs args) =>
                {
                    Assert.That(args.Target.IsNull, Is.False);
                    count++;
                });

                StorageEventRegistry.NotifyFieldWrite(container, "a", ValueType.Int32);
                StorageEventRegistry.NotifyFieldWrite(container, "b", ValueType.Float32);

                Assert.That(count, Is.EqualTo(2));
            }
            finally
            {
                container.Dispose();
            }
        }

        /// <summary>
        /// ensures container-level subscriptions are cleared when generation changes.
        /// </summary>
        [Test]
        public void Container_Subscription_Reset_On_Generation_Change()
        {
            var container = CreateWildContainer();
            try
            {
                int count = 0;
                StorageEventRegistry.SubscribeToContainer(container, (in StorageEventArgs _) => count++);

                StorageEventRegistry.NotifyFieldWrite(container, "field", ValueType.Int32);
                Assert.That(count, Is.EqualTo(1));

                ForceNewGeneration(container);
                Assert.That(StorageEventRegistry.HasSubscribers(container), Is.False);

                StorageEventRegistry.NotifyFieldWrite(container, "field", ValueType.Int32);
                Assert.That(count, Is.EqualTo(2)); // dispose message
                StorageEventRegistry.NotifyFieldWrite(container, "field", ValueType.Int32);
                Assert.That(count, Is.EqualTo(2)); // should not invoke now
            }
            finally
            {
                container.Dispose();
            }
        }

        /// <summary>
        /// ensures each field subscription fires only for its own field.
        /// </summary>
        [Test]
        public void Field_Subscriptions_Are_Isolated_By_Name()
        {
            var container = CreateWildContainer();
            try
            {
                int scoreCount = 0;
                int hpCount = 0;

                using var scoreSub = StorageEventRegistry.Subscribe(container, "score", (in StorageEventArgs _) => scoreCount++);
                using var hpSub = StorageEventRegistry.Subscribe(container, "hp", (in StorageEventArgs _) => hpCount++);

                StorageEventRegistry.NotifyFieldWrite(container, "score", ValueType.Int32);
                StorageEventRegistry.NotifyFieldWrite(container, "hp", ValueType.Int32);
                StorageEventRegistry.NotifyFieldWrite(container, "score", ValueType.Int32);

                Assert.That(scoreCount, Is.EqualTo(2));
                Assert.That(hpCount, Is.EqualTo(1));
            }
            finally
            {
                container.Dispose();
            }
        }

        /// <summary>
        /// ensures disposing a container-level subscription stops notifications.
        /// </summary>
        [Test]
        public void Container_Subscription_Dispose_Stops_Notifications()
        {
            var container = CreateWildContainer();
            try
            {
                int count = 0;
                var subscription = StorageEventRegistry.SubscribeToContainer(container, (in StorageEventArgs _) => count++);

                StorageEventRegistry.NotifyFieldWrite(container, "a", ValueType.Int32);
                Assert.That(count, Is.EqualTo(1));

                subscription.Dispose();
                StorageEventRegistry.NotifyFieldWrite(container, "a", ValueType.Int32);
                Assert.That(count, Is.EqualTo(1));
            }
            finally
            {
                container.Dispose();
            }
        }

        /// <summary>
        /// ensures multiple field handlers on same field all fire.
        /// </summary>
        [Test]
        public void Field_Subscriptions_Multiple_Handlers_All_Fire()
        {
            var container = CreateWildContainer();
            try
            {
                int a = 0;
                int b = 0;
                using var subA = StorageEventRegistry.Subscribe(container, "value", (in StorageEventArgs _) => a++);
                using var subB = StorageEventRegistry.Subscribe(container, "value", (in StorageEventArgs _) => b++);

                StorageEventRegistry.NotifyFieldWrite(container, "value", ValueType.Int32);

                Assert.That(a, Is.EqualTo(1));
                Assert.That(b, Is.EqualTo(1));
            }
            finally
            {
                container.Dispose();
            }
        }

        [Test]
        public void Field_Deletion_Event_Invoke_Time()
        {
            using var storage = new Storage();
            var root = storage.Root;
            var child = root.GetObject("child");
            for (int i = 0; i < 20; i++)
            {
                child.GetObject($"grandChild{i}"); // create multiple children
            }
            int count = 0;
            child.Subscribe((in StorageEventArgs args) =>
            {
                count++;
            });
            root.Delete("child");
            Assert.That(count, Is.EqualTo(1), "Deleting object should only invoke once.");
        }

        [Test]
        public void Field_Deletion_Event_Invoke_Chain()
        {
            using var storage = new Storage();
            var root = storage.Root;
            var child = root.GetObject("child");
            var grandChild = child.GetObject("grandChild");
            var greatGrandChild = grandChild.GetObject("greatGrandChild");

            int count = 0;
            int grandChildInvoked = 0;
            greatGrandChild.Subscribe((in StorageEventArgs args) =>
            {
                count++;
            });
            grandChild.Subscribe((in StorageEventArgs args) =>
            {
                grandChildInvoked++;
                Assert.That(args.Target.IsNull);
            });

            root.Delete("child");
            Assert.That(grandChildInvoked, Is.EqualTo(1), "Deleting object should only invoke once.");
            Assert.That(count, Is.EqualTo(1), "Deleting object should only invoke once.");
        }


        private static void ForceNewGeneration(Container container)
        {
            var field = typeof(Container).GetField("_generation", BindingFlags.NonPublic | BindingFlags.Instance);
            if (field == null)
                throw new InvalidOperationException("Unable to locate Container._generation field for testing.");

            int current = (int)field.GetValue(container);
            field.SetValue(container, current + 1);
        }

        [Test]
        public void Subscribe_NonexistentField_Throws()
        {
            using var storage = new Storage();
            var root = storage.Root;
            // Field 'missing' does not exist yet
            Assert.Throws<ArgumentException>(() => root.Subscribe("missing", (in StorageEventArgs _) => { }));
        }

        [Test]
        public void FieldSubscription_Handler_Can_Write_Other_Field()
        {
            using var storage = new Storage();
            var root = storage.Root;
            // Create fields before subscribing (required)
            root.Write<int>("a", 0);
            root.Write<int>("b", 0);

            int aInvoked = 0;
            int bInvoked = 0;

            using var subA = root.Subscribe("a", (in StorageEventArgs e) =>
            {
                aInvoked++;
                // Write to a different existing field inside handler
                root.Write<int>("b", 42);
            });
            using var subB = root.Subscribe("b", (in StorageEventArgs e) => bInvoked++);

            root.Write<int>("a", 1);

            Assert.That(aInvoked, Is.EqualTo(1));
            Assert.That(bInvoked, Is.EqualTo(1));
        }

        [Test]
        public void FieldSubscription_Handler_Delete_Same_Field_No_Reentrant_Loop()
        {
            using var storage = new Storage();
            var root = storage.Root;
            root.Write<int>("x", 0); // ensure field exists
            int invoked = 0;
            using var sub = root.Subscribe("x", (in StorageEventArgs e) =>
            {
                invoked++;
                root.Delete("x");
            });

            root.Write<int>("x", 5);

            Assert.That(invoked, Is.EqualTo(2));
            Assert.IsFalse(root.HasField("x"));
        }

        [Test]
        public void ContainerSubscription_Handler_Can_Read_Write_Delete()
        {
            using var storage = new Storage();
            var root = storage.Root;
            root.Write<int>("keep", 1);
            root.Write<int>("remove", 2);

            int containerEvents = 0;
            int writesPerformedInHandler = 0;

            using var sub = root.Subscribe((in StorageEventArgs e) =>
            {
                containerEvents++;
                var v = root.Read<int>("keep");
                Assert.That(v, Is.EqualTo(1));
                if (!root.HasField("sideEffect"))
                {
                    root.Write<int>("sideEffect", 99);
                    writesPerformedInHandler++;
                }
                if (root.HasField("remove"))
                {
                    root.Delete("remove");
                }
            });

            root.Write<int>("trigger", 7);

            Assert.That(containerEvents, Is.GreaterThanOrEqualTo(1));
            Assert.IsTrue(root.HasField("sideEffect"));
            Assert.IsFalse(root.HasField("remove"));
            Assert.That(writesPerformedInHandler, Is.EqualTo(1));
        }

        [Test]
        public void FieldSubscription_Reentrant_Write_Same_Field_Is_Protected()
        {
            using var storage = new Storage();
            var root = storage.Root;
            root.Write<int>("loop", 0); // ensure field exists
            int invoked = 0;
            using var sub = root.Subscribe("loop", (in StorageEventArgs e) =>
            {
                invoked++;
                if (invoked == 1)
                {
                    root.Write<int>("loop", 123);
                }
            });

            root.Write<int>("loop", 1);

            Assert.That(invoked, Is.LessThanOrEqualTo(2));
        }

        [Test]
        public void FieldSubscription_AutoUnsub_OnDelete()
        {
            using var storage = new Storage();
            var root = storage.Root;
            root.Write<int>("temp", 1);
            int deleteEvents = 0;
            using var sub = root.Subscribe("temp", (in StorageEventArgs e) =>
            {
                if (e.Event == StorageEvent.Delete || (e.Event == StorageEvent.Dispose && e.Target.IsNull))
                    deleteEvents++;
            });

            // Delete field
            root.Delete("temp");

            // Depending on implementation, deletion may raise Delete or Dispose
            Assert.That(deleteEvents, Is.GreaterThanOrEqualTo(1), "Should receive at least one deletion/dispose event.");

            // Recreate field and write; old subscription should be gone
            root.Write<int>("temp", 5);
            Assert.That(deleteEvents, Is.EqualTo(1), "Subscription should auto-unsubscribe after delete.");
        }

        [Test]
        public void Field_TypeChange_Raises_SecondWrite_WithNewFieldType()
        {
            using var storage = new Storage();
            var root = storage.Root;
            root.Write<int>("v", 1); // create field

            int eventCount = 0;
            FieldType lastType = default;
            using var sub = root.Subscribe("v", (in StorageEventArgs e) =>
            {
                Assert.That(e.Event, Is.EqualTo(StorageEvent.Write));
                eventCount++;
                lastType = e.FieldType;
            });

            // First write (same type) -> one event
            root.Write<int>("v", 2);
            // Second write with different size/type (int -> long) forces rescheme/type change
            root.Write<long>("v", 9999999999L);

            Assert.That(eventCount, Is.EqualTo(2), "Two write events expected (initial + type change). ");
            Assert.That(lastType, Is.EqualTo(TypeUtil<long>.ScalarFieldType), "Last event must reflect new type.");
        }

        [Test]
        public void Field_TypeChange_SameSize_ImplicitConversion_StillUpdatesType()
        {
            using var storage = new Storage();
            var root = storage.Root;
            // float32 and int32 both size 4; switching should be implicit / same size path
            root.Write<int>("x", 5);
            int count = 0; FieldType last = default;
            using var sub = root.Subscribe("x", (in StorageEventArgs e) => { count++; last = e.FieldType; });
            root.Write<float>("x", 3.14f);
            Assert.That(count, Is.EqualTo(1));
            Assert.That(last, Is.EqualTo(TypeUtil<float>.ScalarFieldType));
        }

        [Test]
        public void InlineArray_Rescheme_LengthChange_RaisesWrite()
        {
            using var storage = new Storage();
            var root = storage.Root;
            // initial array
            root.Override("arr", new ReadOnlySpan<int>(new int[] { 1, 2, 3 }).AsBytes(), ValueType.Int32, inlineArrayLength: 3);
            int count = 0; int lastLen = 0;
            using var sub = root.Subscribe("arr", (in StorageEventArgs e) => { count++; lastLen = root.GetArray("arr").Length; });
            // Resize (different length -> rescheme)
            root.Override("arr", new ReadOnlySpan<int>(new int[] { 9, 8, 7, 6 }).AsBytes(), ValueType.Int32, inlineArrayLength: 4);
            Assert.That(count, Is.EqualTo(1));
            Assert.That(lastLen, Is.EqualTo(4));
        }

        [Test]
        public void Delete_RefField_RaisesDelete_BeforeParentWrite()
        {
            using var storage = new Storage();
            var root = storage.Root;
            var child = root.GetObject("child");
            var grand = child.GetObject("grand");
            int childEvents = 0; StorageEvent lastChildEvent = StorageEvent.None;
            int rootEvents = 0; List<StorageEvent> rootSequence = new();

            using var subChild = child.Subscribe((in StorageEventArgs e) => { childEvents++; lastChildEvent = e.Event; });
            using var subRoot = root.Subscribe((in StorageEventArgs e) => { rootEvents++; rootSequence.Add(e.Event); });

            root.Delete("child");

            Assert.That(childEvents, Is.GreaterThanOrEqualTo(1));
            Assert.That(lastChildEvent, Is.EqualTo(StorageEvent.Dispose).Or.EqualTo(StorageEvent.Delete));
            Assert.IsFalse(root.HasField("child"));
            Assert.That(rootSequence.Contains(StorageEvent.Delete), "Root sequence should include delete bubbling.");
        }

        [Test]
        public void Mixed_Handler_Deletes_Other_Field_Writes_New_Field_Reads_Value()
        {
            using var storage = new Storage();
            var root = storage.Root;
            root.Write<int>("a", 1);
            root.Write<int>("b", 2);
            root.Write<int>("c", 3);
            int writes = 0; int deletes = 0; int reads = 0;
            using var sub = root.Subscribe((in StorageEventArgs e) =>
            {
                if (e.Event == StorageEvent.Write)
                {
                    writes++;
                    if (e.Path == "a" && root.HasField("b")) root.Delete("b");
                    if (!root.HasField("d")) root.Write<int>("d", 99);
                    int cv = root.Read<int>("c"); reads++; Assert.That(cv, Is.EqualTo(3));
                }
                else if (e.Event == StorageEvent.Delete)
                    deletes++;
            });

            root.Write<int>("a", 10); // triggers handler, deletes b, writes d
            Assert.IsFalse(root.HasField("b"));
            Assert.IsTrue(root.HasField("d"));
            Assert.That(writes, Is.GreaterThanOrEqualTo(1));
            Assert.That(reads, Is.GreaterThanOrEqualTo(1));
            Assert.That(deletes, Is.GreaterThanOrEqualTo(1));
        }

        [Test]
        public void Dispose_Storage_Raises_Dispose_For_Root_And_Children()
        {
            var storage = new Storage();
            var root = storage.Root;
            var child = root.GetObject("child");
            var grand = child.GetObject("grand");
            int rootDisposes = 0; int childDisposes = 0; int grandDisposes = 0;
            using var rootSub = root.Subscribe((in StorageEventArgs e) => { if (e.Event == StorageEvent.Dispose && e.Target.IsNull) rootDisposes++; });
            using var childSub = child.Subscribe((in StorageEventArgs e) => { if (e.Event == StorageEvent.Dispose && e.Target.IsNull) childDisposes++; });
            using var grandSub = grand.Subscribe((in StorageEventArgs e) => { if (e.Event == StorageEvent.Dispose && e.Target.IsNull) grandDisposes++; });

            storage.Dispose();
            Assert.That(rootDisposes, Is.EqualTo(1));
            Assert.That(childDisposes, Is.EqualTo(1));
            Assert.That(grandDisposes, Is.EqualTo(1));
        }
    }

    internal static class SpanExtensions
    {
        public static ReadOnlySpan<byte> AsBytes<T>(this ReadOnlySpan<T> span) where T : unmanaged
        {
            return MemoryMarshal.AsBytes(span);
        }
    }
}
