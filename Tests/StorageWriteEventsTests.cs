using NUnit.Framework;
using System;
using System.Reflection;

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
            StorageWriteEventRegistry.Subscribe(child.Container, "child", (in StorageFieldWriteEventArgs args) =>
            {
                if (args.Target.IsNull) childDeleted = true;
            });

            // Subscribe to grandchild
            StorageWriteEventRegistry.Subscribe(grandChild.Container, "grandChild", (in StorageFieldWriteEventArgs args) =>
            {
                if (args.Target.IsNull) grandChildDeleted = true;
            });

            // Subscribe to root (parent) to check bubbling (descendant deletion)
            StorageWriteEventRegistry.SubscribeToContainer(root.Container, (in StorageFieldWriteEventArgs args) =>
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

            StorageWriteEventRegistry.SubscribeToContainer(root.Container, (in StorageFieldWriteEventArgs args) =>
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
            StorageWriteEventRegistry.Subscribe(child.Container, "child", (in StorageFieldWriteEventArgs args) => { if (args.Target.IsNull) childDeleted = true; });

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
                var subscription = StorageWriteEventRegistry.Subscribe(container, "score", (in StorageFieldWriteEventArgs _) => invoked++);

                StorageWriteEventRegistry.NotifyField(container, "score", ValueType.Int32);
                Assert.That(invoked, Is.EqualTo(1), "Baseline notification failed.");

                ForceNewGeneration(container);

                Assert.That(StorageWriteEventRegistry.HasSubscribers(container), Is.False, "Generation change should clear subscriptions.");

                StorageWriteEventRegistry.NotifyField(container, "score", ValueType.Int32);
                Assert.That(invoked, Is.EqualTo(1), "Handlers from previous generation must not fire.");

                subscription.Dispose(); // should be a no-op after reset

                using var newSubscription = StorageWriteEventRegistry.Subscribe(container, "score", (in StorageFieldWriteEventArgs _) => invoked++);
                StorageWriteEventRegistry.NotifyField(container, "score", ValueType.Int32);
                Assert.That(invoked, Is.EqualTo(2), "New subscription should work after generation reset.");
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
                    using var subscription = StorageWriteEventRegistry.Subscribe(container, "field", (in StorageFieldWriteEventArgs _) => totalInvocations++);
                    StorageWriteEventRegistry.NotifyField(container, "field", ValueType.Int32);
                    Assert.That(totalInvocations, Is.EqualTo(i + 1), $"Generation {i}: handler did not fire exactly once.");

                    ForceNewGeneration(container);

                    StorageWriteEventRegistry.NotifyField(container, "field", ValueType.Int32);
                    Assert.That(totalInvocations, Is.EqualTo(i + 1), $"Generation {i}: handler leaked into next generation.");
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
                using var subscription = StorageWriteEventRegistry.SubscribeToContainer(container, (in StorageFieldWriteEventArgs args) =>
                {
                    Assert.That(args.Target.IsNull, Is.False);
                    count++;
                });

                StorageWriteEventRegistry.NotifyField(container, "a", ValueType.Int32);
                StorageWriteEventRegistry.NotifyField(container, "b", ValueType.Float32);

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
                StorageWriteEventRegistry.SubscribeToContainer(container, (in StorageFieldWriteEventArgs _) => count++);

                StorageWriteEventRegistry.NotifyField(container, "field", ValueType.Int32);
                Assert.That(count, Is.EqualTo(1));

                ForceNewGeneration(container);
                Assert.That(StorageWriteEventRegistry.HasSubscribers(container), Is.False);

                StorageWriteEventRegistry.NotifyField(container, "field", ValueType.Int32);
                Assert.That(count, Is.EqualTo(1));
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

                using var scoreSub = StorageWriteEventRegistry.Subscribe(container, "score", (in StorageFieldWriteEventArgs _) => scoreCount++);
                using var hpSub = StorageWriteEventRegistry.Subscribe(container, "hp", (in StorageFieldWriteEventArgs _) => hpCount++);

                StorageWriteEventRegistry.NotifyField(container, "score", ValueType.Int32);
                StorageWriteEventRegistry.NotifyField(container, "hp", ValueType.Int32);
                StorageWriteEventRegistry.NotifyField(container, "score", ValueType.Int32);

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
                var subscription = StorageWriteEventRegistry.SubscribeToContainer(container, (in StorageFieldWriteEventArgs _) => count++);

                StorageWriteEventRegistry.NotifyField(container, "a", ValueType.Int32);
                Assert.That(count, Is.EqualTo(1));

                subscription.Dispose();
                StorageWriteEventRegistry.NotifyField(container, "a", ValueType.Int32);
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
                using var subA = StorageWriteEventRegistry.Subscribe(container, "value", (in StorageFieldWriteEventArgs _) => a++);
                using var subB = StorageWriteEventRegistry.Subscribe(container, "value", (in StorageFieldWriteEventArgs _) => b++);

                StorageWriteEventRegistry.NotifyField(container, "value", ValueType.Int32);

                Assert.That(a, Is.EqualTo(1));
                Assert.That(b, Is.EqualTo(1));
            }
            finally
            {
                container.Dispose();
            }
        }


        private static void ForceNewGeneration(Container container)
        {
            var field = typeof(Container).GetField("_generation", BindingFlags.NonPublic | BindingFlags.Instance);
            if (field == null)
                throw new InvalidOperationException("Unable to locate Container._generation field for testing.");

            int current = (int)field.GetValue(container);
            field.SetValue(container, current + 1);
        }
    }
}
