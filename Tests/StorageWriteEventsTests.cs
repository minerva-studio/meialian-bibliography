using NUnit.Framework;
using System;
using System.Reflection;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageWriteEventsTests
    {
        /// <summary>
        /// ensures field subscriptions are cleared when generation changes (pool reuse).
        /// </summary>
        [Test]
        public void Subscriptions_Cleared_After_Pooling()
        {
            var container = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
            try
            {
                int invoked = 0;
                var subscription = StorageWriteEventRegistry.Subscribe(container, "score", (in StorageFieldWriteEventArgs _) => invoked++);

                StorageWriteEventRegistry.Notify(container, "score", ValueType.Int32);
                Assert.That(invoked, Is.EqualTo(1), "Baseline notification failed.");

                ForceNewGeneration(container);

                Assert.That(StorageWriteEventRegistry.HasSubscribers(container), Is.False, "Generation change should clear subscriptions.");

                StorageWriteEventRegistry.Notify(container, "score", ValueType.Int32);
                Assert.That(invoked, Is.EqualTo(1), "Handlers from previous generation must not fire.");

                subscription.Dispose(); // should be a no-op after reset

                using var newSubscription = StorageWriteEventRegistry.Subscribe(container, "score", (in StorageFieldWriteEventArgs _) => invoked++);
                StorageWriteEventRegistry.Notify(container, "score", ValueType.Int32);
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
            var container = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
            try
            {
                int totalInvocations = 0;

                for (int i = 0; i < 25; i++)
                {
                    using var subscription = StorageWriteEventRegistry.Subscribe(container, "field", (in StorageFieldWriteEventArgs _) => totalInvocations++);
                    StorageWriteEventRegistry.Notify(container, "field", ValueType.Int32);
                    Assert.That(totalInvocations, Is.EqualTo(i + 1), $"Generation {i}: handler did not fire exactly once.");

                    ForceNewGeneration(container);

                    StorageWriteEventRegistry.Notify(container, "field", ValueType.Int32);
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
            var container = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
            try
            {
                int count = 0;
                using var subscription = StorageWriteEventRegistry.SubscribeToContainer(container, (in StorageFieldWriteEventArgs args) =>
                {
                    Assert.That(args.Target.IsNull, Is.False);
                    count++;
                });

                StorageWriteEventRegistry.Notify(container, "a", ValueType.Int32);
                StorageWriteEventRegistry.Notify(container, "b", ValueType.Float32);

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
            var container = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
            try
            {
                int count = 0;
                StorageWriteEventRegistry.SubscribeToContainer(container, (in StorageFieldWriteEventArgs _) => count++);

                StorageWriteEventRegistry.Notify(container, "field", ValueType.Int32);
                Assert.That(count, Is.EqualTo(1));

                ForceNewGeneration(container);
                Assert.That(StorageWriteEventRegistry.HasSubscribers(container), Is.False);

                StorageWriteEventRegistry.Notify(container, "field", ValueType.Int32);
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
            var container = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
            try
            {
                int scoreCount = 0;
                int hpCount = 0;

                using var scoreSub = StorageWriteEventRegistry.Subscribe(container, "score", (in StorageFieldWriteEventArgs _) => scoreCount++);
                using var hpSub = StorageWriteEventRegistry.Subscribe(container, "hp", (in StorageFieldWriteEventArgs _) => hpCount++);

                StorageWriteEventRegistry.Notify(container, "score", ValueType.Int32);
                StorageWriteEventRegistry.Notify(container, "hp", ValueType.Int32);
                StorageWriteEventRegistry.Notify(container, "score", ValueType.Int32);

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
            var container = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
            try
            {
                int count = 0;
                var subscription = StorageWriteEventRegistry.SubscribeToContainer(container, (in StorageFieldWriteEventArgs _) => count++);

                StorageWriteEventRegistry.Notify(container, "a", ValueType.Int32);
                Assert.That(count, Is.EqualTo(1));

                subscription.Dispose();
                StorageWriteEventRegistry.Notify(container, "a", ValueType.Int32);
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
            var container = Container.Registry.Shared.CreateWild(ContainerLayout.Empty);
            try
            {
                int a = 0;
                int b = 0;
                using var subA = StorageWriteEventRegistry.Subscribe(container, "value", (in StorageFieldWriteEventArgs _) => a++);
                using var subB = StorageWriteEventRegistry.Subscribe(container, "value", (in StorageFieldWriteEventArgs _) => b++);

                StorageWriteEventRegistry.Notify(container, "value", ValueType.Int32);

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

