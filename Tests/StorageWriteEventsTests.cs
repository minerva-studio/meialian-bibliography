using NUnit.Framework;
using System;
using System.Reflection;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageWriteEventsTests
    {
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

