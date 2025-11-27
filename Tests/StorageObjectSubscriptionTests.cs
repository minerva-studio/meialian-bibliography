using NUnit.Framework;
using System;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage.Tests
{
    public class StorageObjectSubscriptionTests
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

        #region Subscribe

        [Test]
        public void Storage_FieldWrite_Subscription_Fires_On_Write()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            int invoked = 0;

            root.Write("score", 0);
            using var subscription = root.Subscribe("score", (in StorageEventArgs args) =>
            {
                invoked++;
                Assert.That(args.Path, Is.EqualTo("score"));
                Assert.That(args.FieldType, Is.EqualTo(TypeUtil<int>.ScalarFieldType));
                Assert.That(args.Target.Read<int>("score"), Is.EqualTo(123));
            });

            root.Write("score", 123);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_Unsubscribes()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            int invoked = 0;

            root.Write("hp", 0);
            var subscription = root.Subscribe("hp", (in StorageEventArgs _) => invoked++);

            root.Write("hp", 10);
            Assert.That(invoked, Is.EqualTo(1));

            subscription.Dispose();
            root.Write("hp", 20);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Missing_Container_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.Throws<ArgumentException>(() => root.Subscribe("player", (in StorageEventArgs _) => { }));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_TryWriteFailure_DoesNotNotify()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write<byte>("small", 1);
            int invoked = 0;

            using var subscription = root.Subscribe("small", (in StorageEventArgs _) => invoked++);

            root.TryWrite<int>("small", 99, allowRescheme: false);
            Assert.That(invoked, Is.EqualTo(0));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_MultipleHandlers_AllInvoked()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            int a = 0;
            int b = 0;

            root.Write("score", 0);
            using var subA = root.Subscribe("score", (in StorageEventArgs _) => a++);
            using var subB = root.Subscribe("score", (in StorageEventArgs _) => b++);

            root.Write("score", 10);

            Assert.That(a, Is.EqualTo(1));
            Assert.That(b, Is.EqualTo(1));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_String_And_Array_Writes_Notify()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int stringInvoked = 0;
            root.Write("playerName", string.Empty);
            using var stringSub = root.Subscribe("playerName", (in StorageEventArgs args) =>
            {
                stringInvoked++;
                Assert.That(args.Target.ReadString(), Is.EqualTo("Hero"));
            });

            var stats = root.GetObject("stats");
            stats.WriteArray("speeds", Array.Empty<float>());
            int arrayInvoked = 0;
            using var arraySub = stats.Subscribe("speeds", (in StorageEventArgs args) =>
            {
                arrayInvoked++;
                CollectionAssert.AreEqual(new[] { 1.0f, 2.5f }, args.Target.ReadArray<float>());
            });

            root.Write("playerName", "Hero");
            stats.WriteArray("speeds", new[] { 1.0f, 2.5f });

            Assert.That(stringInvoked, Is.EqualTo(1));
            Assert.That(arrayInvoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_Only_Target_Fires()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);
            root.Write("hp", 0);

            int scoreInvoked = 0;
            int hpInvoked = 0;

            using var scoreSub = root.Subscribe("score", (in StorageEventArgs _) => scoreInvoked++);
            using var hpSub = root.Subscribe("hp", (in StorageEventArgs _) => hpInvoked++);

            root.Write("score", 10);
            root.Write("hp", 5);

            Assert.That(scoreInvoked, Is.EqualTo(1));
            Assert.That(hpInvoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_Custom_Separator()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var stats = root.GetObject("player").GetObject("stats");
            stats.Write("hp", 0);
            int invoked = 0;
            using var sub = root.Subscribe("player/stats/hp", (in StorageEventArgs _) => invoked++, '/');

            stats.Write("hp", 9);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_Missing_Intermediate_Throws()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.GetObject("player"); // but no stats

            Assert.Throws<ArgumentException>(() =>
                root.Subscribe("player.stats.hp", (in StorageEventArgs _) => { }));
        }

        [Test]
        public void Storage_FieldWrite_Subscription_OnChildObject()
        {
            using var storage = new Storage(_rootLayout);
            var root = storage.Root;
            var child = root.GetObject("child", layout: _leafLayout);
            int invoked = 0;

            using var sub = child.Subscribe("hp", (in StorageEventArgs args) =>
            {
                invoked++;
                Assert.That(args.Target.ID, Is.EqualTo(child.ID));
                Assert.That(args.Target.Read<int>("hp"), Is.EqualTo(55));
            });

            child.Write("hp", 55);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Name_Equals_Child_Subscribe()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.GetObject("entity");
            int viaRoot = 0;
            using var rootSub = root.Subscribe("entity", (in StorageEventArgs _) => viaRoot++);

            var entity = root.GetObject("entity");
            int viaChild = 0;
            using var childSub = entity.Subscribe((in StorageEventArgs _) => viaChild++);

            entity.Write("hp", 10);

            Assert.That(viaRoot, Is.EqualTo(1));
            Assert.That(viaChild, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_EmptyString_Targets_Current_Container()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int invoked = 0;
            using var sub = root.Subscribe("", (in StorageEventArgs _) => invoked++);

            root.Write("hp", 5);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_Equals_Nested_Subscribe()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var persistent = root.GetObject("persistent");
            persistent.GetObject("entity");
            int viaPath = 0;
            using var pathSub = root.Subscribe("persistent.entity", (in StorageEventArgs _) => viaPath++);

            var nested = root.GetObject("persistent");
            int viaNested = 0;
            using var nestedSub = nested.Subscribe("entity", (in StorageEventArgs _) => viaNested++);

            nested.GetObject("entity").Write("hp", 9);

            Assert.That(viaPath, Is.EqualTo(1));
            Assert.That(viaNested, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_Must_Exist()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            Assert.Throws<ArgumentException>(() =>
                root.Subscribe("missing", (in StorageEventArgs _) => { }));

            root.Write("existing", 1);
            int invoked = 0;
            using var sub = root.Subscribe("existing", (in StorageEventArgs _) => invoked++);

            root.Write("existing", 2);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_Navigates_To_Child()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.GetObject("entity").GetObject("child");
            int invoked = 0;
            using var sub = root.Subscribe("entity.child", (in StorageEventArgs _) => invoked++);

            var child = root.GetObject("entity").GetObject("child");
            child.Write("hp", 3);

            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_To_Field()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var stats = root.GetObject("player").GetObject("stats");
            stats.Write("hp", 0);

            int invoked = 0;
            using var sub = root.Subscribe("player.stats.hp", (in StorageEventArgs args) =>
            {
                invoked++;
                Assert.That(args.Path, Is.EqualTo("hp"));
                Assert.That(args.Target.Read<int>("hp"), Is.EqualTo(42));
            });

            stats.Write("hp", 42);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Container_Subscription_Fires_For_All_Fields()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int invoked = 0;
            using var sub = root.Subscribe((in StorageEventArgs args) =>
            {
                invoked++;
                Assert.That(args.Target.ID, Is.EqualTo(root.ID));
            });

            root.Write("a", 1);
            root.Write("b", 2);

            Assert.That(invoked, Is.EqualTo(2));
        }

        [Test]
        public void Storage_Container_Subscription_Dispose_Stops_Notifications()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int invoked = 0;
            var sub = root.Subscribe((in StorageEventArgs _) => invoked++);

            root.Write("hp", 10);
            Assert.That(invoked, Is.EqualTo(1));

            sub.Dispose();
            root.Write("hp", 11);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_WritePath_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);

            int invoked = 0;
            using var sub = root.Subscribe("score", (in StorageEventArgs _) => invoked++);

            root.WritePath("score", 42);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_FromChildWritePath_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var player = root.GetObject("player");
            player.Write("hp", 0);

            int invoked = 0;
            using var sub = player.Subscribe("hp", (in StorageEventArgs _) => invoked++);

            root.WritePath("player.hp", 9);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_RepeatedWritesNotifyEachTime()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);

            int invoked = 0;
            using var sub = root.Subscribe("score", (in StorageEventArgs _) => invoked++);

            root.Write("score", 1);
            root.Write("score", 2);

            Assert.That(invoked, Is.EqualTo(2));
        }

        [Test]
        public void Storage_Subscribe_Field_DisposeOneHandlerLeavesOthers()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);

            int first = 0;
            int second = 0;
            var subA = root.Subscribe("score", (in StorageEventArgs _) => first++);
            using var subB = root.Subscribe("score", (in StorageEventArgs _) => second++);

            subA.Dispose();
            root.Write("score", 7);

            Assert.That(first, Is.EqualTo(0));
            Assert.That(second, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_DisposeInsideHandlerStopsFuture()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);

            int invoked = 0;
            StorageSubscription subscription = default;
            subscription = root.Subscribe("score", (in StorageEventArgs _) =>
            {
                invoked++;
                subscription.Dispose();
            });

            root.Write("score", 5);
            root.Write("score", 6);

            Assert.That(invoked, Is.EqualTo(1));
            subscription.Dispose();
        }

        [Test]
        public void Storage_Subscribe_Field_WriteArrayPath_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var stats = root.GetObject("stats");
            stats.WriteArray("speeds", Array.Empty<float>());

            int invoked = 0;
            using var sub = root.Subscribe("stats.speeds", (in StorageEventArgs args) =>
            {
                invoked++;
                CollectionAssert.AreEqual(new[] { 3f, 4f }, args.Target.ReadArray<float>());
            });

            root.WriteArrayPath("stats.speeds", new[] { 3f, 4f });
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_WriteStringPath_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var dialog = root.GetObject("dialog");
            dialog.Write("line", string.Empty);

            int invoked = 0;
            using var sub = root.Subscribe("dialog.line", (in StorageEventArgs args) =>
            {
                invoked++;
                Assert.That(args.Target.ReadString(), Is.EqualTo("Hello"));
            });

            root.WritePath("dialog.line", "Hello");
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_OverrideScalar_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);

            int invoked = 0;
            using var sub = root.Subscribe("score", (in StorageEventArgs _) => invoked++);

            root.Override("score", 123);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_OverrideArray_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var stats = root.GetObject("stats");
            stats.Override("speeds", MemoryMarshal.AsBytes(new ReadOnlySpan<float>(new float[] { 3f, 3f })), ValueType.Float32, 2);

            int invoked = 0;
            using var sub = stats.Subscribe("speeds", (in StorageEventArgs args) =>
            {
                invoked++;
                CollectionAssert.AreEqual(new[] { 5f, 6f }, args.Target.ReadArray<float>());
            });

            var newSpeeds = new float[] { 5f, 6f };
            stats.Override("speeds", MemoryMarshal.AsBytes(new ReadOnlySpan<float>(newSpeeds)), ValueType.Float32, newSpeeds.Length);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_Delete_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("hp", 10);

            int invoked = 0;
            using var sub = root.Subscribe("hp", (in StorageEventArgs _) => invoked++);

            Assert.That(root.Delete("hp"), Is.True);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Container_DeleteChild_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var player = root.GetObject("player");
            player.Write("hp", 1);

            int invoked = 0;
            using var sub = player.Subscribe((in StorageEventArgs _) => invoked++);

            Assert.That(player.Delete("hp"), Is.True);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Delete_Multiple_Notifies_All()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("hp", 1);
            root.Write("mp", 2);

            int hp = 0;
            int mp = 0;
            using var hpSub = root.Subscribe("hp", (in StorageEventArgs _) => hp++);
            using var mpSub = root.Subscribe("mp", (in StorageEventArgs _) => mp++);

            Assert.That(root.Delete("hp", "mp"), Is.EqualTo(2));
            Assert.That(hp, Is.EqualTo(1));
            Assert.That(mp, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Container_DeleteMissing_NoNotify()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int invoked = 0;
            using var sub = root.Subscribe((in StorageEventArgs _) => invoked++);

            Assert.That(root.Delete("missing"), Is.False);
            Assert.That(invoked, Is.EqualTo(0));
        }

        [Test]
        public void Storage_Subscribe_Field_DeleteThenRewrite_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);

            int invoked = 0;
            using var sub = root.Subscribe("score", (in StorageEventArgs _) => invoked++);
            root.Delete("score");

            root.Write("score", 55);
            int invoked2 = 0;
            using var sub2 = root.Subscribe("score", (in StorageEventArgs _) => invoked2++);
            root.Write("score", 55);
            root.Delete("score");

            Assert.That(invoked, Is.EqualTo(1));
            Assert.That(invoked2, Is.EqualTo(2));
        }

        [Test]
        public void Storage_Subscribe_Field_DeleteAndFailRewrite_NoNotification()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);

            int invoked = 0;
            using var sub = root.Subscribe("score", (in StorageEventArgs _) => invoked++);
            root.Delete("score");

            // rewrite attempt without resubscribe should not notify old handler
            root.Write("score", 10);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_DeleteResubscribeThenDelete_NotifiesNewOnly()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("flag", 1);

            int first = 0;
            using var sub = root.Subscribe("flag", (in StorageEventArgs _) => first++);
            root.Delete("flag");
            Assert.That(first, Is.EqualTo(1));

            root.Write("flag", 2);
            int second = 0;
            using var sub2 = root.Subscribe("flag", (in StorageEventArgs _) => second++);
            root.Delete("flag");

            Assert.That(first, Is.EqualTo(1), "Original subscription should remain at 1");
            Assert.That(second, Is.EqualTo(1), "New subscription should see delete");
        }

        [Test]
        public void Storage_Subscribe_Field_DeleteResubscribeThenWrite_NotifiesNewOnly()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("hp", 100);

            int first = 0;
            using var sub = root.Subscribe("hp", (in StorageEventArgs _) => first++);
            root.Delete("hp");

            root.Write("hp", 50);
            int second = 0;
            using var sub2 = root.Subscribe("hp", (in StorageEventArgs _) => second++);

            root.Write("hp", 25);
            Assert.That(first, Is.EqualTo(1));
            Assert.That(second, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_RecreateAfterDeleteRequiresResubscribe()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("mp", 5);

            int invoked = 0;
            using var sub = root.Subscribe("mp", (in StorageEventArgs _) => invoked++);
            root.Delete("mp");

            root.Write("mp", 20);
            Assert.That(invoked, Is.EqualTo(1));

            using var sub2 = root.Subscribe("mp", (in StorageEventArgs _) => invoked++);
            root.Write("mp", 30);
            Assert.That(invoked, Is.EqualTo(2));
        }

        [Test]
        public void Storage_Subscribe_Field_DeleteMultipleResubscribeEach()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("a", 1);
            root.Write("b", 2);

            int aCount = 0;
            using var aSub = root.Subscribe("a", (in StorageEventArgs _) => aCount++);
            int bCount = 0;
            using var bSub = root.Subscribe("b", (in StorageEventArgs _) => bCount++);

            root.Delete("a", "b");
            Assert.That(aCount, Is.EqualTo(1));
            Assert.That(bCount, Is.EqualTo(1));

            root.Write("a", 10);
            root.Write("b", 20);

            using var aSub2 = root.Subscribe("a", (in StorageEventArgs _) => aCount++);
            using var bSub2 = root.Subscribe("b", (in StorageEventArgs _) => bCount++);
            root.Write("a", 30);
            root.Write("b", 40);

            Assert.That(aCount, Is.EqualTo(2));
            Assert.That(bCount, Is.EqualTo(2));
        }

        [Test]
        public void Storage_Subscribe_Field_DeleteWriteInterleaved_NoStaleNotifications()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);

            int deleteCount = 0;
            using var sub = root.Subscribe("score", (in StorageEventArgs args) =>
            {
                if (args.Event == StorageEvent.Delete)
                    deleteCount++;
            });

            root.Delete("score"); // delete event
            root.Write("score", 10); // no notification because field removed

            Assert.That(deleteCount, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_ResubscribeAfterDeleteGetsNewWrites()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("score", 0);

            using (root.Subscribe("score", (in StorageEventArgs _) => { }))
                root.Delete("score");

            root.Write("score", 10);

            int invoked = 0;
            using var sub2 = root.Subscribe("score", (in StorageEventArgs _) => invoked++);
            root.Write("score", 20);

            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_WriteDuringDelete_NoNotificationAfterward()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("value", 1);

            int deleteNotified = 0;
            using var sub = root.Subscribe("value", (in StorageEventArgs args) =>
            {
                if (args.Event == StorageEvent.Delete)
                    deleteNotified++;
#if UNITY_EDITOR
                UnityEngine.Debug.Log(args);
#endif
            });

            root.Delete("value");
            root.Write("value", 2);

            Assert.That(deleteNotified, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_DeleteWriteDelete_WriteRequiresResubscribe()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("hp", 1);

            using (root.Subscribe("hp", (in StorageEventArgs _) => { }))
                root.Delete("hp");

            root.Write("hp", 2);
            using var sub2 = root.Subscribe("hp", (in StorageEventArgs _) => { });
            root.Delete("hp");

            int writeCount = 0;
            root.Write("hp", 3);
            using var sub3 = root.Subscribe("hp", (in StorageEventArgs _) => writeCount++);
            root.Write("hp", 4);

            Assert.That(writeCount, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_MultipleDeleteWriteSequences()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("flag", 1);

            int deleteCount = 0;
            using var sub = root.Subscribe("flag", (in StorageEventArgs args) =>
            {
                if (args.Event == StorageEvent.Delete)
                    deleteCount++;
            });

            root.Delete("flag");
            root.Write("flag", 2);
            root.Delete("flag");
            root.Write("flag", 3);

            Assert.That(deleteCount, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_ReschemeWrite_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write<byte>("value", 1);

            int invoked = 0;
            using var sub = root.Subscribe("value", (in StorageEventArgs _) => invoked++);

            root.Write("value", 1234567890123L);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_TryWriteSuccess_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write<int>("value", 1);

            int invoked = 0;
            using var sub = root.Subscribe("value", (in StorageEventArgs _) => invoked++);

            Assert.That(root.TryWrite("value", 5), Is.True);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Field_TryWriteImplicitConversion_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write<double>("value", 1d);

            int invoked = 0;
            using var sub = root.Subscribe("value", (in StorageEventArgs _) => invoked++);

            Assert.That(root.TryWrite("value", 2f), Is.True);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_FieldAfterContainersExist_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var stats = root.GetObject("player").GetObject("stats");
            stats.Write("mana", 0);

            int invoked = 0;
            using var sub = root.Subscribe("player.stats.mana", (in StorageEventArgs _) => invoked++);

            stats.Write("mana", 5);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Path_And_Direct_BothFire()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var stats = root.GetObject("player").GetObject("stats");
            stats.Write("hp", 0);

            int viaPath = 0;
            int viaDirect = 0;

            using var pathSub = root.Subscribe("player.stats.hp", (in StorageEventArgs _) => viaPath++);
            using var directSub = stats.Subscribe("hp", (in StorageEventArgs _) => viaDirect++);

            stats.Write("hp", 20);
            Assert.That(viaPath, Is.EqualTo(1));
            Assert.That(viaDirect, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Container_OnlyChildWritesNotify()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var player = root.GetObject("player");
            player.Write("hp", 0);
            root.Write("score", 0);

            int invoked = 0;
            using var sub = player.Subscribe((in StorageEventArgs _) => invoked++);

            root.Write("score", 1);
            Assert.That(invoked, Is.EqualTo(0));

            player.Write("hp", 2);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Container_MultipleHandlers_AllFire()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var player = root.GetObject("player");
            player.Write("hp", 0);

            int a = 0;
            int b = 0;
            using var subA = player.Subscribe((in StorageEventArgs _) => a++);
            using var subB = player.Subscribe((in StorageEventArgs _) => b++);

            player.Write("hp", 3);
            Assert.That(a, Is.EqualTo(1));
            Assert.That(b, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Container_FieldAndContainerTogether()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var player = root.GetObject("player");
            player.Write("hp", 0);

            int containerCount = 0;
            int fieldCount = 0;
            using var containerSub = player.Subscribe((in StorageEventArgs _) => containerCount++);
            using var fieldSub = player.Subscribe("hp", (in StorageEventArgs _) => fieldCount++);

            player.Write("hp", 4);
            Assert.That(containerCount, Is.EqualTo(1));
            Assert.That(fieldCount, Is.EqualTo(1));
        }

        [Test]
        public void Storage_Subscribe_Container_DisposeInsideHandlerStopsFuture()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var player = root.GetObject("player");
            player.Write("hp", 0);

            int invoked = 0;
            StorageSubscription subscription = default;
            subscription = player.Subscribe((in StorageEventArgs _) =>
            {
                invoked++;
                subscription.Dispose();
            });

            player.Write("hp", 6);
            player.Write("hp", 7);

            Assert.That(invoked, Is.EqualTo(1));
            subscription.Dispose();
        }

        [Test]
        public void Storage_Subscribe_Container_PathMatchesDirect()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var persistent = root.GetObject("persistent");
            persistent.GetObject("entity").Write("hp", 0);

            int viaPath = 0;
            int viaChild = 0;

            using var pathSub = root.Subscribe("persistent.entity", (in StorageEventArgs _) => viaPath++);
            var entity = persistent.GetObject("entity");
            using var childSub = entity.Subscribe((in StorageEventArgs _) => viaChild++);

            entity.Write("hp", 9);
            Assert.That(viaPath, Is.EqualTo(1));
            Assert.That(viaChild, Is.EqualTo(1));
        }

        [Test]
        public void Storage_DeleteParent_NotifiesDescendantsAndFields()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var grandParent = root.GetObject("grandParent");

            var parent = grandParent.GetObject("parent");
            parent.Write("meta", 1);

            var child = parent.GetObject("child");
            child.Write("stat", 5);

            var grandChild = child.GetObject("grand");
            grandChild.Write("hp", 25);

            bool grandParentKnowsChildDeleted = false;
            bool parentDeleted = false;
            bool childDeleted = false;
            bool grandChildDeleted = false;
            bool childFieldDeleted = false;
            bool grandFieldDeleted = false;

            int grandParentSubCount = 0;
            int parentSubCount = 0;
            int childSubCount = 0;
            int grandChildSubCount = 0;
            int childFieldSubCount = 0;
            int grandFieldSubCount = 0;

            using var grandParentSub = grandParent.Subscribe((in StorageEventArgs args) =>
            {
                grandParentSubCount++;
                Assert.That(args.Event == StorageEvent.Delete, "parent should receive delete event");
                Assert.That(args.Target == grandParent, "grand parent should not be destroyed");
                Assert.That(!args.Target.IsNull, "grand parent should not be destroyed");
                Assert.That(args.Path, Is.EqualTo("parent"), "grandParentSub should be parent");
                grandParentKnowsChildDeleted = true;
            });

            using var parentSub = parent.Subscribe((in StorageEventArgs args) =>
            {
                parentSubCount++;
                Assert.That(args.Event == StorageEvent.Dispose, "parent should receive dispose event");
                Assert.That(args.Target == parent, "parent should receive right target");
                Assert.That(args.Target.IsNull, "parent should be destroyed");
                Assert.That(string.IsNullOrEmpty(args.Path), "parent should recieve empty string");
                parentDeleted = true;

            });

            using var childSub = child.Subscribe((in StorageEventArgs args) =>
            {
                Assert.That(args.Event == StorageEvent.Dispose, "childSub should recieve dispose event");
                childSubCount++;
                if (args.Target.IsNull)
                {
                    if (args.Path == string.Empty)
                        childDeleted = true;
                }
            });

            using var grandChildSub = grandChild.Subscribe((in StorageEventArgs args) =>
            {
                Assert.That(args.Event == StorageEvent.Dispose, "grandChildSub should recieve dispose event");
                grandChildSubCount++;
                if (args.Target.IsNull)
                {
                    Assert.That(string.IsNullOrEmpty(args.Path), "grandChildSub should be empty");
                    grandChildDeleted = true;
                }
            });

            using var childFieldSub = child.Subscribe("stat", (in StorageEventArgs args) =>
            {
                Assert.That(args.Event == StorageEvent.Dispose, "childFieldSub should recieve dispose event");
                childFieldSubCount++;
                if (args.Target.IsNull)
                {
                    Assert.That(string.IsNullOrEmpty(args.Path), "childFieldSub should be stat");
                    childFieldDeleted = true;
                }
            });

            using var grandFieldSub = grandChild.Subscribe("hp", (in StorageEventArgs args) =>
            {
                Assert.That(args.Event == StorageEvent.Dispose, "grandFieldSub should recieve dispose event");
                grandFieldSubCount++;
                if (args.Target.IsNull)
                {
                    Assert.That(string.IsNullOrEmpty(args.Path), "grandFieldSub should be hp");
                    grandFieldDeleted = true;
                }
            });

            Assert.That(grandParent.Delete("parent"), Is.True, "Expected parent container to be removed.");

            Assert.That(grandParentSubCount, Is.EqualTo(1), "Grand parent should be notified of parent deletion.");
            Assert.That(parentSubCount, Is.EqualTo(1), "Parent should be notified of child deletion.");
            Assert.That(childSubCount, Is.EqualTo(1), "Child should be notified of grand child deletion.");
            Assert.That(grandChildSubCount, Is.EqualTo(1), "Grand child should be notified of grand field deletion.");
            Assert.That(childFieldSubCount, Is.EqualTo(1), "Child field should be notified of child field deletion.");
            Assert.That(grandFieldSubCount, Is.EqualTo(1), "Grand field should be notified of grand field deletion.");

            Assert.That(grandParentKnowsChildDeleted, Is.True, "Grand parent should know parent is deleted.");
            Assert.That(parentDeleted, Is.True, "Parent container should receive deletion callback.");
            Assert.That(childDeleted, Is.True, "Child container should receive deletion callback.");
            Assert.That(grandChildDeleted, Is.True, "Grand-child container should receive deletion callback.");
            Assert.That(childFieldDeleted, Is.True, "Field subscribers within deleted child should receive deletion callback.");
            Assert.That(grandFieldDeleted, Is.True, "Field subscribers within deeper descendants should receive deletion callback.");
        }

        [Test]
        public void Storage_ChildWritesBubbleToAllAncestors()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var level1 = root.GetObject("level1");
            var level2 = level1.GetObject("level2");
            level2.Write("stat", 0);

            int rootCount = 0;
            int level1Count = 0;
            int level2ContainerCount = 0;
            int level2FieldCount = 0;

            using var rootSub = root.Subscribe((in StorageEventArgs args) =>
            {
                if (!args.Target.IsNull)
                {
                    rootCount++;
                    Assert.That(args.Target.ID, Is.EqualTo(root.ID));
                    Assert.That(args.Path, Is.EqualTo("level1.level2.stat"));
                }
            });

            using var level1Sub = level1.Subscribe((in StorageEventArgs args) =>
            {
                if (!args.Target.IsNull)
                {
                    level1Count++;
                    Assert.That(args.Target.ID, Is.EqualTo(level1.ID));
                    Assert.That(args.Path, Is.EqualTo("level2.stat"));
                }
            });

            using var level2ContainerSub = level2.Subscribe((in StorageEventArgs args) =>
            {
                if (!args.Target.IsNull)
                {
                    level2ContainerCount++;
                    Assert.That(args.Target.ID, Is.EqualTo(level2.ID));
                    Assert.That(args.Path, Is.EqualTo("stat"));
                }
            });

            using var level2FieldSub = level2.Subscribe("stat", (in StorageEventArgs args) =>
            {
                if (!args.Target.IsNull)
                {
                    level2FieldCount++;
                    Assert.That(args.Target.ID, Is.EqualTo(level2.ID));
                    Assert.That(args.Path, Is.EqualTo("stat"));
                }
            });

            level2.Write("stat", 1);
            level2.Write("stat", 2);

            Assert.That(level2FieldCount, Is.EqualTo(2), "Field subscribers on the child should fire for each write.");
            Assert.That(level2ContainerCount, Is.EqualTo(2), "Container subscribers on the child should fire for each write.");
            Assert.That(level1Count, Is.EqualTo(2), "Parent containers should observe their child writes.");
            Assert.That(rootCount, Is.EqualTo(2), "Root container should observe descendant writes.");
        }

        [Test]
        public void Write_ScalarField_TriggersEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            bool notified = false;
            root.Write("hp", 0);

            using var sub = root.Subscribe("hp", (in StorageEventArgs args) =>
            {
                notified = true;
                Assert.That(args.Path, Is.EqualTo("hp"));
                Assert.That(args.Target.Read<int>("hp"), Is.EqualTo(100));
            });

            root.Write("hp", 100);
            Assert.That(notified, Is.True);
        }

        [Test]
        public void Write_DeeplyNestedField_BubblesEventsToAllAncestors()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Setup A.B.C
            var a = root.GetObject("A");
            var b = a.GetObject("B");
            var c = b.GetObject("C");

            int rootCount = 0;
            int aCount = 0;
            int bCount = 0;
            int cCount = 0;

            using var subRoot = root.Subscribe((in StorageEventArgs args) =>
            {
                if (!args.Target.IsNull) { rootCount++; Assert.That(args.Path, Is.EqualTo("A.B.C.val")); }
            });
            using var subA = a.Subscribe((in StorageEventArgs args) =>
            {
                if (!args.Target.IsNull) { aCount++; Assert.That(args.Path, Is.EqualTo("B.C.val")); }
            });
            using var subB = b.Subscribe((in StorageEventArgs args) =>
            {
                if (!args.Target.IsNull) { bCount++; Assert.That(args.Path, Is.EqualTo("C.val")); }
            });
            using var subC = c.Subscribe((in StorageEventArgs args) =>
            {
                if (!args.Target.IsNull) { cCount++; Assert.That(args.Path, Is.EqualTo("val")); }
            });

            c.Write("val", 42);

            Assert.That(cCount, Is.EqualTo(1));
            Assert.That(bCount, Is.EqualTo(1));
            Assert.That(aCount, Is.EqualTo(1));
            Assert.That(rootCount, Is.EqualTo(1));
        }

        [Test]
        public void Delete_LeafNode_NotifiesParent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var child = root.GetObject("child");

            bool parentNotified = false;
            using var sub = root.Subscribe((in StorageEventArgs args) =>
            {
                // Root should see "child" deleted
                if (args.Target == root && args.Event == StorageEvent.Delete && args.Path == "child")
                    parentNotified = true;
            });

            root.Delete("child");
            Assert.That(parentNotified, Is.True);
        }

        [Test]
        public void Delete_MiddleNode_NotifiesParent_And_Descendants()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var parent = root.GetObject("parent");
            var child = parent.GetObject("child");

            bool rootNotified = false;
            bool parentSelfNotified = false;
            bool childSelfNotified = false;

            using var subRoot = root.Subscribe((in StorageEventArgs args) =>
            {
                if (args.Target == root && args.Path == "parent") rootNotified = true;
            });

            using var subParent = parent.Subscribe((in StorageEventArgs args) =>
            {
                if (args.Target == parent && args.Path == string.Empty) parentSelfNotified = true;
            });

            using var subChild = child.Subscribe((in StorageEventArgs args) =>
            {
                if (args.Target == child && args.Path == string.Empty) childSelfNotified = true;
            });

            root.Delete("parent");

            Assert.That(rootNotified, Is.True, "Root missed notification");
            Assert.That(parentSelfNotified, Is.True, "Parent missed self-destruct notification");
            Assert.That(childSelfNotified, Is.True, "Child missed self-destruct notification");
        }

        [Test]
        public void Write_OnDeletedObject_ThrowsObjectDisposed()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var child = root.GetObject("child");
            var childId = child.ID;

            root.Delete("child");

            // Attempt to write to the stale view
            Assert.Throws<ObjectDisposedException>(() => child.Write("val", 1));
        }

        [Test]
        public void Write_Path_TriggersIntermediateEvents()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            int eventCount = 0;
            using var sub = root.Subscribe((in StorageEventArgs args) =>
            {
                if (!args.Target.IsNull) eventCount++;
            });

            root.WritePath("A.B.C", 1);

            Assert.That(eventCount, Is.GreaterThan(0));
        }

        [Test]
        public void Delete_Root_NotifiesDescendants()
        {
            var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var child = root.GetObject("child");

            bool childNotified = false;
            using var sub = child.Subscribe((in StorageEventArgs args) =>
            {
                if (args.Target.IsNull && args.Path == string.Empty) childNotified = true;
            });

            storage.Dispose(); // Deletes root

            Assert.That(childNotified, Is.True);
        }

        [Test]
        public void Write_Array_TriggersEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            bool notified = false;
            root.WriteArray("arr", new[] { 0, 0, 0 });
            int eventCount = 0;

            using var sub = root.Subscribe((in StorageEventArgs args) =>
            {
                // print the field name
                // Assert.That(args.FieldName, Is.EqualTo("arr"), "Field name should be 'arr'");

                // if (!args.Target.IsNull && args.FieldName == "arr")
                // {
                // }
                eventCount++;
                notified = true;
                Assert.That(args.Target.ReadArray<int>("arr").Length, Is.EqualTo(3));
            });

            root.WriteArray("arr", new[] { 1, 2, 3 });
            Assert.That(eventCount, Is.EqualTo(1));
            Assert.That(notified, Is.True);
        }

        [Test]
        public void Write_String_TriggersEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            bool notified = false;
            root.Write("str", "hi");

            int eventCount = 0;

            using var sub = root.Subscribe((in StorageEventArgs args) =>
            {
                eventCount++;
                notified = true;
                Assert.That(args.Target.ReadString("str"), Is.EqualTo("hello"));
            });

            root.Write("str", "hello");
            Assert.That(eventCount, Is.EqualTo(1));
            Assert.That(notified, Is.True);
        }

        [Test]
        public void Sibling_Writes_DoNotCrossTalk()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var sib1 = root.GetObject("sib1");
            var sib2 = root.GetObject("sib2");

            bool sib1Heard = false;
            using var sub1 = sib1.Subscribe((in StorageEventArgs args) => sib1Heard = true);

            sib2.Write("val", 1);

            Assert.That(sib1Heard, Is.False);
        }

        [Test]
        public void Rescheme_Field_TriggersEvent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write<int>("val", 1);

            bool notified = false;
            using var sub = root.Subscribe("val", (in StorageEventArgs args) =>
            {
                notified = true;
                Assert.That(args.FieldType, Is.EqualTo(TypeUtil<float>.ScalarFieldType));
            });

            // Change type int -> float
            root.Write<float>("val", 1.0f);
            Assert.That(notified, Is.True);
        }

        [Test]
        public void Delete_MultipleFields_TriggersMultipleEvents()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("a", 1);
            root.Write("b", 2);

            int events = 0;
            using var sub = root.Subscribe((in StorageEventArgs args) =>
            {
                events++;
                Assert.That(args.Target, Is.EqualTo(root));
                Assert.That(args.Path, Is.EqualTo("a").Or.EqualTo("b"));
            });

            root.Delete("a", "b");
            Assert.That(events, Is.EqualTo(2));
        }

        [Test]
        public void Bubbling_Stops_At_Detached_Subtree()
        {
            // Root -> A -> B
            // Delete A.
            // Root should get notification for A.
            // Root should NOT get notification for B (child of A).

            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var a = root.GetObject("A");
            var b = a.GetObject("B");

            var rootEvents = new System.Collections.Generic.List<string>();
            using var sub = root.Subscribe((in StorageEventArgs args) =>
            {
                rootEvents.Add(args.Path);
            });

            root.Delete("A");

            // Root should see "A" deletion.
            // Should NOT see "A.B" deletion because bubbling stops at A (which is deleted).
            Assert.That(rootEvents, Contains.Item("A"));
            Assert.That(rootEvents, Does.Not.Contain("A.B"));
        }

        [Test]
        public void Subscription_Filter_Respects_FieldNames()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            bool aNotified = false;
            bool bNotified = false;
            root.Write("a", 0);
            root.Write("b", 0);

            using var subA = root.Subscribe("a", (in StorageEventArgs _) => aNotified = true);
            using var subB = root.Subscribe("b", (in StorageEventArgs _) => bNotified = true);

            root.Write("a", 1);
            Assert.That(aNotified, Is.True);
            Assert.That(bNotified, Is.False);

            aNotified = false;
            root.Write("b", 1);
            Assert.That(aNotified, Is.False);
            Assert.That(bNotified, Is.True);
        }

        [Test]
        public void Subscription_On_New_Object_Works_Immediately()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            var child = root.GetObject("child");
            child.Write("val", 0);

            bool notified = false;
            using var sub = child.Subscribe("val", (in StorageEventArgs _) => notified = true);

            child.Write("val", 1);
            Assert.That(notified, Is.True);
        }

        #endregion
    }
}