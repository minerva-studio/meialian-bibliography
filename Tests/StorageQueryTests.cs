using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageQueryTests
    {
        [SetUp]
        public void SetUp()
        {
            // Nothing special: use empty layout (dynamic schema)
        }

        [Test]
        public void Query_Write_And_Read_Scalar()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query()
                        .Location("player")
                        .Location("stats")
                        .Location("hp");
            q.Ensure().Scalar(123);

            Assert.That(root.ReadPath<int>("player.stats.hp"), Is.EqualTo(123));
            Assert.That(q.Read<int>(), Is.EqualTo(123));
        }

        [Test]
        public void Query_Ensure_Scalar_Creates_Default()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var value = root.Query()
                            .Location("game")
                            .Location("session")
                            .Location("tick")
                            .Ensure()
                            .Is().Scalar<int>(); // should create with default 0

            Assert.That(value, Is.EqualTo(0));
            Assert.That(root.ReadPath<int>("game.session.tick"), Is.EqualTo(0));
        }

        [Test]
        public void Query_Ensure_ObjectArray_Index_Write_Read()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Ensure object array with min length
            var arr = root.Query()
                          .Location("world")
                          .Location("entities")
                          .Make()
                          .ObjectArray(3);
            Assert.That(arr.Length, Is.GreaterThanOrEqualTo(3));

            // Write hp to index 1 entity
            root.Query()
                .Location("world")
                .Location("entities")
                .Index(1)
                .Location("hp")
                .Write(77);

            int hp = root.Query()
                         .Location("world")
                         .Location("entities")
                         .Index(1)
                         .Location("hp")
                         .Read<int>();

            Assert.That(hp, Is.EqualTo(77));
        }

        [Test]
        public void Query_Exist_On_Missing_Field_ReturnsFalse()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var exists = root.Query()
                             .Location("player")
                             .Location("stats")
                             .Location("mana")
                             .Exist()
                             .Has;

            Assert.That(exists, Is.False);
        }

        [Test]
        public void Query_Expect_Object_Succeeds()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.GetObject("player").GetObject("stats");

            var q = root.Query()
                        .Location("player").Expect().Object()
                        .Location("stats").Expect().Object();

            Assert.That(!q.Result.Success, Is.False);
        }

        [Test]
        public void Query_Expect_ObjectArray_Fails_On_Scalar()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("scores", 5); // scalar, not object array

            var q = root.Query()
                        .Location("scores")
                        .Expect().ObjectArray(); // should fail

            Assert.That(!q.Result.Success, Is.True);
            Assert.That(q.Result.ErrorMessage, Is.Not.Null.And.Contains("not object array"));
        }

        [Test]
        public void Query_Index_ObjectElement_Expect_Succeeds()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Prepare object array: world.entities[2]
            root.Query()
                .Location("world")
                .Location("entities")
                .Make()
                .ObjectArray(3);

            // Add a field to entity[2]
            root.Query()
                .Location("world")
                .Location("entities")
                .Index(2)
                .Location("hp")
                .Write(10);

            var q = root.Query()
                        .Location("world").Expect().Object()
                        .Location("entities").Expect().ObjectArray()
                        .Index(2).Expect().ObjectElement()
                        .Location("hp").Expect().Scalar<int>();

            Assert.That(q.Result.Success, Is.True);
            Assert.That(q.Result.Value, Is.EqualTo(10));
        }

        [Test]
        public void Query_Index_ObjectElement_Expect_Fails_On_OutOfRange_Or_Null()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // world.entities with length 2, index 5 should fail
            root.Query()
                .Location("world")
                .Location("entities")
                .Make()
                .ObjectArray(2);

            var q1 = root.Query()
                         .Location("world")
                         .Location("entities")
                         .Index(5)
                         .Expect().ObjectElement();

            Assert.That(q1.Result.Success, Is.False);
            Assert.That(q1.Result.ErrorMessage, Is.Not.Null.And.Contains("out of range"));

            // index 1 exists but null element should fail
            var q2 = root.Query()
                         .Location("world")
                         .Location("entities")
                         .Index(1)
                         .Expect().ObjectElement();

            Assert.That(q2.Result.Success, Is.False);
            Assert.That(q2.Result.ErrorMessage, Is.Not.Null.And.Contains("is null"));
        }

        [Test]
        public void Query_Expect_Failure_Shorts_Further_Expect()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("value", 1); // scalar

            var q = root.Query()
                        .Location("value").Expect().Object()  // should fail
                        .Location("anything").Expect().Scalar<int>(); // should be ignored

            Assert.That(!q.Result.Success, Is.True);
            Assert.That(q.Result.ErrorMessage, Is.Not.Null.And.Contains("not object"));
        }

        [Test]
        public void Query_Expect_SoftCheck_Does_Not_Record_Failure()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // make scalar at "x"
            root.Write("x", 42);

            // non-strict check should not record failure even if it fails
            var q = root.Query()
                        .Location("x").Expect().Object(strict: false)
                        .Then()
                        .Location("x").Expect().Scalar<int>(); // still succeeds

            Assert.That(q.Result.Success, Is.True);
            Assert.That(q.Result.Value, Is.EqualTo(42));
        }

        [Test]
        public void Query_Expect_String_Succeeds_And_Fails()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Ensure string at path
            root.Query()
                .Location("meta")
                .Location("title")
                .Ensure()
                .Is().String("Hello");

            // success
            var ok = root.Query()
                         .Location("meta")
                         .Location("title")
                         .Expect().String();

#if UNITY_EDITOR
            UnityEngine.Debug.Log(ok.Result);
#endif

            Assert.That(ok.Result.Success, Is.True);

            // failure on non-char16 array
            root.WritePath("meta.count", 3);
            var bad = root.Query()
                          .Location("meta")
                          .Location("count")
                          .Expect().String();

            Assert.That(!bad.Result.Success, Is.True);
            Assert.That(bad.Result.ErrorMessage, Is.Not.Null.And.Contains("not an array"));
        }

        [Test]
        public void Query_Ensure_Override_Type()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // First create scalar
            root.Query()
                .Location("config")
                .Location("mode")
                .Write(5);

            // Override to array
            var arr = root.Location("config")
                          .Location("mode")
                          .Make()
                          .Array<int>(minLength: 4, allowOverride: true);

            Assert.That(arr.Length, Is.GreaterThanOrEqualTo(4));
        }

        [Test]
        public void Query_Ensure_Make_Throws_On_Failed_Query()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("stats", 1); // scalar

            // Force an expectation failure then call Make(), which should throw
            Assert.Throws<System.InvalidOperationException>(() =>
            {
                root.Query()
                    .Location("stats").Expect().ObjectArray() // fails
                    .Make(); // Make() should ThrowIfFailed
            });
        }

        [Test]
        public void Query_ToString_Returns_Full_Path()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query()
                        .Location("a")
                        .Location("b")
                        .Location("c")
                        .Index(2)
                        .Location("value");

            Assert.That(q.Path, Is.EqualTo("a.b.c[2].value"));
            Assert.That(q.ToString(), Is.EqualTo("Query(a.b.c[2].value)"));
        }

        [Test]
        public void Query_TryRead_Scalar_Success()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.WritePath("game.score", 999);

            var q = root.Query()
                        .Location("game")
                        .Location("score");

            Assert.That(q.TryRead<int>(out var v), Is.True);
            Assert.That(v, Is.EqualTo(999));
        }

        [Test]
        public void Query_TryRead_Scalar_Fail()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query()
                        .Location("missing")
                        .Location("hp");

            Assert.That(q.TryRead<int>(out var v), Is.False);
            Assert.That(v, Is.EqualTo(0));
        }

        [Test]
        public void Query_Subscribe_Field_Notifies()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("hp", 0);
            int invoked = 0;

            using var sub = root.Query()
                                .Location("hp")
                                .Subscribe((in StorageEventArgs args) =>
                                {
                                    invoked++;
                                    Assert.That(args.Path, Is.EqualTo("hp"));
                                });

            root.Write("hp", 55);
            Assert.That(invoked, Is.EqualTo(1));
        }

        [Test]
        public void Query_Exist_As_Type_Checks()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WritePath("player.stats.hp", 10);

            var exist = root.Query()
                            .Location("player")
                            .Location("stats")
                            .Location("hp")
                            .Exist();

            Assert.That(exist.Has, Is.True);
            Assert.That(exist.As<int>(exact: true), Is.True);
            Assert.That(exist.As<float>(exact: false), Is.True); // implicit conversion
            Assert.That(exist.As<float>(exact: true), Is.False); // exact mismatch
        }

        [Test]
        public void Query_Array_Access_TryArray_Success()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArrayPath("stats.speeds", new float[] { 1, 2, 3 });

            var q = root.Query()
                        .Location("stats")
                        .Location("speeds");

            Assert.That(q.Exist().ArrayOf<float>(out var arr), Is.True);
            Assert.That(arr.Length, Is.EqualTo(3));
        }

        [Test]
        public void Query_Array_Access_TryArray_Failure()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("stats", 5); // scalar, not array

            var q = root.Query()
                        .Location("stats");

            Assert.That(q.Exist().ArrayOf<float>(out var arr), Is.False);
            Assert.That(arr.IsDisposed, Is.True);
        }

        [Test]
        public void Query_Persist_Returns_Persistent_And_Does_Not_AutoDispose()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query()
                        .Location("player")
                        .Location("name");
            var p = q.Persist();

            // After Persist(), StorageQuery should dispose its temp buffer, Persistent holds copy
            Assert.That(p.IsDisposed, Is.False);
            Assert.That(p.PathSpan.ToString(), Is.EqualTo("player.name"));

            // Use Persistent to write/read without implicit finalization
            p.Ensure().Is().String("Alice");
            Assert.That(root.ReadStringPath("player.name"), Is.EqualTo("Alice"));

            // Explicit dispose
            p.Dispose();
            Assert.That(p.IsDisposed, Is.True);
        }

        [Test]
        public void Query_Previous_Walks_Back_One_Segment()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query()
                        .Location("a")
                        .Location("b")
                        .Location("c");

            q = q.Previous(); // a.b
            Assert.That(q.Path, Is.EqualTo("a.b"));

            q.Write(1); // write to a.b
            Assert.That(root.ReadPath<int>("a.b"), Is.EqualTo(1));
        }

        [Test]
        public void Query_Then_Returns_Query_Back_To_Parent()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var qr = root.Query()
                         .Location("cfg")
                         .Location("ver")
                         .Ensure()
                         .Scalar<int>(42);

            var qParent = qr.Then(); // Previous(): cfg
            Assert.That(qParent.Path, Is.EqualTo("cfg"));

            qParent.Location("name").Ensure().Is().String("v1");
            Assert.That(root.ReadStringPath("cfg.name"), Is.EqualTo("v1"));
        }
    }
}