using NUnit.Framework;
using System;

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
        [Test]
        public void Exist_ReturnsValueAndFinalizes_WhenSuccess_FromEnsureScalar()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // create a QueryResult via Ensure().Scalar(...) which returns QueryResult<StorageQuery,int>
            var qr = root.Query()
                         .Location("player")
                         .Location("stats")
                         .Location("hp")
                         .Ensure()
                         .Scalar<int>(123);

            // Act
            bool ok = qr.Exist(out int value);

            // Assert
            Assert.IsTrue(ok, "Exist should return true for successful QueryResult.");
            Assert.AreEqual(123, value, "Exist should return the value contained in the QueryResult.");
            Assert.IsTrue(qr.Query.IsDisposed, "Query must be finalized (disposed) after Exist is called.");
        }

        [Test]
        public void Exist_ReturnsFalseAndFinalizes_WhenFailed_ConstructedWithFailedResult()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Construct a failing QueryResult directly
            var failingQuery = root.Query().Location("something");
            var failedResult = Result.Failed<int>("some failure");
            var qr = new QueryResult<StorageQuery, int>(failingQuery, failedResult);

            // Act
            bool ok = qr.Exist(out int value);

            // Assert
            Assert.IsFalse(ok, "Exist should return false for failed QueryResult.");
            Assert.AreEqual(0, value, "Out value should be default on failure.");
            Assert.IsTrue(qr.Query.IsDisposed, "Query must be finalized (disposed) after Exist is called even on failure.");
        }

        [Test]
        public void ExistOrThrow_SuppressFalse_Throws_OnFailure()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var failingQuery = root.Query().Location("x");
            var failedResult = Result.Failed<int>("boom");
            var qr = new QueryResult<StorageQuery, int>(failingQuery, failedResult);

            try
            {
                qr.ExistOrThrow(suppress: false);
                Assert.Fail("ExistOrThrow should have thrown an exception on failure when suppress is false.");
            }
            catch (InvalidOperationException) { }
            Assert.IsTrue(qr.Query.IsDisposed, "Query must be finalized (disposed) after ExistOrThrow even when it throws.");
        }

        [Test]
        public void ExistOrThrow_SuppressTrue_ReturnsDefault_AndFinalizes()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var failingQuery = root.Query().Location("x.y");
            var failedResult = Result.Failed<int>("boom");
            var qr = new QueryResult<StorageQuery, int>(failingQuery, failedResult);

            int v = qr.ExistOrThrow(suppress: true);

            Assert.AreEqual(0, v, "When suppressed, ExistOrThrow should return default on failure.");
            Assert.IsTrue(qr.Query.IsDisposed, "Query must be finalized (disposed) after ExistOrThrow even when suppressed.");
        }

        [Test]
        public void Location_Throws_OnEmptySegment()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query();
            Assert.Throws<ArgumentException>(() => q.Location("")); // ReadOnlySpan overload
        }

        [Test]
        public void Index_Throws_WhenNoSegmentOrNegative()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query();
            Assert.Throws<InvalidOperationException>(() => q.Index(1)); // index before any Location

            var q2 = root.Query().Location("a");
            Assert.Throws<ArgumentOutOfRangeException>(() => q2.Index(-5)); // negative index
        }

        [Test]
        public void Persist_Disposes_Original_And_Persistent_Behaviors()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query().Location("player").Location("name");
            var p = q.Persist();

            // original query should have disposed its internal buffer
            Assert.That(q.IsDisposed, Is.True);

            // persistent holds path
            Assert.That(p.IsDisposed, Is.False);
            Assert.That(p.PathSpan.ToString(), Is.EqualTo("player.name"));

            // Persistent.Location empty throws
            Assert.Throws<ArgumentException>(() => p.Location(""));

            p.Dispose();
            Assert.That(p.IsDisposed, Is.True);
        }

        [Test]
        public void Make_Array_Throws_WhenMemberExists_And_NoAllowOverride()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // create scalar at "a"
            root.Write("a", 5);

            // Ensure.Is().Array without allowOverride should throw
            Assert.Throws<InvalidOperationException>(() =>
            {
                root.Query().Location("a").Ensure().Is().Array<int>();
            });
        }

        [Test]
        public void TryGetMember_ReturnsMember_ForExistingField()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.Write("score", 123);

            var q = root.Query().Location("score");
            Assert.IsTrue(q.TryGetMember(out var member));
            // member should exist and read back correct scalar
            Assert.IsTrue(member.Exist);
            Assert.AreEqual(123, member.AsScalar<int>().Value);
        }

        [Test]
        public void Subscribe_Throws_OnNullHandler_And_Throws_WhenRootDisposed()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query().Location("hp");
            Assert.Throws<ArgumentNullException>(() => q.Subscribe(null));

            // dispose storage/root and then try to subscribe -> EnsureRootValid should throw
            storage.Dispose();
            Assert.Throws<ObjectDisposedException>(() =>
            {
                // provide a no-op handler
                q.Subscribe((in StorageEventArgs _) => { });
            });
        }

        [Test]
        public void StorageArray_Query_Extensions_Preserve_Path()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            root.WriteArrayPath("arr", new int[] { 1, 2 });

            var arr = root.GetArray("arr");
            var qFromArray = arr.Query();
            Assert.That(qFromArray.Path, Is.EqualTo("arr").Or.EqualTo(ContainerLayout.ArrayName));

            var qIndex = arr.Query(1);
            // extension builds `${handle.Name.ToString()}[{index}]`
            Assert.That(qIndex.Path, Is.EqualTo("arr[1]").Or.EqualTo(ContainerLayout.ArrayName + "[1]"));
        }

        [Test]
        public void QueryResult_Location_Extension_Chaining_Works()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // create an object at player
            root.GetObject("player").GetObject("stats");
            // Expect.Object returns QueryResult<TQuery, StorageObject>
            var qr = root.Query().Location("player").Expect().Object();
            // extension: Location on QueryResult<TQuery, StorageObject>
            var chained = qr.Location("stats");
            Assert.That(chained.Path, Is.EqualTo("player.stats"));
        }





        // ---------------------------
        // Expect statement tests
        // ---------------------------

        [Test]
        public void Expect_Scalar_Object_Array_and_String_Success()
        {
            // Arrange: create storage and populate values
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Create nested object and values
            root.Query().Location("player").Location("stats").Ensure().Is().Object();
            root.WritePath("player.stats.hp", 100);
            root.WritePath("player.meta.title", "GameTitle");
            root.WriteArrayPath("player.speeds", new float[] { 1f, 2f, 3f });

            // Act & Assert: scalar expectation returns QueryResult with value
            var qrHp = root.Query().Location("player").Location("stats").Location("hp").Expect().Scalar<int>();
            Assert.That(qrHp.Result.Success, Is.True);
            Assert.That(qrHp.Result.Value, Is.EqualTo(100));

            // Act & Assert: string expectation
            var qrTitle = root.Query().Location("player").Location("meta").Location("title").Expect().String();
            Assert.That(qrTitle.Result.Success, Is.True);

            // Act & Assert: value array expectation and out storageArray
            var q = root.Query().Location("player").Location("speeds").Expect();
            Assert.DoesNotThrow(() => q.ValueArray<float>());
            Assert.DoesNotThrow(() => q.ValueArray<float>().Result.ThrowIfFailed());
            Assert.DoesNotThrow(() =>
            {
                q.ValueArray<float>(out StorageArray arr);
                Assert.That(arr.Length, Is.EqualTo(3));
            });
        }

        [Test]
        public void Expect_ObjectArray_ObjectElement_FailurePaths()
        {
            // Arrange
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Create a scalar at "scores" to provoke failure
            root.Write("scores", 5);

            // Act: Expect.ObjectArray should fail on scalar
            var fail = root.Query().Location("scores").Expect().ObjectArray();
            Assert.That(fail.Result.Success, Is.False);
            Assert.That(fail.Result.ErrorMessage, Does.Contain("not object array"));

            // Act: object element expects index; missing index should error
            var qNoIndex = root.Query().Location("scores").Expect();
            Assert.Throws<InvalidOperationException>(() => qNoIndex.ObjectElement().ExistOrThrow());

            // Create an object array with limited length and test out-of-range
            root.Query().Location("world").Location("entities").Make().ObjectArray(1);
            var qOut = root.Query().Location("world").Location("entities").Index(5).Expect().ObjectElement();
            Assert.That(qOut.Result.Success, Is.False);
            Assert.That(qOut.Result.ErrorMessage, Does.Contain("out of range"));
        }

        [Test]
        public void Expect_SoftCheck_DoesNotRecordFailure()
        {
            // Arrange
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            root.Write("x", 42);

            // Act: soft object check should not set Result.Failed
            var q = root.Query()
                        .Location("x").Expect().Object(strict: false)
                        .Then()
                        .Location("x").Expect().Scalar<int>();

            // Assert
            Assert.That(q.Result.Success, Is.True);
            Assert.That(q.Result.Value, Is.EqualTo(42));
        }

        // ---------------------------
        // Ensure statement tests
        // ---------------------------

        [Test]
        public void Ensure_Scalar_And_String_And_Array_CreateAndReturnResults()
        {
            // Arrange
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Act: ensure scalar with explicit value
            var qr = root.Query().Location("cfg").Location("max").Ensure().Scalar<int>(99);
            Assert.That(qr.Result.Success, Is.True);

            // Ensure default create (no value provided)
            Assert.DoesNotThrow(() =>
            {
                int i = root.Query().Location("cfg").Location("min").Ensure().Is().Scalar<int>();
                Assert.That(i, Is.EqualTo(0)); // default int
            });

            // Ensure string creation
            var qrStr = root.Query().Location("meta").Location("title").Ensure().String("Hello");
            Assert.That(qrStr.Result.Success, Is.True);
            Assert.That(root.ReadStringPath("meta.title"), Is.EqualTo("Hello"));

            // Ensure array creation (value array)
            var qra = root.Query().Location("data").Location("values").Ensure().Array<int>(out var arr, minLength: 3);
            Assert.That(arr.Length, Is.GreaterThanOrEqualTo(3));
        }

        [Test]
        public void Ensure_Object_And_ObjectArray_WithAllowOverride()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Create scalar at path then override to object with allowOverride=false -> should throw
            root.WritePath("node", 1);
            Assert.Throws<InvalidOperationException>(() =>
                root.Query().Location("node").Ensure().Is().Object(allowOverride: false));

            // Now allow override -> should succeed
            var obj = root.Query().Location("node").Ensure().Is().Object(allowOverride: true);
            Assert.That(obj.IsNull, Is.False);

            // ObjectArray override
            root.WritePath("list", 1); // scalar
            Assert.Throws<InvalidOperationException>(() =>
                root.Query().Location("list").Ensure().Is().ObjectArray(allowOverride: false));

            var arr = root.Query().Location("list").Ensure().Is().ObjectArray(minLength: 2, allowOverride: true);
            Assert.That(arr.Length, Is.GreaterThanOrEqualTo(2));
        }

        // ---------------------------
        // Make statement tests
        // ---------------------------

        [Test]
        public void Make_Scalar_Writes_And_Reads_And_Finalizes()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Make() finalizes the query; write scalar using Make().Scalar(value)
            root.Query().Location("player").Location("hp").Make().Scalar<int>(77);

            // Read via path
            Assert.That(root.ReadPath<int>("player.hp"), Is.EqualTo(77));

            // Make().Scalar when member exists but incompatible without allowOverride -> throws
            root.WritePath("conflict", 1);
            Assert.Throws<InvalidOperationException>(() =>
                root.Query().Location("conflict").Make().Scalar<double>(allowOverride: false));
        }

        [Test]
        public void Make_ObjectArray_CreateAndManipulateElements()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // Create an object array and ensure length
            var objArr = root.Query().Location("world").Location("ents").Make().ObjectArray(2);
            Assert.That(objArr.Length, Is.GreaterThanOrEqualTo(2));

            // Write to element
            root.WritePath("world.ents[1].hp", 11);
            Assert.That(root.ReadPath<int>("world.ents[1].hp"), Is.EqualTo(11));
        }

        // ---------------------------
        // Exist statement tests
        // ---------------------------

        [Test]
        public void Exist_Has_Object_Scalar_Array_Checks()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            // prepare fields
            root.WritePath("player.stats.hp", 10);
            root.Query().Location("player").Location("inventory").Make().Array<int>(minLength: 2);

            // Has & As checks
            var existHp = root.Query().Location("player").Location("stats").Location("hp").Exist();
            Assert.That(existHp.Has, Is.True);
            Assert.That(existHp.As<int>(exact: true), Is.True);
            Assert.That(existHp.As<float>(exact: false), Is.True);

            // Array checks
            var existArr = root.Query().Location("player").Location("inventory").Exist();
            Assert.That(existArr.ArrayOf<int>(out var arr), Is.True);
            Assert.That(arr.Length, Is.GreaterThanOrEqualTo(2));

            // Object(out) when missing returns false and out default
            Assert.That(root.Query().Location("missing").Exist().Object(out StorageObject so), Is.False);
            Assert.That(so.IsNull, Is.True);
        }

        [Test]
        public void Exist_Finalizer_Disposes_Query()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query().Location("a").Location("b");
            // Exist returns ExistStatement and implicitly finalizes underlying query
            var exist = q.Exist();
            // After exist statement created, original query's buffer should be disposed (finalized)
            Assert.That(q.IsDisposed, Is.True);
            // Using the ExistStatement still works for presence check
            Assert.That(exist.Has, Is.False);
        }

        // ---------------------------
        // Persistent/Previous/Then tests (supporting APIs)
        // ---------------------------

        [Test]
        public void Persist_Previous_Then_Chaining_Behavior()
        {
            using var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;

            var q = root.Query().Location("a").Location("b").Location("c").Index(2).Location("val");
            // Persist creates Persistent that keeps path and finalizes original query
            var p = q.Persist();
            Assert.That(q.IsDisposed, Is.True);
            Assert.That(p.PathSpan.ToString(), Is.EqualTo("a.b.c[2].val"));

            // Previous on persistent should walk back
            var pPrev = p.Previous();
            Assert.That(pPrev.PathSpan.ToString(), Is.EqualTo("a.b.c[2]"));

            // Ensure using Then() on a QueryResult goes back to parent
            var created = root.Query().Location("cfg").Location("ver").Ensure().Scalar<int>(1);
            var parent = created.Then();
            Assert.That(parent.Path, Is.EqualTo("cfg"));

            p.Dispose();
        }
    }
}