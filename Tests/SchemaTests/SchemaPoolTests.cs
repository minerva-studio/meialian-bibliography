// SchemaPoolTests.cs
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Amlos.Container.Tests
{
    [Ignore("Abandoning Schema")]
    [TestFixture]
    public class SchemaPoolTests
    {
        private static Schema_Old MakeSchema(bool canonicalize, params (string name, int len)[] fields)
        {
            var b = new SchemaBuilder(canonicalizeByName: canonicalize);
            foreach (var (n, l) in fields) b.AddFieldFixed(n, l);
            return b.Build();
        }

        [Test]
        public void NewPool_HasEmptyEntry()
        {
            var pool = new SchemaPool();
            // Constructor inserts (EmptyKey -> Schema.Empty)
            Assert.That(pool.Count, Is.EqualTo(1));

            // TryGetExisting on empty should succeed
            Assert.That(pool.TryGetExisting(Array.Empty<FieldDescriptor_Old>(), 0, out var s), Is.True);
            Assert.That(ReferenceEquals(s, Schema_Old.Empty), Is.True);
        }

        [Test]
        public void Intern_EquivalentSchemas_ReturnSameInstance()
        {
            var pool = new SchemaPool();

            // Build two equivalent schemas (canonicalization on to normalize order)
            var s1 = MakeSchema(true, ("hp", 4), ("spd", 4));
            var s2 = SchemaBuilder.FromSchema(s1, canonicalizeByName: true).Build();

            // Intern via schema
            var i1 = pool.Intern(s1);
            // Intern via fields + stride (IReadOnlyList fast path)
            var i2 = pool.Intern(s2.Fields, s2.Stride);

            Assert.That(ReferenceEquals(i1, i2), Is.True);
            // pool already had Empty + this one
            Assert.That(pool.Count, Is.EqualTo(2));
        }

        [Test]
        public void Intern_ListInput_HitNoDuplicateAndStableAfterMissFreeze()
        {
            var pool = new SchemaPool();
            var s = MakeSchema(true, ("a", 2), ("b", 8));

            // Prepare a List<FieldDescriptor> based on s.Fields
            var list = s.Fields.ToList();

            // 1) First call: miss -> pool freezes to array and stores
            var i1 = pool.Intern((IEnumerable<FieldDescriptor_Old>)list, s.Stride);

            // mutate the list AFTER interning to ensure pool key stability
            list.Add(FieldDescriptor_Old.Fixed("tmp", 1)); // this should NOT affect stored mapping

            // 2) Second call with the original schema (from s.Fields): hit -> same instance
            var i2 = pool.Intern(s.Fields, s.Stride);

            Assert.That(ReferenceEquals(i1, i2), Is.True);
            // Count: Empty + this schema
            Assert.That(pool.Count, Is.EqualTo(2));
        }

        [Test]
        public void OrderSensitive_WhenCanonicalizeOff_DifferentInstances()
        {
            var pool = new SchemaPool();

            // Two schemas with same set but different order; canonicalizeByName: false
            var s1 = MakeSchema(false, ("a", 4), ("b", 4));
            var s2 = MakeSchema(false, ("b", 4), ("a", 4));

            var i1 = pool.Intern(s1);
            var i2 = pool.Intern(s2);

            Assert.That(ReferenceEquals(i1, i2), Is.False);
            // Count: Empty + s1 + s2
            Assert.That(pool.Count, Is.EqualTo(3));
        }

        [Test]
        public void TryGetExisting_WorksBeforeAndAfterIntern()
        {
            var pool = new SchemaPool();
            var s = MakeSchema(true, ("x", 2), ("y", 4));

            // Before interning: should be false
            Assert.That(pool.TryGetExisting(s.Fields, s.Stride, out var none), Is.False);
            Assert.That(none, Is.Null);

            // Intern
            var i1 = pool.Intern(s.Fields, s.Stride);

            // After interning: should be true and return same instance
            Assert.That(pool.TryGetExisting(s.Fields, s.Stride, out var found), Is.True);
            Assert.That(ReferenceEquals(found, i1), Is.True);
        }

        [Test]
        public void Intern_WithArrayEnumerableFastPath()
        {
            var pool = new SchemaPool();
            var s = MakeSchema(true, ("hp", 4), ("mp", 4));

            // Pass as array explicitly
            var arr = s.Fields.ToArray(); // array with correct Name/Length/Offset
            var i1 = pool.Intern((IEnumerable<FieldDescriptor_Old>)arr, s.Stride);

            // Intern the schema object should return the same instance
            var i2 = pool.Intern(s);

            Assert.That(ReferenceEquals(i1, i2), Is.True);
            Assert.That(pool.Count, Is.EqualTo(2));
        }

        [Test]
        public void Intern_SameSchemaFromDifferentInputShapes_AllDeduped()
        {
            var pool = new SchemaPool();
            var s = MakeSchema(true, ("a", 1), ("b", 2), ("c", 8));

            // 1) IEnumerable (materialized fallback)
            var i1 = pool.Intern((IEnumerable<FieldDescriptor_Old>)s.Fields, s.Stride);
            // 2) IReadOnlyList fast path
            var i2 = pool.Intern(s.Fields, s.Stride);
            // 3) Schema object
            var i3 = pool.Intern(s);

            Assert.That(ReferenceEquals(i1, i2) && ReferenceEquals(i2, i3), Is.True);
            Assert.That(pool.Count, Is.EqualTo(2)); // Empty + s
        }

        [Test]
        public void Clear_EmptiesPool()
        {
            var pool = new SchemaPool();
            var s = MakeSchema(true, ("k1", 4));
            pool.Intern(s);
            Assert.That(pool.Count, Is.EqualTo(2));

            pool.Clear();
            Assert.That(pool.Count, Is.EqualTo(1)); // constructor logic not rerun; Clear means empty
        }

        [Test]
        public void Concurrent_Intern_SameSchema_OnlyOneStored()
        {
            var pool = new SchemaPool();
            var s = MakeSchema(true, ("a", 4), ("b", 8));

            const int n = 32;
            Parallel.For(0, n, _ =>
            {
                // Mix different entry points
                if ((_ & 1) == 0) pool.Intern(s);
                else pool.Intern(s.Fields, s.Stride);
            });

            // Only one new unique schema should exist besides Empty
            Assert.That(pool.Count, Is.EqualTo(2));
        }
    }
}
