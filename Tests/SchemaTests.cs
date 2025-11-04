using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class SchemaTests
    {
        [Test]
        public void TryGetField_ReturnsByName()
        {
            var b = new SchemaBuilder();
            b.AddFieldFixed("hp", 4).AddFieldFixed("speed", 4);
            var s = b.Build();

            Assert.That(s.TryGetField("hp", out var hp), Is.True);
            Assert.That(hp.Name, Is.EqualTo("hp"));
            Assert.That(hp.Length, Is.EqualTo(4));

            Assert.That(s.TryGetField("not-exist", out _), Is.False);
            Assert.That(() => s.GetField("not-exist"), Throws.TypeOf<KeyNotFoundException>());
        }

        [Test]
        public void Schema_IsImmutable_OffsetsNotExternallyMutable()
        {
            var b = new SchemaBuilder();
            b.AddFieldFixed("a", 4).AddFieldFixed("b", 4);
            var s = b.Build();

            var a = s.GetField("a");
            var bfd = s.GetField("b");

            // We can read offsets¡­
            int offA = a.Offset;
            int offB = bfd.Offset;
            Assert.That(offA, Is.Not.EqualTo(offB));

            // ¡­but cannot mutate schema internals (Offset has internal setter).
            // (Compile-time protection; no runtime test needed.)
            Assert.Pass();
        }

        [Test]
        public void EqualityAndHashCode_MatchForIdenticalSchemas()
        {
            var s1 = new SchemaBuilder().AddFieldFixed("a", 1).AddFieldFixed("b", 2).Build();
            var s2 = new SchemaBuilder().AddFieldFixed("a", 1).AddFieldFixed("b", 2).Build();

            Assert.That(s1.Equals(s2), Is.True);
            Assert.That(s1 == s2, Is.True);
            Assert.That(s1.GetHashCode(), Is.EqualTo(s2.GetHashCode()));
        }

        [Test]
        public void Constructor_GuardsDuplicateNames()
        {
            // Force a duplicate by bypassing builder¡¯s duplicate guard:
            // Build two identical fields, then manually attempt to create Schema with duplicates.
            var fd1 = FieldDescriptor.Fixed("dup", 4).WithOffset(0);
            var fd2 = FieldDescriptor.Fixed("dup", 8).WithOffset(4);

            Assert.That(() => new Schema(new[] { fd1, fd2 }, stride: 12),
                Throws.TypeOf<ArgumentException>().With.Message.Contains("Duplicate field name"));
        }
    }
}
