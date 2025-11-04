using NUnit.Framework;
using System;
using System.Linq;
using static Amlos.Container.Tests.SchemaTestUtils;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class SchemaBuilderTests
    {
        [Test]
        public void Build_AutoAlignment_Int_Byte_Double()
        {
            var b = new SchemaBuilder(canonicalizeByName: false);
            b.AddFieldOf<int>("i")
             .AddFieldFixed("b", 1)
             .AddFieldOf<double>("d");

            var s = b.Build();

            // Data-relative expectations: i@0(4), b@4(1), d@8(8), end=16.
            AssertFieldAt(s, "i", 0, 4);
            AssertFieldAt(s, "b", 4, 1);
            AssertFieldAt(s, "d", 8, 8);
            AssertStride(s, expectedRelativeBytesEnd: 16);
        }

        [Test]
        public void CanonicalizeByName_ProducesEqualLayoutsForSameSet()
        {
            var b1 = new SchemaBuilder(canonicalizeByName: true);
            b1.AddFieldFixed("a", 3).AddFieldFixed("b", 4);
            var s1 = b1.Build();

            var b2 = new SchemaBuilder(canonicalizeByName: true);
            b2.AddFieldFixed("b", 4).AddFieldFixed("a", 3);
            var s2 = b2.Build();

            Assert.That(s1, Is.EqualTo(s2));
            Assert.That(s1.GetField("a").Offset, Is.EqualTo(s2.GetField("a").Offset));
            Assert.That(s1.GetField("b").Offset, Is.EqualTo(s2.GetField("b").Offset));
            Assert.That(s1.Stride, Is.EqualTo(s2.Stride));
        }

        [Test]
        public void WithoutCanonicalization_InsertionOrderChangesLayout()
        {
            var b1 = new SchemaBuilder(canonicalizeByName: false);
            b1.AddFieldFixed("a", 3).AddFieldFixed("b", 4);
            var s1 = b1.Build();

            var b2 = new SchemaBuilder(canonicalizeByName: false);
            b2.AddFieldFixed("b", 4).AddFieldFixed("a", 3);
            var s2 = b2.Build();

            // Different offsets due to different order (even with same auto-alignment rule)
            Assert.That(s1.Equals(s2), Is.False);
        }

        [Test]
        public void FromSchema_RebuildsEquivalentSchema_WithAutoAlignment()
        {
            var b = new SchemaBuilder(canonicalizeByName: true);
            b.AddFieldFixed("x", 2).AddFieldOf<int>("y"); // auto-alignment applied
            var s = b.Build();

            var b2 = SchemaBuilder.FromSchema(s, canonicalizeByName: true);
            var s2 = b2.Build();

            // Same canonicalization + same auto-align rule -> identical layout
            Assert.That(s2, Is.EqualTo(s));
        }

        [Test]
        public void Build_EmptySchema_HasStrideZero()
        {
            var b = new SchemaBuilder();
            var s = b.Build();
            Assert.That(s.Stride, Is.EqualTo(0));
            Assert.That(s.Fields.Count, Is.EqualTo(0));
        }

        [Test]
        public void AddFieldFixed_DuplicateName_Throws()
        {
            var b = new SchemaBuilder();
            b.AddFieldFixed("dup", 4);
            Assert.That(() => b.AddFieldFixed("dup", 8),
                Throws.TypeOf<ArgumentException>().With.Message.Contains("already added"));
        }

        [Test]
        public void CanonicalizeByName_MakesLayoutsEqualForSameFieldSet()
        {
            // Builder 1: insertion order A,B
            var b1 = new SchemaBuilder(canonicalizeByName: true);
            b1.AddFieldFixed("a", 4).AddFieldFixed("b", 4);
            var s1 = b1.Build();

            // Builder 2: insertion order B,A
            var b2 = new SchemaBuilder(canonicalizeByName: true);
            b2.AddFieldFixed("b", 4).AddFieldFixed("a", 4);
            var s2 = b2.Build();

            Assert.That(s1, Is.EqualTo(s2));
            Assert.That(s1.GetField("a").Offset, Is.EqualTo(s2.GetField("a").Offset));
            Assert.That(s1.GetField("b").Offset, Is.EqualTo(s2.GetField("b").Offset));
        }

        [Test]
        public void WithoutCanonicalization_InsertionOrderChangesLayoutAndSchemaInequality()
        {
            var b1 = new SchemaBuilder(false);
            b1.AddFieldFixed("a", 4).AddFieldFixed("b", 4);
            var s1 = b1.Build();

            var b2 = new SchemaBuilder(false);
            b2.AddFieldFixed("b", 4).AddFieldFixed("a", 4);
            var s2 = b2.Build();

            // In s1: a@0, b@4 (relative)
            AssertFieldAt(s1, "a", 0, 4);
            AssertFieldAt(s1, "b", 4, 4);

            // In s2: b@0, a@4 (relative)
            AssertFieldAt(s2, "b", 0, 4);
            AssertFieldAt(s2, "a", 4, 4);

            // Layout differs ¡ú schemas are not equal.
            Assert.That(s1.Equals(s2), Is.False);
        }


        [Test]
        public void FromSchema_RebuildsEquivalentSchema()
        {
            var b = new SchemaBuilder(canonicalizeByName: true);
            b.AddFieldFixed("x", 2).AddFieldOf<int>("y");
            var s = b.Build();

            var b2 = SchemaBuilder.FromSchema(s, canonicalizeByName: true);
            var s2 = b2.Build();

            Assert.That(s2, Is.EqualTo(s));
            Assert.That(s2.Stride, Is.EqualTo(s.Stride));
            Assert.That(s2.Fields.Select(f => (f.Name, f.Length)),
                Is.EquivalentTo(s.Fields.Select(f => (f.Name, f.Length))));
        }

        [Test]
        public void Build_EmptySchema_HasStrideZeroAndNoFields()
        {
            var b = new SchemaBuilder();
            var s = b.Build();
            Assert.That(s.Stride, Is.EqualTo(0));
            Assert.That(s.Fields.Count, Is.EqualTo(0));
        }
    }
}
