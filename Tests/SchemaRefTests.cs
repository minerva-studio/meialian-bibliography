using NUnit.Framework;
using static Amlos.Container.Tests.SchemaTestUtils;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class SchemaRefTests
    {
        [Test]
        public void FieldDescriptor_Ref_Semantics_AreCorrect()
        {
            var single = FieldDescriptor.Reference("child");
            Assert.That(single.IsRef, Is.True);
            Assert.That(single.AbsLength, Is.EqualTo(FieldDescriptor.REF_SIZE));
            Assert.That(single.RefCount, Is.EqualTo(1));
            Assert.That(single.IsRefArray, Is.False);

            var arr = FieldDescriptor.ReferenceArray("children", 3);
            Assert.That(arr.IsRef, Is.True);
            Assert.That(arr.AbsLength, Is.EqualTo(3 * FieldDescriptor.REF_SIZE));
            Assert.That(arr.RefCount, Is.EqualTo(3));
            Assert.That(arr.IsRefArray, Is.True);

            var val = FieldDescriptor.Fixed("hp", 4);
            Assert.That(val.IsRef, Is.False);
            Assert.That(val.AbsLength, Is.EqualTo(4));
            Assert.That(val.RefCount, Is.EqualTo(0));
            Assert.That(val.IsRefArray, Is.False);
        }

        [Test]
        public void Build_With_Mixed_Ref_And_Value_AlignsAndOffsets_Deterministically()
        {
            var b = new SchemaBuilder(canonicalizeByName: true);
            b.AddFieldOf<int>("a_val");
            b.AddRef("b_child");
            b.AddFieldFixed("c_blob", 16);

            var s = b.Build();

            Assert.That(s.Fields.Count, Is.EqualTo(3));

            AssertFieldAt(s, "a_val", 0, 4);   // relative 0
            AssertFieldAt(s, "b_child", 8, 8);   // aligned to 8 relative
            AssertFieldAt(s, "c_blob", 16, 16);  // relative 16

            AssertStride(s, expectedRelativeBytesEnd: 32);
        }

        [Test]
        public void Build_With_RefArray_Computes_Count_And_Offsets()
        {
            var b = new SchemaBuilder(canonicalizeByName: true);
            b.AddRefArray("children", 3);      // 24 bytes, align 8 (relative 0)
            b.AddFieldOf<short>("flag");       // 2 bytes, align 2 (relative 24)

            var s = b.Build();

            var fChildren = s.GetField("children");
            var fFlag = s.GetField("flag");

            Assert.That(fChildren.IsRef, Is.True);
            Assert.That(fChildren.RefCount, Is.EqualTo(3));
            Assert.That(fChildren.AbsLength, Is.EqualTo(24));
            Assert.That(fChildren.Offset % 8, Is.EqualTo(0));

            // children@0(rel), flag@24(rel)
            AssertFieldAt(s, "children", 0, 24);
            AssertFieldAt(s, "flag", 24, 2);

            // Stride = AlignUp(DB + 26, 8)
            AssertStride(s, expectedRelativeBytesEnd: 26);
        }

        [Test]
        public void CanonicalizeByName_Makes_Order_Independent()
        {
            // Same fields different insertion order
            var a1 = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .AddRef("child")
                .AddFieldFixed("blob", 8)
                .Build();

            var a2 = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldFixed("blob", 8)
                .AddRef("child")
                .AddFieldOf<int>("hp")
                .Build();

            // Equal and interned to identical instances (via pool)
            Assert.That(a1, Is.EqualTo(a2));
            var i1 = SchemaPool.Shared.Intern(a1);
            var i2 = SchemaPool.Shared.Intern(a2);
            Assert.That(ReferenceEquals(i1, i2), Is.True);
        }

        [Test]
        public void NonCanonical_Order_Matters()
        {
            var s1 = new SchemaBuilder(false)
                .AddFieldOf<int>("hp")
                .AddRef("child")
                .Build();

            var s2 = new SchemaBuilder(false)
                .AddRef("child")
                .AddFieldOf<int>("hp")
                .Build();

            // In s1, relative: hp@0, child after hp
            AssertFieldAt(s1, "hp", 0, 4);
            Assert.That(s1.GetField("child").Offset, Is.GreaterThan(s1.GetField("hp").Offset));

            // In s2, relative: child@0, hp after child
            AssertFieldAt(s2, "child", 0, 8);
            Assert.That(s2.GetField("hp").Offset, Is.GreaterThan(s2.GetField("child").Offset));

            Assert.That(s1.Equals(s2), Is.False);
        }


        [Test]
        public void FromSchema_Rebuild_Equivalent_When_Canonicalization_Matches()
        {
            var original = new SchemaBuilder(canonicalizeByName: true)
                .AddFieldOf<int>("hp")
                .AddRefArray("children", 2)
                .Build();

            var b = SchemaBuilder.FromSchema(original, canonicalizeByName: true);
            var rebuilt = b.Build();

            Assert.That(rebuilt, Is.EqualTo(original));

            // Pool should return the same interned instance
            var i1 = SchemaPool.Shared.Intern(original);
            var i2 = SchemaPool.Shared.Intern(rebuilt);
            Assert.That(ReferenceEquals(i1, i2), Is.True);
        }

        [Test]
        public void Pool_Interning_With_RefFields_Returns_Same_Instance()
        {
            var s1 = new SchemaBuilder(true)
                .AddRef("child")
                .AddRefArray("siblings", 2)
                .AddFieldOf<long>("ts")
                .Build();

            var s2 = new SchemaBuilder(true)
                .AddFieldOf<long>("ts")
                .AddRefArray("siblings", 2)
                .AddRef("child")
                .Build();

            var i1 = SchemaPool.Shared.Intern(s1);
            var i2 = SchemaPool.Shared.Intern(s2);

            Assert.That(s1, Is.EqualTo(s2));
            Assert.That(ReferenceEquals(i1, i2), Is.True);
        }

        [Test]
        public void Offsets_Are_Monotonic_And_DoNotOverlap()
        {
            var s = new SchemaBuilder(true)
                .AddRefArray("children", 3)  // 24 @ 8
                .AddFieldOf<int>("hp")       // 4 @ 4
                .AddFieldFixed("blob", 12)   // 12 @ 8
                .Build();

            Assert.That(s.Fields[0].Offset, Is.EqualTo(s.DataBase));

            // Simple overlap check: for each field pair, their [offset, offset+len) ranges don't intersect
            for (int i = 0; i < s.Fields.Count; i++)
            {
                for (int j = i + 1; j < s.Fields.Count; j++)
                {
                    var fi = s.Fields[i];
                    var fj = s.Fields[j];
                    int iBeg = fi.Offset, iEnd = fi.Offset + fi.AbsLength;
                    int jBeg = fj.Offset, jEnd = fj.Offset + fj.AbsLength;

                    bool overlap = !(iEnd <= jBeg || jEnd <= iBeg);
                    Assert.That(overlap, Is.False, $"Fields overlap: {fi.Name} [{iBeg},{iEnd}) vs {fj.Name} [{jBeg},{jEnd})");
                }
            }
        }
    }
}
