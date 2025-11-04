// SchemaRefTests.cs
using NUnit.Framework;

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
            // Names chosen so that alphabetical order == insertion order (for clarity)
            var b = new SchemaBuilder(canonicalizeByName: true);
            b.AddFieldOf<int>("a_val");        // 4 bytes, align 4
            b.AddRef("b_child");                // 8 bytes (ref), align 8
            b.AddFieldFixed("c_blob", 16);      // 16 bytes, align (capped at 8)

            var s = b.Build();

            // Basic sanity
            Assert.That(s.Fields.Count, Is.EqualTo(3));
            var a = s.GetField("a_val");
            var bchild = s.GetField("b_child");
            var cblob = s.GetField("c_blob");

            // a_val at offset 0 with 4 bytes
            Assert.That(a.Offset, Is.EqualTo(0));
            Assert.That(a.AbsLength, Is.EqualTo(4));
            // b_child aligned to 8: previous end = 4 -> pad 4 -> offset 8
            Assert.That(bchild.Offset % 8, Is.EqualTo(0));
            Assert.That(bchild.Offset, Is.EqualTo(8));
            Assert.That(bchild.AbsLength, Is.EqualTo(8));
            Assert.That(bchild.IsRef, Is.True);

            // c_blob align 8, previous end = 16 -> already aligned -> offset 16
            Assert.That(cblob.Offset, Is.EqualTo(16));
            Assert.That(cblob.AbsLength, Is.EqualTo(16));

            // Stride: 0..4 (a) + pad4 + 8 (b) + 16 (c) => end at 32
            Assert.That(s.Stride, Is.EqualTo(32));
        }

        [Test]
        public void Build_With_RefArray_Computes_Count_And_Offsets()
        {
            var b = new SchemaBuilder(canonicalizeByName: true);
            b.AddRefArray("children", 3);      // 24 bytes, align 8
            b.AddFieldOf<short>("flag");       // 2 bytes, align 2

            var s = b.Build();

            var fChildren = s.GetField("children");
            var fFlag = s.GetField("flag");

            Assert.That(fChildren.IsRef, Is.True);
            Assert.That(fChildren.RefCount, Is.EqualTo(3));
            Assert.That(fChildren.AbsLength, Is.EqualTo(24));
            Assert.That(fChildren.Offset % 8, Is.EqualTo(0));

            // After children (24 @ align 8) -> next offset 24; flag align=2 -> (24 already aligned for 2)
            Assert.That(fFlag.Offset, Is.EqualTo(24));
            Assert.That(s.Stride, Is.EqualTo(24 + 2)); // note: no tail pad added by builder
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
            var s1 = new SchemaBuilder(canonicalizeByName: false)
                .AddFieldOf<int>("hp")
                .AddRef("child")
                .Build();

            var s2 = new SchemaBuilder(canonicalizeByName: false)
                .AddRef("child")
                .AddFieldOf<int>("hp")
                .Build();

            // Layout differs (offsets swapped)
            Assert.That(s1.Equals(s2), Is.False);
            Assert.That(s1.GetField("hp").Offset, Is.EqualTo(0));
            Assert.That(s1.GetField("child").Offset, Is.GreaterThan(s1.GetField("hp").Offset));

            Assert.That(s2.GetField("child").Offset, Is.EqualTo(0));
            Assert.That(s2.GetField("hp").Offset, Is.GreaterThan(s2.GetField("child").Offset));
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
