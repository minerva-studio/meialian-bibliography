// SchemaTests.cs
using NUnit.Framework;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class FieldDescriptorTests
    {
        [Test]
        public void FromFixed_SetsNameAndLength()
        {
            var f = FieldDescriptor_Old.Fixed("hp", 4);
            Assert.That(f.Name, Is.EqualTo("hp"));
            Assert.That(f.Length, Is.EqualTo(4));
            Assert.That(f.Offset, Is.EqualTo(0)); // default until builder assigns
        }

        [Test]
        public void FromType_UsesSizeOfT()
        {
            var fI32 = FieldDescriptor_Old.Type<int>("i32");
            var fF32 = FieldDescriptor_Old.Type<float>("f32");
            Assert.That(fI32.Length, Is.EqualTo(4));
            Assert.That(fF32.Length, Is.EqualTo(4));
        }

        [Test]
        public void Clone_CopiesNameAndLength_OffsetIsIndependent()
        {
            var f = FieldDescriptor_Old.Fixed("x", 8).WithOffset(16);

            var c = f.WithBaseInfo();
            Assert.That(c.Name, Is.EqualTo("x"));
            Assert.That(c.Length, Is.EqualTo(8));
            Assert.That(c.Offset, Is.EqualTo(0)); // clone starts ¡°unplaced¡±
        }

        [Test]
        public void Equality_ComparesNameLengthOffset()
        {
            var a = FieldDescriptor_Old.Fixed("a", 4).WithOffset(0);
            var b = FieldDescriptor_Old.Fixed("a", 4).WithOffset(0);
            var c = FieldDescriptor_Old.Fixed("a", 4).WithOffset(4);

            Assert.That(a == b, Is.True);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a != c, Is.True);
            Assert.That(a.Equals(c), Is.False);

            // HashCode matches equality contract (probabilistic check)
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        }
    }
}
