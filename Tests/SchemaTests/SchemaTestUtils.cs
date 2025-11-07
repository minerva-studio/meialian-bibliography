using NUnit.Framework;

namespace Amlos.Container.Tests
{
    public static class SchemaTestUtils
    {
        public static void AssertFieldAt(Schema_Old s, string name, int expectedRelativeOffset, int expectedLength)
        {
            // Translate a "data-relative" expected offset to absolute by adding DataBase.
            var f = s.GetField(name);
            Assert.That(f.Offset, Is.EqualTo(s.DataBase + expectedRelativeOffset), $"Field '{name}' offset mismatch.");
            Assert.That(f.AbsLength, Is.EqualTo(expectedLength), $"Field '{name}' length mismatch.");
        }

        public static void AssertStride(Schema_Old s, int expectedRelativeBytesEnd)
        {
            // Stride = AlignUp(DataBase + endOfDataRelative, 8)
            var expected = Schema_Old.AlignUp(s.DataBase + expectedRelativeBytesEnd, Schema_Old.ALIGN);
            Assert.That(s.Stride, Is.EqualTo(expected), "Stride mismatch.");
        }

    }
}
