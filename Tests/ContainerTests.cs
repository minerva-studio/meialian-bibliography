using System;
using System.Linq;
using NUnit.Framework;
using UnityEngine;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class ContainerTests
    {
        private static Schema_Old MakeSchema(bool canonicalize, params (string name, int len)[] fields)
        {
            var b = new SchemaBuilder(canonicalizeByName: canonicalize);
            foreach (var (n, l) in fields) b.AddFieldFixed(n, l);
            return b.Build();
        }

        [Test]
        public void Construct_ZeroInit_LengthAndDataMatchStride()
        {
            var s = MakeSchema(true, ("hp", 4), ("spd", 4), ("id", 8));
            using var c = Container.CreateWild(s); // default zero-init

            Assert.That(c.Length, Is.EqualTo(s.Stride));
            Assert.That(c.Span.Length, Is.EqualTo(s.Stride));
            Assert.That(c.Span.ToArray().All(b => b == 0), Is.True);
        }

        [Test]
        public void Construct_FromBytes_CopiesIntoBuffer()
        {
            var s = MakeSchema(true, ("hp", 4), ("spd", 4));
            var src = new byte[s.Stride];
            for (int i = 0; i < src.Length; i++) src[i] = (byte)(i + 1);

            using var c = Container.CreateWild(s, src);
            Assert.That(c.Span.SequenceEqual(src), Is.True);
        }

        [Test]
        public void WriteRead_Unmanaged_ByName_Works()
        {
            var s = MakeSchema(true, ("hp", 4), ("spd", 4));
            using var c = Container.CreateWild(s);

            c.WriteNoRescheme<int>("hp", 123456789);
            c.WriteNoRescheme<float>("spd", 3.5f);

            Assert.That(c.Read<int>("hp"), Is.EqualTo(123456789));
            Assert.That(c.Read<float>("spd"), Is.EqualTo(3.5f).Within(1e-6));
        }

        [Test]
        public void Write_Unmanaged_SizeTooLarge_Throws()
        {
            var s = MakeSchema(true, ("tiny", 2));
            using var c = Container.CreateWild(s);
            Assert.That(() => c.WriteNoRescheme<int>("tiny", 42),
                Throws.TypeOf<ArgumentException>().With.Message.Contains("exceeds field length"));
        }

        [Test]
        public void TryReadWrite_Unmanaged_ReturnsTrueOnSmallerContainer()
        {
            var s = MakeSchema(true, ("a", 2), ("b", 4));
            using var c = Container.CreateWild(s);

            var fa = s.GetField("a");
            Assert.That(c.TryWrite<int>("a", 7, false), Is.False); // sizeof(int) > 2 -> false

            c.WriteNoRescheme<int>("b", 300);

            Assert.That(c.TryRead<int>("b", out var v), Is.True);
            Assert.That(v, Is.EqualTo(300));

            Debug.Log(c.ToString());
            Assert.That(c.TryRead<byte>("b", out _), Is.True);
        }

        [Test]
        public void TryReadWrite_Unmanaged_ReturnsTrueOnLargerContainer()
        {
            var s = MakeSchema(true, ("a", 2), ("b", 4));
            using var c = Container.CreateWild(s);

            var fa = s.GetField("a");
            Assert.That(c.TryWrite<int>("a", 7, false), Is.False); // sizeof(int) > 2 -> false

            c.WriteNoRescheme<int>("b", 99);
            Assert.That(c.TryRead<int>("b", out var v), Is.True);
            Assert.That(v, Is.EqualTo(99));

            Assert.That(c.TryRead<long>("b", out _), Is.True);
        }

        [Test]
        public void WriteBytes_ReadBytes_SizeChecksAndRoundtrip()
        {
            var s = MakeSchema(true, ("id", 8));
            using var c = Container.CreateWild(s);

            var payload = new byte[8];
            new System.Random(0xC0FFEE).NextBytes(payload);

            c.WriteBytes("id", payload);
            var dst = new byte[8];
            c.ReadBytes("id", dst);

            Assert.That(dst, Is.EqualTo(payload));

            Assert.That(() => c.WriteBytes("id", new byte[7]),
                Throws.TypeOf<ArgumentException>().With.Message.Contains("must equal field length"));
            Assert.That(c.TryWriteBytes("id", new byte[7]), Is.False);
        }

        [Test]
        public void TrailingBytes_AreCleared_WhenWritingSmallerT()
        {
            Assert.Inconclusive("Haven't figure out what is the best aligment yet");
            // field length 4; write Int16 then read Int32 -> upper bytes must be zeroed
            var s = MakeSchema(true, ("word", 4));
            using var c = Container.CreateWild(s);

            c.WriteNoRescheme<short>("word", unchecked((short)0xABCD)); // little-endian: [CD AB 00 00]
            int val = c.Read<int>("word");
            Assert.That(val, Is.EqualTo(0x0000ABCD));
        }

        [Test]
        public void Clone_ProducesIndependentCopy()
        {
            var s = MakeSchema(true, ("hp", 4), ("mp", 4));
            using var c1 = Container.CreateWild(s);
            c1.WriteNoRescheme<int>("hp", 10);
            c1.WriteNoRescheme<int>("mp", 5);

            using var c2 = c1.Clone();
            Assert.That(c2.Read<int>("hp"), Is.EqualTo(10));
            Assert.That(c2.Read<int>("mp"), Is.EqualTo(5));

            c1.WriteNoRescheme<int>("hp", 77);
            Assert.That(c1.Read<int>("hp"), Is.EqualTo(77));
            Assert.That(c2.Read<int>("hp"), Is.EqualTo(10)); // <-- fixed here
        }

        [Test]
        public void CopyFrom_RequiresIdenticalSchema()
        {
            var s1 = MakeSchema(true, ("a", 4));
            var s2 = MakeSchema(true, ("a", 8)); // different stride

            using var c1 = Container.CreateWild(s1);
            using var c2 = Container.CreateWild(s2);

            Assert.That(() => c1.CopyFrom(c2), Throws.TypeOf<ArgumentException>().With.Message.Contains("Schema mismatch"));
        }

        [Test]
        public void Dispose_ThenAccess_ThrowsObjectDisposed()
        {
            var s = MakeSchema(true, ("x", 4));
            var c = Container.CreateWild(s);
            c.Dispose();

            Assert.That(() => { var _ = c.Span; }, Throws.TypeOf<ObjectDisposedException>());
            Assert.That(() => c.Clear(), Throws.TypeOf<ObjectDisposedException>());
        }

        [Test]
        public void ZeroStride_UsesEmptyBuffer()
        {
            var empty = Schema_Old.Empty;
            using var c = Container.CreateWild(empty);
            Assert.That(c.Length, Is.EqualTo(0));
            Assert.That(c.Span.Length, Is.EqualTo(0));
        }

        [Test]
        public void Pool_RentReturn_BalancesRetainedCount()
        {
            var s = MakeSchema(true, ("hp", 4));
            int before = s.Pool.RetainedCount;

            var c = Container.CreateWild(s);
            c.Dispose();

            int after = s.Pool.RetainedCount;
            Assert.That(after, Is.GreaterThanOrEqualTo(before));
        }
    }
}
