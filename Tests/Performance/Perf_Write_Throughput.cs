using NUnit.Framework;
using System.Collections.Generic;

namespace Amlos.Container.Tests
{
    [TestFixture, Category("Perf"), Explicit] // run manually
    public class Perf_Write_Throughput
    {
        [Test]
        public void Write_Int_1M()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            // warmup
            for (int i = 0; i < 50_000; i++) s.Root.Write("x", i);

            const int n = 1_000_000;
            var ns = Perf.MeasureOnce(() => { s.Root.Write("x", 42); }, n);
            var nsPerOp = (double)ns / n;

            TestContext.Progress.WriteLine($"Write(int) ns/op = {nsPerOp:F1}");
            Assert.Less(nsPerOp, 200);
        }

        [Test]
        public void Write_Int_1M_Index()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            s.Root.Write("x", 0);
            var idx = 0;

            // warmup
            for (int i = 0; i < 50_000; i++) s.Root.Write(idx, i);

            const int n = 1_000_000;
            var ns = Perf.MeasureOnce(() => { s.Root.Write(idx, 42); }, n);
            var nsPerOp = (double)ns / n;

            TestContext.Progress.WriteLine($"Write(int) ns/op = {nsPerOp:F1}");
            Assert.Less(nsPerOp, 200);
        }

        [Test]
        public void WriteDictionary_Int_1M_Index()
        {
            var dictionary = new Dictionary<string, object>();

            // warmup
            for (int i = 0; i < 50_000; i++) dictionary["x"] = i;

            const int n = 1_000_000;
            var ns = Perf.MeasureOnce(() => { dictionary["x"] = 42; }, n);
            var nsPerOp = (double)ns / n;

            TestContext.Progress.WriteLine($"Write(int) ns/op = {nsPerOp:F1}");
            Assert.Less(nsPerOp, 200);
        }

        [Test]
        public void Write_IntThenFloat_Promotion_Cost()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            // int baseline
            var nsInt = Perf.MeasureOnce(() => { s.Root.Write("n", 1); }, 500_000) / 500_000.0;

            // trigger promotion (int -> double) then steady float writes
            r.Write("n", 1.0f);
            var nsFloat = Perf.MeasureOnce(() => { s.Root.Write("n", 1.0f); }, 500_000) / 500_000.0;

            TestContext.Progress.WriteLine($"int ns/op={nsInt:F1}, float-after-promotion ns/op={nsFloat:F1}");
            Assert.Less(nsFloat, nsInt * 1.5); // promotion 后 steady 性能不应显著回退
        }
    }
}
