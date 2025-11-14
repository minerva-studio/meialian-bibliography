using NUnit.Framework;

namespace Minerva.DataStorage.Tests
{
    [TestFixture, Category("Perf"), Explicit]
    public class Perf_Latency_Distribution
    {
        [Test]
        public void WriteThenRead_Double_Latency_Distribution()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;
            var stats = new Perf.LatencyStats();

            const int n = 200_000;
            var sw = System.Diagnostics.Stopwatch.StartNew();
            for (int i = 0; i < n; i++)
            {
                var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
                r.Write("x", (double)i);
                var v = r.Read<double>("x");
                var t1 = System.Diagnostics.Stopwatch.GetTimestamp();

                if (v != i) Assert.Fail("Read-after-write mismatch");
                var ns = Perf.StopwatchTicksToNs(t1 - t0);
                if ((i & 3) == 0) stats.Add(ns); // sample to reduce overhead
            }
            sw.Stop();

            var p50 = stats.Percentile(0.50);
            var p95 = stats.Percentile(0.95);
            var p99 = stats.Percentile(0.99);
            TestContext.Progress.WriteLine($"write+read(double): p50={p50}ns, p95={p95}ns, p99={p99}ns");

            Assert.Less(p95, 2_000); // 合理阈值：根据机器调；Debug 放宽
            Assert.Less(p99, 5_000);
        }
    }
}
