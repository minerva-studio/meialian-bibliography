using NUnit.Framework;
using System;

namespace Minerva.DataStorage.Tests
{
    [TestFixture, Category("Perf"), Explicit]
    public class Perf_StringLeaf_CapacityReuse
    {
        private static string GenString(char ch, int len)
        {
            // avoid per-iteration new char[len]: rely on string ctor optimization
            return new string(ch, len);
        }

        [Test]
        public void ExternalString_Alternating_HugeTiny_NoThrash()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            // warmup with medium
            r.Write("S", GenString('a', 4096));

            var stats = new Perf.LatencyStats();
            int huge = 64 * 1024, tiny = 1;

            for (int i = 0; i < 20_000; i++)
            {
                var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
                r.Write("S", GenString('X', (i & 1) == 0 ? huge : tiny));
                var t1 = System.Diagnostics.Stopwatch.GetTimestamp();
                if ((i & 1) == 0) stats.Add(Perf.StopwatchTicksToNs(t1 - t0));
            }

            var p95 = stats.Percentile(0.95);
            TestContext.Progress.WriteLine($"External string write (huge): p95={p95}ns");

            Assert.Less(p95, 150_000);
        }
    }

    [TestFixture, Category("Perf"), Explicit]
    public class Perf_Mixed_Workload
    {
        [TestCase(0.60, 0.20, 0.10, 0.10)] // 60% numeric writes, 20% reads, 10% string, 10% object
        [TestCase(0.30, 0.50, 0.10, 0.10)]
        public void Mixed_RW_Object_String(double wWriteNum, double wReadNum, double wStr, double wObj)
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;
            var rnd = new Random(42);
            var stats = new Perf.LatencyStats();

            const int n = 300_000;
            double tWrite = 0, tRead = 0, tStr = 0, tObj = 0;

            for (int i = 0; i < n; i++)
            {
                double p = rnd.NextDouble();
                var t0 = System.Diagnostics.Stopwatch.GetTimestamp();

                if (p < wWriteNum)
                {
                    r.Write("n", rnd.NextDouble()); // promotes to double; steady
                    tWrite++;
                }
                else if (p < wWriteNum + wReadNum)
                {
                    _ = r.Read<double>("n", true);
                    tRead++;
                }
                else if (p < wWriteNum + wReadNum + wStr)
                {
                    r.Write("s", new string('x', (i & 31) == 0 ? 4096 : 8));
                    tStr++;
                }
                else
                {
                    var c = r.GetObject("o");
                    if ((i & 7) == 0) c.Write("k", i);
                    tObj++;
                }

                var t1 = System.Diagnostics.Stopwatch.GetTimestamp();
                if ((i & 7) == 0) stats.Add(Perf.StopwatchTicksToNs(t1 - t0));
            }

            var p95 = stats.Percentile(0.95);
            TestContext.Progress.WriteLine($"Mixed workload p95={p95}ns, mix=({tWrite / n:P0} W, {tRead / n:P0} R, {tStr / n:P0} S, {tObj / n:P0} O)");
            Assert.Less(p95, 10_000);
        }
    }
}
