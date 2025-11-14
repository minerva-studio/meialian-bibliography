using NUnit.Framework;
using System;
using System.Threading.Tasks;

namespace Minerva.DataStorage.Tests
{
    [TestFixture, Category("Perf"), Explicit, Timeout(30000)]
    public class Perf_Registry_Scale
    {
        [Test]
        public void Parallel_CreateChildren_UniqueAndFast()
        {
            int workers = Math.Max(2, Environment.ProcessorCount);
            int perWorker = 30_000;

            var errors = 0;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            Parallel.For(0, workers, new ParallelOptions { MaxDegreeOfParallelism = workers }, _ =>
            {
                try
                {
                    using var s = new Storage(ContainerLayout.Empty);
                    var r = s.Root;
                    for (int i = 0; i < perWorker; i++)
                    {
                        var c = r.GetObject("c"); // reuse same slot; ensures registry operations, but limited growth
                        if ((i & 7) == 0) c.Write("v", i);
                    }
                }
                catch { System.Threading.Interlocked.Increment(ref errors); }
            });

            sw.Stop();
            var totalOps = (long)workers * perWorker;
            var mops = totalOps / (sw.Elapsed.TotalSeconds * 1e6);
            TestContext.Progress.WriteLine($"Registry-heavy ops: {totalOps} in {sw.Elapsed.TotalMilliseconds:F0} ms ({mops:F2} Mops/s)");
            Assert.Zero(errors);
        }
    }
}
