using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Minerva.DataStorage.Tests
{
    [TestFixture, Category("Perf"), Explicit, Timeout(20000)]
    public class Perf_CreateDispose_Curve
    {
        [Test]
        public void Windowed_CreateDispose_Curve()
        {
            int window = Math.Max(2, Environment.ProcessorCount);
            int rounds = 200;
            var perRoundMs = new List<double>(rounds);

            for (int r = 0; r < rounds; r++)
            {
                var storages = new List<Storage>(window);
                var sw = System.Diagnostics.Stopwatch.StartNew();

                for (int i = 0; i < window; i++)
                {
                    var s = new Storage(ContainerLayout.Empty);
                    storages.Add(s);
                    var root = s.Root;
                    root.Write("i", i);
                    root.Write("f", i * 0.5f);
                    root.Write("s", "abcdefg");
                }

                foreach (var s in storages) s.Dispose();
                sw.Stop();

                perRoundMs.Add(sw.Elapsed.TotalMilliseconds);

                // non-blocking GC occasionally
                if ((r & 7) == 0) GC.Collect(2, GCCollectionMode.Forced, blocking: false, compacting: false);
            }

            var lastHalfAvg = perRoundMs.Skip(perRoundMs.Count / 2).Average();
            TestContext.Progress.WriteLine($"Create+Dispose window={window}: last-half avg = {lastHalfAvg:F2} ms");
            // 稳态不应比前半显著变差（例如 < 1.5x）
            var firstHalfAvg = perRoundMs.Take(perRoundMs.Count / 2).Average();
            Assert.LessOrEqual(lastHalfAvg, firstHalfAvg * 1.5);
        }
    }
}
