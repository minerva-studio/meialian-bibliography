using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Amlos.Container.Tests
{
    internal static class Perf
    {
        // Simple percentile-friendly histogram with reservoir sampling
        public sealed class LatencyStats
        {
            private readonly List<long> _samples = new(capacity: 1024);
            private readonly int _reservoir; // keep at most N samples to control memory
            private readonly Random _rnd = new(123);

            public LatencyStats(int reservoir = 8192) => _reservoir = Math.Max(256, reservoir);

            public void Add(long ns)
            {
                if (_samples.Count < _reservoir) { _samples.Add(ns); return; }
                // reservoir sampling
                int i = _rnd.Next(int.MaxValue) % (_samples.Count + 1);
                if (i < _reservoir) _samples[i] = ns;
            }

            public int Count => _samples.Count;
            public double Mean() => _samples.Count == 0 ? 0 : _samples.Average();
            public long Percentile(double p)
            {
                if (_samples.Count == 0) return 0;
                var arr = _samples.ToArray();
                Array.Sort(arr);
                int idx = Math.Clamp((int)Math.Round(p * (arr.Length - 1)), 0, arr.Length - 1);
                return arr[idx];
            }
        }

        public static long StopwatchTicksToNs(long ticks) =>
            (long)(ticks * (1_000_000_000.0 / Stopwatch.Frequency));

        public static long MeasureOnce(Action body, int iters)
        {
            // warmup
            for (int i = 0; i < Math.Min(10_000, iters / 10 + 1); i++) body();

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < iters; i++) body();
            sw.Stop();
            return StopwatchTicksToNs(sw.ElapsedTicks);
        }
    }
}
