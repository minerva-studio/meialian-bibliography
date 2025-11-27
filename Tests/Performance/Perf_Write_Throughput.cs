#if UNITY_EDITOR
using NUnit.Framework;
using System.Collections.Generic;
using Unity.PerformanceTesting;

namespace Minerva.DataStorage.Tests
{
    [TestFixture, Category("Perf"), Explicit] // 手动跑
    public class Perf_Write_Throughput_UTP
    {
        [Test, Performance]
        public void Write_Int_ByName()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            // 初始化字段，避免测量期间触发建字段/促升
            r.Write("x", 0);

            Measure.Method(() =>
            {
                s.Root.Write("x", 42);
            })
                .SampleGroup(new SampleGroup("Write(int) by name", SampleUnit.Microsecond)) // UPT会按“每次迭代”计时
                .WarmupCount(20)
                .MeasurementCount(50)
                .IterationsPerMeasurement(100)
                .GC()
                .Run();
        }

        [Test, Performance]
        public void Write_Int_ByIndex()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            r.Write("x", 0);
            int idx = 0;

            Measure.Method(() =>
            {
                s.Root.Write(idx, 42);
            })
                .SampleGroup(new SampleGroup("Write(int) by index", SampleUnit.Microsecond))
                .WarmupCount(20)
                .MeasurementCount(50)
                .IterationsPerMeasurement(100)
                .GC()
                .Run();
        }

        [Test, Performance]
        public void Dictionary_Write_Int()
        {
            var dict = new Dictionary<string, object>(capacity: 4);
            dict["x"] = 0;

            Measure.Method(() =>
            {
                dict["x"] = 42;
            })
                .SampleGroup(new SampleGroup("Dictionary write(int)", SampleUnit.Microsecond))
                .WarmupCount(20)
                .MeasurementCount(50)
                .IterationsPerMeasurement(100)
                .GC()
                .Run();
        }

        [Test, Performance]
        public void Dictionary_Read_Int()
        {
            int arrLen = 1000;
            int[] arr = new int[arrLen];
            var dict = new Dictionary<string, object>(capacity: 4);
            dict["x"] = 0;
            int i = 0;

            Measure.Method(() =>
            {
                arr[i++ % arrLen] = (int)dict["x"];
            })
                .SampleGroup(new SampleGroup("Dictionary write(int)", SampleUnit.Microsecond))
                .WarmupCount(20)
                .MeasurementCount(50)
                .IterationsPerMeasurement(100)
                .GC()
                .Run();
        }

        [Test, Performance]
        public void Read_Int_ByName()
        {
            int arrLen = 1000;
            int[] arr = new int[arrLen];
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;
            int i = 0;

            // 初始化字段，避免测量期间触发建字段/促升
            r.Write("x", 0);

            Measure.Method(() =>
            {
                arr[i++ % arrLen] = s.Root.Read<int>("x");
            })
                .SampleGroup(new SampleGroup("Write(int) by name", SampleUnit.Microsecond)) // UPT会按“每次迭代”计时
                .WarmupCount(20)
                .MeasurementCount(50)
                .IterationsPerMeasurement(100)
                .GC()
                .Run();
        }

        [Test, Performance]
        public void Read_Int_ByIndex()
        {
            int arrLen = 1000;
            int[] arr = new int[arrLen];
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;
            int i = 0;

            // 初始化字段，避免测量期间触发建字段/促升
            r.Write("x", 0);

            Measure.Method(() =>
            {
                arr[i++ % arrLen] = s.Root.Read<int>(0);
            })
                .SampleGroup(new SampleGroup("Write(int) by name", SampleUnit.Microsecond)) // UPT会按“每次迭代”计时
                .WarmupCount(20)
                .MeasurementCount(50)
                .IterationsPerMeasurement(100)
                .GC()
                .Run();
        }
    }
}
#else
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Minerva.DataStorage.Tests
{
    [TestFixture, Category("Perf"), Explicit]
    public class Perf_Write_Throughput
    {
        private static void RunSimpleBenchmark(string name, Action action, int warmup = 20, int measurementCount = 50, int iterationsPerMeasurement = 100)
        {
            // Warmup
            for (int w = 0; w < warmup; w++) action();

            var sw = new Stopwatch();
            double totalMicros = 0;
            double best = double.MaxValue;
            double worst = double.MinValue;

            for (int m = 0; m < measurementCount; m++)
            {
                sw.Restart();
                for (int i = 0; i < iterationsPerMeasurement; i++) action();
                sw.Stop();
                var micros = sw.Elapsed.TotalMilliseconds * 1000.0 / iterationsPerMeasurement;
                totalMicros += micros;
                if (micros < best) best = micros;
                if (micros > worst) worst = micros;
            }

            var avg = totalMicros / measurementCount;
            TestContext.WriteLine($"[Benchmark] {name}: avg {avg:F3} µs/iter (best {best:F3}, worst {worst:F3}) over {measurementCount}x{iterationsPerMeasurement} iterations");
        }

        [Test]
        public void Write_Int_ByName()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            r.Write("x", 0);

            RunSimpleBenchmark("Write(int) by name", () => s.Root.Write("x", 42));
        }

        [Test]
        public void Write_Int_ByIndex()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            r.Write("x", 0);
            int idx = 0;

            RunSimpleBenchmark("Write(int) by index", () => s.Root.Write(idx, 42));
        }

        [Test]
        public void Dictionary_Write_Int()
        {
            var dict = new Dictionary<string, object>(capacity: 4);
            dict["x"] = 0;

            RunSimpleBenchmark("Dictionary write(int)", () => dict["x"] = 42);
        }

        [Test]
        public void Dictionary_Read_Int()
        {
            int arrLen = 1000;
            int[] arr = new int[arrLen];
            var dict = new Dictionary<string, object>(capacity: 4);
            dict["x"] = 0;
            int i = 0;

            RunSimpleBenchmark("Dictionary read(int)", () => { arr[i++ % arrLen] = (int)dict["x"]; });
        }

        [Test]
        public void Read_Int_ByName()
        {
            int arrLen = 1000;
            int[] arr = new int[arrLen];
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;
            int i = 0;

            r.Write("x", 0);

            RunSimpleBenchmark("Read(int) by name", () => { arr[i++ % arrLen] = s.Root.Read<int>("x"); });
        }

        [Test]
        public void Read_Int_ByIndex()
        {
            int arrLen = 1000;
            int[] arr = new int[arrLen];
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;
            int i = 0;

            r.Write("x", 0);

            RunSimpleBenchmark("Read(int) by index", () => { arr[i++ % arrLen] = s.Root.Read<int>(0); });
        }
    }
}
#endif
