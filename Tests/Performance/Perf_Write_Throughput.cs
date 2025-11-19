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
#endif
