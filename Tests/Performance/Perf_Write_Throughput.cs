using NUnit.Framework;
using System.Collections.Generic;
using Unity.PerformanceTesting;

namespace Amlos.Container.Tests
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
            var dict = new Dictionary<string, int>(capacity: 4);
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

        //// -- B. 促升后 steady：int→float（或直接 double） --
        //[Test, Performance]
        //public void Write_IntThenFloat_Promotion_SteadyFloat()
        //{
        //    using var s = new Storage(ContainerLayout.Empty);
        //    var r = s.Root;

        //    r.Write("n", 1);
        //    r.Write("n", 1.0f);

        //    Measure.Method(() =>
        //    {
        //        s.Root.Write("n", 1.25f);
        //    })
        //        .SampleGroup(new SampleGroup("Write(float) after promotion", SampleUnit.Microsecond))
        //        .WarmupCount(20)
        //        .MeasurementCount(10)
        //        .IterationsPerMeasurement(100)
        //        .GC()
        //        .Run();
        //}

        //// -- C. 混合负载：数字写/读 + 字符串（UTF-16外联）+ 子对象 --
        //[Test, Performance]
        //public void Mixed_Workload_WriteRead_Object_String()
        //{
        //    using var s = new Storage(ContainerLayout.Empty);
        //    var rnd = new Random(42);

        //    // 预热：建立必要字段/对象，避免测量期建树
        //    s.Root.Write("n", 0.0);              // numeric
        //    s.Root.Write("s", "seed");           // string 外联
        //    var child = s.Root.GetObject("o");   // object
        //    child.Write("k", 0);

        //    // Body：一次迭代内做少量混合操作（多次迭代后取均值）
        //    Measure.Method(() =>
        //    {
        //        var r = s.Root;
        //        double p = rnd.NextDouble();
        //        if (p < 0.60)
        //        {
        //            r.Write("n", rnd.NextDouble()); // 60% 数值写
        //        }
        //        else if (p < 0.80)
        //        {
        //            _ = r.Read<double>("n");        // 20% 数值读
        //        }
        //        else if (p < 0.90)
        //        {
        //            // 10% 字符串外联：大/小交替制造压力，父容器不应迁移
        //            r.Write("s", ((rnd.Next() & 1) == 0) ? new string('x', 4096) : "x");
        //        }
        //        else
        //        {
        //            // 10% 子对象轻写
        //            var c = r.GetObject("o");
        //            c.Write("k", rnd.Next());
        //        }
        //    })
        //        .SampleGroup(new SampleGroup("Mixed workload (per op)", SampleUnit.Microsecond))
        //        .WarmupCount(20)
        //        .MeasurementCount(10)
        //        .IterationsPerMeasurement(100)
        //        .GC()
        //        .Run();
        //}
    }
}
