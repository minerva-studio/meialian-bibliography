using NUnit.Framework;
using Unity.PerformanceTesting;

namespace Minerva.DataStorage.Tests
{
    [Explicit]
    public class Perf_Rescheme
    {
        [Test, Performance]
        public void Recheme()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            r.Write("x", 0);

            Measure.Method(() =>
            {
                r.Container.ReschemeFor<long>("longField");
            })
                .SampleGroup(new SampleGroup("ReschemeFor()", SampleUnit.Millisecond))
                .WarmupCount(20)
                .MeasurementCount(50)
                .IterationsPerMeasurement(100)
                .GC()
                .Run();
        }

        [Test, Performance]
        public void Recheme_Old()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var r = s.Root;

            r.Write("x", 0);

            Measure.Method(() =>
            {
                r.Container.ReschemeFor_Old<long>("longField");
            })
                .SampleGroup(new SampleGroup("ReschemeFor()", SampleUnit.Millisecond))
                .WarmupCount(20)
                .MeasurementCount(50)
                .IterationsPerMeasurement(100)
                .GC()
                .Run();
        }
    }
}
