using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Minerva.DataStorage.Tests
{
    [Timeout(15000)] // 15s 
    [TestFixture]
    public class MemorySafetyTests
    {
        private static void SpinGCSoft()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
        /// <summary>
        /// Make GC + wait a bit to let finalizers (if any) run.
        /// </summary>
        private static void SpinGC(int sleepMs = 10)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Thread.Sleep(sleepMs);
        }


        /// <summary>
        /// No public API surfaces should expose the internal Container type:
        /// no public return type / parameter type / field / property of type Container.
        /// </summary>
        [Test]
        public void ApiSurface_DoesNotExpose_Container()
        {
            var asm = typeof(Storage).Assembly;
            var containerType = asm.GetType("Minerva.DataStorage.Container", throwOnError: true)!;

            // Exported public types in the assembly
            foreach (var t in asm.GetExportedTypes())
            {
                // Public methods: return/parameter
                foreach (var m in t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
                {
                    if (m.ReturnType == containerType)
                        Assert.Fail($"Public method returns Container: {t.FullName}.{m.Name}");

                    foreach (var p in m.GetParameters())
                        if (p.ParameterType == containerType)
                            Assert.Fail($"Public method parameter is Container: {t.FullName}.{m.Name}({p.Name})");
                }

                // Public properties
                foreach (var p in t.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
                    if (p.PropertyType == containerType)
                        Assert.Fail($"Public property exposes Container: {t.FullName}.{p.Name}");

                // Public fields
                foreach (var f in t.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
                    if (f.FieldType == containerType)
                        Assert.Fail($"Public field exposes Container: {t.FullName}.{f.Name}");
            }

            Assert.Pass("No public API exposing Container found.");
        }


        /// <summary>
        /// Under steady-size create/dispose loop, the number of unique byte[] buffers observed
        /// should plateau instead of growing linearly (indicating pool reuse).
        /// </summary>
        [Test]
        public void ArrayPool_Stable_Reuses_Buffers_In_SteadyState()
        {
            // Warm-up: allocate and dispose many times at similar size.
            var uniqueBuffers = new HashSet<byte[]>(ReferenceEqualityComparer<byte[]>.Instance);

            // Choose a stable access pattern that forces container to allocate a moderate buffer.
            const int warmup = 500;
            for (int i = 0; i < warmup; i++)
            {
                using var s = new Storage(ContainerLayout.Empty);
                var root = s.Root;

                // Write several fields to force a non-trivial buffer
                root.Write("a", 123);
                root.Write("b", 456.0f);
                root.Write("c", "hello");

                var inner = root.Container;
                var buf = inner.Memory.Array;
                uniqueBuffers.Add(buf);
            }

            // Snapshot after warm-up
            int uniqueAfterWarmup = uniqueBuffers.Count;

            // Steady phase: much more iterations; we expect unique buffer count increases very slowly or plateaus.
            const int loops = 4000;
            for (int i = 0; i < loops; i++)
            {
                using var s = new Storage(ContainerLayout.Empty);
                var root = s.Root;

                root.Write("x", i);
                root.Write("y", i * 0.5);
                root.Write("z", $"k{i}");

                var inner = root.Container;
                var buf = inner.Memory.Array;
                uniqueBuffers.Add(buf);
            }

            SpinGC();

            int uniqueTotal = uniqueBuffers.Count;
            int delta = uniqueTotal - uniqueAfterWarmup;

            // Heuristic threshold: in a healthy pool we should see reuse quickly.
            // Platform/CLR differences exist, so choose a lenient cap.
            Assert.That(uniqueAfterWarmup, Is.GreaterThan(0), "Warm-up should have seen some buffers.");
            Assert.That(delta, Is.LessThanOrEqualTo(64),
                $"Unique buffers grew too much in steady state (delta={delta}). Pool may not be reusing arrays.");
        }


        /// <summary>
        /// Two live storages should not share the same container instance or id.
        /// After disposing A, its containers must be unregistered and not accessible to B.
        /// </summary>
        [Test]
        public void No_Cross_Contamination_Between_Storages()
        {
            var reg = Container.Registry.Shared;

            using var a = new Storage(ContainerLayout.Empty);
            using var b = new Storage(ContainerLayout.Empty);

            var aRoot = a.Root;
            var bRoot = b.Root;

            // Create children to ensure multiple containers per storage
            var aChild = aRoot.GetObject("child");
            var bChild = bRoot.GetObject("child");

            // IDs must be distinct while both alive
            Assert.That(aRoot.ID, Is.Not.EqualTo(bRoot.ID));
            Assert.That(aChild.ID, Is.Not.EqualTo(bChild.ID));

            // Registry resolves each id to a single container; they must not collide
            var aRootC = reg.GetContainer(aRoot.ID);
            var bRootC = reg.GetContainer(bRoot.ID);
            var aChildC = reg.GetContainer(aChild.ID);
            var bChildC = reg.GetContainer(bChild.ID);

            Assert.NotNull(aRootC);
            Assert.NotNull(bRootC);
            Assert.NotNull(aChildC);
            Assert.NotNull(bChildC);

            Assert.False(ReferenceEquals(aRootC, bRootC), "Two storages must not share same container instance.");
            Assert.False(ReferenceEquals(aChildC, bChildC), "Two storages' children must not share instance.");

            // Dispose A -> its containers must be unregistered and unreachable from Registry
            var aRootId = aRoot.ID;
            var aChildId = aChild.ID;

            a.Dispose();
            SpinGC();

            Assert.IsNull(reg.GetContainer(aRootId), "A root must be unregistered after dispose.");
            Assert.IsNull(reg.GetContainer(aChildId), "A child must be unregistered after dispose.");

            // B remains intact
            Assert.NotNull(reg.GetContainer(bRoot.ID));
            Assert.NotNull(reg.GetContainer(bChild.ID));
        }


        /// <summary>
        /// At any moment, a container id maps to exactly one live container owned by one storage.
        /// There should be no duplicate ids among concurrently live storages.
        /// </summary>
        [Test]
        public void No_Double_Ownership_Same_Id_Not_Shared()
        {
            var reg = Container.Registry.Shared;

            using var s1 = new Storage(ContainerLayout.Empty);
            using var s2 = new Storage(ContainerLayout.Empty);

            var ids = new HashSet<ulong>();

            // Probe a few objects per storage
            var s1Root = s1.Root; ids.Add(s1Root.ID);
            var s2Root = s2.Root; ids.Add(s2Root.ID);

            var s1c0 = s1Root.GetObject("c0"); ids.Add(s1c0.ID);
            var s2c0 = s2Root.GetObject("c0"); ids.Add(s2c0.ID);

            // Ensure no duplicate ids across two live storages
            Assert.That(ids.Count, Is.EqualTo(4), "Duplicate container IDs found across live storages.");

            // Each id must resolve to a non-null, unique container instance
            var resolved = ids.Select(id => reg.GetContainer(id)).ToArray();
            Assert.That(resolved.All(x => x != null), Is.True);

            // Instance uniqueness (dict makes id��instance unique)
            Assert.That(resolved.Distinct(ReferenceEqualityComparer<object>.Instance).Count(),
                        Is.EqualTo(resolved.Length),
                        "Two different ids resolved to same container instance unexpectedly.");
        }



        /// <summary>
        /// Reschema / repeated writes that grow and shrink should still keep unique buffers bounded.
        /// </summary>
        [Test]
        public void Migration_Rewrites_Do_Not_Leak_Buffers()
        {
            var uniqueBuffers = new HashSet<byte[]>(ReferenceEqualityComparer<byte[]>.Instance);

            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            // Repeatedly grow/shrink payload to force buffer replacements inside container
            for (int i = 0; i < 2000; i++)
            {
                // Grow
                root.Write("blob", new string('X', 1024 + (i % 5) * 2048));
                // Shrink
                root.Write("blob", "tiny");
                // Touch another field to vary layout
                root.Write("n", i);

                var inner = root.Container;
                var buf = inner.Memory.Array;
                uniqueBuffers.Add(buf);
            }

            // If migration kept returning old buffers to the pool, unique buffers should stay modest.
            Assert.That(uniqueBuffers.Count, Is.LessThanOrEqualTo(128),
                $"Too many unique buffers observed during migration: {uniqueBuffers.Count}");
        }

        // =====================================================================
        // 5) Parallel plateau: create/dispose in many threads; unique buffers should plateau
        // =====================================================================

        /// <summary>
        /// Parallel create/dispose with steady payload should show plateau in the number
        /// of unique byte[] buffers observed concurrently (pool reuse under contention).
        /// </summary> 
        [Test]
        [Timeout(10000)]
        public void ArrayPool_Parallel_Reuse_PerWorkerPlateau()
        {
            var concurrent = Math.Max(2, Environment.ProcessorCount);
            int perWorker = 4000;

            var globalUnique = new ConcurrentDictionary<byte[], byte>(ReferenceEqualityComparer<byte[]>.Instance);

            Parallel.For(0, concurrent, new ParallelOptions { MaxDegreeOfParallelism = concurrent }, _ =>
            {
                using var s = new Storage(ContainerLayout.Empty);
                var root = s.Root;

                var localUnique = new HashSet<byte[]>(ReferenceEqualityComparer<byte[]>.Instance);

                for (int i = 0; i < perWorker; i++)
                {
                    root.Write("i", i);
                    root.Write("f", i * 0.25f);
                    root.Write("s", "abcdefg");

                    if ((i & 31) == 0)
                    {
                        var buf = root.Memory.Array;
                        localUnique.Add(buf);
                    }
                }

                foreach (var b in localUnique) globalUnique.TryAdd(b, 0);

                Assert.That(localUnique.Count, Is.LessThanOrEqualTo(16),
                    $"Per-thread unique buffers too many: {localUnique.Count}");
            });

            SpinGCSoft();

            var bound = concurrent * 32;
            Assert.That(globalUnique.Count, Is.LessThanOrEqualTo(bound),
                $"Global unique buffers too many: {globalUnique.Count}, bound={bound}");
        }

        [Test]
        public void ArrayPool_Windowed_Batch_CreateDispose_Plateau()
        {
            int window = Math.Max(2, Environment.ProcessorCount);
            int rounds = 200;
            var uniquePerWindow = new List<int>(rounds);

            for (int r = 0; r < rounds; r++)
            {
                var storages = new List<Storage>(window);
                var unique = new HashSet<byte[]>(ReferenceEqualityComparer<byte[]>.Instance);

                for (int i = 0; i < window; i++)
                {
                    var s = new Storage(ContainerLayout.Empty);
                    storages.Add(s);

                    var root = s.Root;
                    root.Write("i", i);
                    root.Write("f", i * 0.5f);
                    root.Write("s", "abcdefg");

                    unique.Add(root.Container.Memory.Array);
                }

                uniquePerWindow.Add(unique.Count);

                foreach (var s in storages) s.Dispose();
                storages.Clear();

                if ((r & 7) == 0) GC.Collect(2, GCCollectionMode.Forced, blocking: false, compacting: false);
            }

            var lastHalf = uniquePerWindow.Skip(uniquePerWindow.Count / 2).ToArray();
            var maxLast = lastHalf.Max();
            Assert.That(maxLast, Is.LessThanOrEqualTo(window * 2),
                $"Per-window unique buffers too many in steady rounds: {maxLast}, window={window}");
        }



        // =====================================================================
        // 6) Registry consistency under parallel creation (no duplicate IDs while both alive)
        // =====================================================================

        /// <summary>
        /// Create many storages in parallel and hold them alive; all container IDs must be unique at the same time.
        /// </summary>
        [Test]
        [Timeout(10000)]
        public void Registry_UniqueIds_WhileAlive_UnderParallel()
        {
            var reg = Container.Registry.Shared;
            var storages = new List<Storage>();
            var ids = new ConcurrentDictionary<ulong, byte>();

            int total = Math.Min(512, 64 * Environment.ProcessorCount);

            // Create and hold
            Parallel.For(0, total, i =>
            {
                var s = new Storage(ContainerLayout.Empty);
                lock (storages) storages.Add(s);
                var r = s.Root;

                // root + one child
                ids.TryAdd(r.ID, 0);
                var c = r.GetObject("child");
                if (!ids.TryAdd(c.ID, 0))
                    Assert.Fail($"Duplicate ID detected at creation time: {c.ID}");
            });

            // While all alive, every id should resolve to a non-null distinct instance
            var arr = ids.Keys.ToArray();
            Assert.That(arr.Length, Is.EqualTo(arr.Distinct().Count()), "ID collision detected");
            var resolved = arr.Select(id => reg.GetContainer(id)).ToArray();
            Assert.That(resolved.All(x => x != null), Is.True);

            // Tear down
            foreach (var s in storages) s.Dispose();
            storages.Clear();

            SpinGCSoft();

            // After teardown, none of those ids should resolve
            foreach (var id in arr)
                Assert.IsNull(reg.GetContainer(id), $"ID {id} still registered after disposal");
        }

        // =====================================================================
        // 7) Fuzzing: random write patterns (ints/floats/bools/strings/objects/arrays)
        // =====================================================================

        /// <summary>
        /// Randomized fuzz of writes/overwrites/reschema; pool should still reuse,
        /// registry should not leak, and live buffers should not explode.
        /// </summary>
        [Test]
        public void Fuzz_Randomized_Writes_DoNotLeak_TypeStablePerField()
        {
            var rnd = new System.Random(42);
            var uniqueParent = new HashSet<byte[]>(ReferenceEqualityComparer<byte[]>.Instance);

            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            // Partition field names by allowed type families.
            var numNames = new[] { "a", "b", "c" };        // numeric family: int/float/double
            var boolNames = new[] { "d" };                 // bool only
            var strNames = new[] { "e", "f" };             // string only
            var objNames = new[] { "g", "h" };             // object only

            for (int i = 0; i < 5000; i++)
            {
                switch (rnd.Next(4))
                {
                    case 0: // numeric family (type can vary within numeric)
                        {
                            var n = numNames[rnd.Next(numNames.Length)];
                            switch (rnd.Next(3))
                            {
                                case 0: root.Write(n, rnd.Next()); break;                    // int
                                case 1: root.Write(n, (float)rnd.NextDouble()); break;       // float
                                case 2: root.Write(n, rnd.NextDouble()); break;              // double
                            }
                            break;
                        }
                    case 1: // bool
                        {
                            var n = boolNames[rnd.Next(boolNames.Length)];
                            root.Write(n, rnd.Next(2) == 0);
                            break;
                        }
                    case 2: // string (externalized): random size to churn leaf buffers
                        {
                            var n = strNames[rnd.Next(strNames.Length)];
                            int len = rnd.Next(0, 4096);
                            root.Write(n, new string('x', len)); // ��� Write(string,string) ��ӳ�䵽 WriteString
                            break;
                        }
                    case 3: // object leaf: ensure children materialize
                        {
                            var n = objNames[rnd.Next(objNames.Length)];
                            var child = root.GetObject(n);
                            // �ᴥ�Ӷ��������д��
                            if ((i & 3) == 0) child.Write("k", i);
                            break;
                        }
                }

                // periodically snapshot parent buffer identity
                if ((i & 31) == 0)
                    uniqueParent.Add(root.Memory.Array);
            }

            Assert.That(uniqueParent.Count, Is.LessThanOrEqualTo(256),
                $"Unique parent buffers too many: {uniqueParent.Count}");
        }


        // =====================================================================
        // 8) Finalizer path: drop without Dispose; registry should still clean up
        // =====================================================================

        /// <summary>
        /// When Storage is not disposed explicitly, finalizer path should unregister containers.
        /// </summary>
        [Test]
        public void Finalizer_Unregisters_Ids_EvenIfPooled()
        {
            var reg = Container.Registry.Shared;
            ulong rootId = 0;
            ulong c0Id = 0, c1Id = 0;

            void CreateAndAbandon()
            {
                var s = new Storage(ContainerLayout.Empty);
                var root = s.Root;
                var c0 = root.GetObject("c0");
                var c1 = root.GetObject("c1");

                rootId = root.ID;
                c0Id = c0.ID;
                c1Id = c1.ID;

                // No dispose and wait for gc
            }

            CreateAndAbandon();

            var sw = System.Diagnostics.Stopwatch.StartNew();
            bool rootGone = false, c0Gone = false, c1Gone = false;

            const int timeoutMs = 2000;
            while (sw.ElapsedMilliseconds < timeoutMs && !(rootGone && c0Gone && c1Gone))
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                rootGone = reg.GetContainer(rootId) is null;
                c0Gone = reg.GetContainer(c0Id) is null;
                c1Gone = reg.GetContainer(c1Id) is null;

                if (!(rootGone && c0Gone && c1Gone))
                    Thread.Sleep(10);
            }

            Assert.IsTrue(rootGone, "Root ID not unregistered after finalizer/cleanup.");
            Assert.IsTrue(c0Gone, "Child c0 ID not unregistered after finalizer/cleanup.");
            Assert.IsTrue(c1Gone, "Child c1 ID not unregistered after finalizer/cleanup.");
        }


        // =====================================================================
        // 9) No buffer aliasing between siblings (live)
        // =====================================================================

        /// <summary>
        /// Two live child objects within the same storage should not share the same backing byte[] buffer.
        /// </summary>
        [Test]
        public void Sibling_Containers_DoNotShare_Buffers_Live()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var c0 = root.GetObject("child0");
            var c1 = root.GetObject("child1");

            // Touch payloads to ensure both allocate
            c0.Write("x", 1);
            c1.Write("y", 2);

            var b0 = c0.Memory.Array;
            var b1 = c1.Memory.Array;

            Assert.False(ReferenceEquals(b0, b1), "Siblings unexpectedly share the same byte[] buffer while both alive");
        }

        // =====================================================================
        // 10) Cross-thread registry reads after dispose must be safe (no resurrection)
        // =====================================================================

        /// <summary>
        /// After disposing a storage, concurrent registry lookups of its ids must keep returning null.
        /// </summary>
        [Test]
        [Timeout(10000)]
        public void AfterDispose_RegistryLookup_IsStable_Null()
        {
            var reg = Container.Registry.Shared;
            ulong[] ids;

            using (var s = new Storage(ContainerLayout.Empty))
            {
                var r = s.Root;
                var cA = r.GetObject("A");
                var cB = r.GetObject("B");
                var cC = r.GetObject("C");
                ids = new[] { r.ID, cA.ID, cB.ID, cC.ID };
            }

            SpinGCSoft();

            var errors = 0;
            Parallel.For(0, 8 * Environment.ProcessorCount, _ =>
            {
                foreach (var id in ids)
                {
                    if (reg.GetContainer(id) != null)
                        Interlocked.Increment(ref errors);
                }
            });

            Assert.That(errors, Is.EqualTo(0), "Found non-null registry entries after owner disposed");
        }

        // =====================================================================
        // 11) Stress: pool churn with random long/short strings (external leaves)
        // =====================================================================

        /// <summary>
        /// Rapid alternation between tiny and huge strings should not explode parent buffer set.
        /// Strings are externalized; parent stays stable; only leaf churns.
        /// </summary>
        [Test]
        public void StringChurn_ParentBuffer_RemainsStable()
        {
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            var uniqueParent = new HashSet<byte[]>(ReferenceEqualityComparer<byte[]>.Instance);

            for (int i = 0; i < 3000; i++)
            {
                // Huge string then tiny string to same field
                root.Write("S", new string('Y', (i % 2 == 0) ? 64 * 1024 : 1));
                var buf = root.Memory.Array;
                uniqueParent.Add(buf);
            }

            Assert.That(uniqueParent.Count, Is.LessThanOrEqualTo(16),
                $"Parent buffer changed too often under external-string churn: {uniqueParent.Count}");
        }

        // =====================================================================
        // 12) Long-run soak: mixed workloads, ensure plateau (optional slow)
        // =====================================================================

        /// <summary>
        /// Soak test with mixed operations for a longer period; adjust Category to skip in quick runs.
        /// </summary>
        [Test, Category("Soak")]
        public void Soak_Mixed_Workloads_Plateau()
        {
            var unique = new HashSet<byte[]>(ReferenceEqualityComparer<byte[]>.Instance);

            for (int round = 0; round < 200; round++)
            {
                using var s = new Storage(ContainerLayout.Empty);
                var r = s.Root;

                // Build a small tree
                var a = r.GetObject("a");
                var b = r.GetObject("b");
                var c = r.GetObject("c");

                // write different shapes
                a.Write("i", round);
                b.Write("f", round * 0.1f);
                c.Write("s", new string('z', (round % 7) * 3000)); // externalized

                // vary keys to force layout work
                r.Write($"k{round % 5}", round * 7);

                unique.Add(r.Memory.Array);
                unique.Add(a.Memory.Array);
                unique.Add(b.Memory.Array);
                unique.Add(c.Memory.Array);
            }

            SpinGCSoft();
            Assert.That(unique.Count, Is.LessThanOrEqualTo(512),
                $"Soak observed too many unique buffers: {unique.Count}");
        }

        /// <summary>
        /// Fuzz round-trip correctness: after randomized writes (type-stable per field),
        /// values read back must match the oracle (with small numeric tolerance).
        /// </summary>
        [Test]
        public void Fuzz_RoundTrip_NoDataLoss_TypeStablePerField()
        {
            var rnd = new Random(123);

            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            // Field groups (type-stable)
            var numNames = new[] { "a", "b", "c" };  // numeric family
            var boolNames = new[] { "d" };            // bool only
            var strNames = new[] { "e", "f" };       // string (externalized)
            var objNames = new[] { "g", "h" };       // object only

            // Oracle (shadow model)
            var expectNum = new Dictionary<string, double>(); // always store as double
            var expectBool = new Dictionary<string, bool>();
            var expectStr = new Dictionary<string, string>();
            var expectObjK = new Dictionary<string, int>();    // child.<k> last int

            // Numeric write helper: update oracle & container
            void WriteNumeric(StorageObject root, string name)
            {
                switch (rnd.Next(3))
                {
                    case 0:
                        {
                            int v = rnd.Next(int.MinValue / 2, int.MaxValue / 2);
                            root.Write(name, v);
                            expectNum[name] = (double)v;
                            break;
                        }
                    case 1:
                        {
                            float v = (float)(rnd.NextDouble() * 1e6 - 5e5);
                            root.Write(name, v);
                            expectNum[name] = (double)v; // stored as double per promotion rule
                            break;
                        }
                    case 2:
                        {
                            double v = rnd.NextDouble() * 1e12 - 5e11;
                            root.Write(name, v);
                            expectNum[name] = v;
                            break;
                        }
                }
            }

            // Bool write helper
            void WriteBool(StorageObject root, string name)
            {
                bool v = (rnd.Next(2) == 0);
                root.Write(name, v);
                expectBool[name] = v;
            }

            // String write helper
            void WriteStringField(StorageObject root, string name)
            {
                int len = rnd.Next(0, 4096);
                string v = new string((char)('a' + rnd.Next(26)), len);
                root.Write(name, v); // mapped to external WriteString 
                expectStr[name] = v;
            }

            // Object write helper: write <k> inside child
            void WriteObjectK(StorageObject root, string name)
            {
                var child = root.GetObject(name);
                int v = rnd.Next(-1_000_000, 1_000_000);
                child.Write("k", v);
                expectObjK[name] = v;
            }

            // Fuzz loop
            for (int i = 1; i <= 5000; i++)
            {
                switch (rnd.Next(4))
                {
                    case 0: WriteNumeric(root, numNames[rnd.Next(numNames.Length)]); break;
                    case 1: WriteBool(root, boolNames[rnd.Next(boolNames.Length)]); break;
                    case 2: WriteStringField(root, strNames[rnd.Next(strNames.Length)]); break;
                    case 3: WriteObjectK(root, objNames[rnd.Next(objNames.Length)]); break;
                }

                // Periodically validate round-trip against oracle
                if ((i & 63) == 0)
                {
                    // Numeric: read as double; compare with tolerance
                    foreach (var kv in expectNum)
                    {
                        double actual = root.Read<double>(kv.Key);
                        AssertNearlyEqual(actual, kv.Value, 1e-9, $"Numeric mismatch at '{kv.Key}'");
                    }

                    // Bool: exact
                    foreach (var kv in expectBool)
                    {
                        bool actual = root.Read<bool>(kv.Key);
                        Assert.That(actual, Is.EqualTo(kv.Value), $"Bool mismatch at '{kv.Key}'");
                    }

                    // String: exact (externalized leaf)
                    foreach (var kv in expectStr)
                    {
                        // assuming you have ReadString; if named differently, adjust here
                        string actual = root.ReadString(kv.Key);
                        Assert.That(actual, Is.EqualTo(kv.Value), $"String mismatch at '{kv.Key}' (len exp={kv.Value.Length}, act={actual.Length})");
                    }

                    // Object child 'k': exact int
                    foreach (var kv in expectObjK)
                    {
                        var child = root.GetObject(kv.Key);
                        int actual = child.Read<int>("k");
                        Assert.That(actual, Is.EqualTo(kv.Value), $"Object '{kv.Key}.k' mismatch");
                    }
                }
            }

            // Final full validation at the end
            foreach (var kv in expectNum)
            {
                double actual = root.Read<double>(kv.Key);
                AssertNearlyEqual(actual, kv.Value, 1e-9, $"Final numeric mismatch at '{kv.Key}'");
            }
            foreach (var kv in expectBool)
            {
                bool actual = root.Read<bool>(kv.Key);
                Assert.That(actual, Is.EqualTo(kv.Value), $"Final bool mismatch at '{kv.Key}'");
            }
            foreach (var kv in expectStr)
            {
                string actual = root.ReadString(kv.Key);
                Assert.That(actual, Is.EqualTo(kv.Value), $"Final string mismatch at '{kv.Key}'");
            }
            foreach (var kv in expectObjK)
            {
                var child = root.GetObject(kv.Key);
                int actual = child.Read<int>("k");
                Assert.That(actual, Is.EqualTo(kv.Value), $"Final object '{kv.Key}.k' mismatch");
            }
        }

        // ULP/relative tolerant compare for doubles
        private static void AssertNearlyEqual(double actual, double expected, double relEps, string message = null)
        {
            if (double.IsNaN(expected) || double.IsInfinity(expected))
            {
                Assert.Fail("Unexpected NaN/Infinity in expected value");
            }
            double tol = relEps * Math.Max(1.0, Math.Abs(expected));
            Assert.That(Math.Abs(actual - expected), Is.LessThanOrEqualTo(tol),
                message ?? $"Expected {expected} ~= {actual} within {tol}");
        }

        [Test]
        public void Child_Delete_Unregisters_Container()
        {
            var reg = Container.Registry.Shared;
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;
            var child = root.GetObject("child");
            ulong childId = child.ID;
            Assert.NotNull(reg.GetContainer(childId), "Child container should be registered after creation.");

            // Delete field hosting child container
            root.Delete("child");
            SpinGCSoft();

            Assert.IsNull(reg.GetContainer(childId), "Child container must be unregistered after Delete().");
        }

        [Test]
        public void ParentMap_MatchesActualReferenceSlots()
        {
            var reg = Container.Registry.Shared;
            using var s = new Storage(ContainerLayout.Empty);
            var root = s.Root;

            // Create several direct children
            var children = new List<StorageObject>();
            for (int i = 0; i < 12; i++)
            {
                children.Add(root.GetObject("child" + i));
            }
            // Create nested child: outer.inner
            var outer = root.GetObject("outer");
            var inner = outer.GetObject("inner");

            // Validate direct children parent mapping and actual presence in root ref slots
            foreach (var child in children)
            {
                var c = reg.GetContainer(child.ID);
                Assert.NotNull(c, "Child container missing from registry.");
                Assert.IsTrue(reg.TryGetParent(c, out var parent), "TryGetParent failed for child.");
                Assert.That(parent.ID, Is.EqualTo(root.ID), "Parent ID mismatch for direct child.");

                // Ensure parent's reference fields actually contain child's ID
                bool foundInParent = false;
                for (int fi = 0; fi < parent.FieldCount && !foundInParent; fi++)
                {
                    ref var fh = ref parent.GetFieldHeader(fi);
                    if (!fh.IsRef) continue;
                    var refs = parent.GetFieldData<ContainerReference>(in fh);
                    for (int k = 0; k < refs.Length; k++)
                    {
                        if (refs[k] == child.ID)
                        {
                            foundInParent = true;
                            break;
                        }
                    }
                }
                Assert.IsTrue(foundInParent, $"Child {child.ID} not found in any parent ref field.");
            }

            // Validate nested child parent mapping (inner -> outer)
            var innerC = reg.GetContainer(inner.ID);
            Assert.NotNull(innerC);
            Assert.IsTrue(reg.TryGetParent(innerC, out var innerParent));
            Assert.That(innerParent.ID, Is.EqualTo(outer.ID), "Nested inner parent ID mismatch.");

            // Ensure outer actually references inner
            bool innerFound = false;
            for (int fi = 0; fi < innerParent.FieldCount && !innerFound; fi++)
            {
                ref var fh = ref innerParent.GetFieldHeader(fi);
                if (!fh.IsRef) continue;
                var refs = innerParent.GetFieldData<ContainerReference>(in fh);
                for (int k = 0; k < refs.Length; k++)
                {
                    if (refs[k] == inner.ID) { innerFound = true; break; }
                }
            }
            Assert.IsTrue(innerFound, "Nested inner container ID not found in outer's ref fields.");

            // Delete a few children and ensure registry no longer returns them
            for (int i = 0; i < 5; i++)
            {
                var child = children[i];
                ulong id = child.ID;
                root.Delete("child" + i);
                SpinGCSoft();
                Assert.IsNull(reg.GetContainer(id), "Deleted child still present in registry.");
            }
        }

        [Test]
        [Timeout(12000)]
        public void No_Cross_Contamination_Between_Storages_ParallelOps()
        {
            var reg = Container.Registry.Shared;
            using var a = new Storage(ContainerLayout.Empty);
            using var b = new Storage(ContainerLayout.Empty);
            var aRoot = a.Root; var bRoot = b.Root;
            ulong aRootId = aRoot.ID; ulong bRootId = bRoot.ID;

            var aChildIds = new ConcurrentBag<ulong>();
            var bChildIds = new ConcurrentBag<ulong>();
            int iterations = 3000;
            int errors = 0;

            void Worker(StorageObject root, ConcurrentBag<ulong> ids, StorageObject otherRoot)
            {
                for (int i = 0; i < iterations; i++)
                {
                    string name = "c" + (i % 7);
                    var child = root.GetObject(name);
                    child.Write("v", i);
                    ids.Add(child.ID);

                    if ((i & 15) == 0)
                    {
                        root.Delete(name);
                    }

                    if (reg.GetContainer(otherRoot.ID) == null)
                        Interlocked.Increment(ref errors);
                }
            }

            Parallel.Invoke(
                () => Worker(aRoot, aChildIds, bRoot),
                () => Worker(bRoot, bChildIds, aRoot)
            );

            SpinGCSoft();

            Assert.That(errors, Is.EqualTo(0));
            var aRootContainer = reg.GetContainer(aRootId);
            var bRootContainer = reg.GetContainer(bRootId);
            Assert.NotNull(aRootContainer);
            Assert.NotNull(bRootContainer);
            Assert.False(ReferenceEquals(aRootContainer, bRootContainer));

            // Validate live children: correct parent AND presence in parent's ref slots
            void ValidateLiveChildren(IEnumerable<ulong> ids)
            {
                foreach (var id in ids.Distinct())
                {
                    var c = reg.GetContainer(id);
                    if (c == null) continue; // deleted
                    Assert.IsTrue(reg.TryGetParent(c, out var parent), $"Child {id} has no parent.");
                    Assert.IsTrue(parent.ID == aRootId || parent.ID == bRootId, $"Child {id} parent {parent.ID} not a root.");
                    bool found = false;
                    for (int fi = 0; fi < parent.FieldCount && !found; fi++)
                    {
                        ref var fh = ref parent.GetFieldHeader(fi);
                        if (!fh.IsRef) continue;
                        var refs = parent.GetFieldData<ContainerReference>(in fh);
                        for (int k = 0; k < refs.Length; k++)
                        {
                            if (refs[k] == id) { found = true; break; }
                        }
                    }
                    Assert.IsTrue(found, $"Live child {id} not found in parent ref slots.");
                }
            }

            ValidateLiveChildren(aChildIds);
            ValidateLiveChildren(bChildIds);
        }
    }
}