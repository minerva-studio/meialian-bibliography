using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Minerva.DataStorage
{
    internal partial class Container
    {
        internal class Registry
        {
            private static readonly ObjectPool<Container> pool = new ObjectPool<Container>(() => new Container());

            public static class ID
            {
                public const ulong Empty = 0UL;
                public const ulong FirstUser = 1UL;
                public const ulong Wild = ulong.MaxValue;
            }

            public static Registry Shared { get; } = new Registry();
            public static int PoolCount => pool.Count;

            private ulong _next = ID.FirstUser; // 0 reserved for "null", 1 reserved for "empty"
            private readonly Queue<ulong> _freed = new();
            private readonly object _lock = new();
            private readonly Dictionary<ulong, Container> _table = new();
            private readonly Dictionary<ulong, ParentLink> _parents = new();

            private readonly struct ParentLink
            {
                public readonly ulong ParentId;
                public readonly string Segment;

                public ParentLink(ulong parentId, string segment)
                {
                    ParentId = parentId;
                    Segment = segment;
                }
            }

            public void RegisterParent(Container child, Container parent, ReadOnlySpan<char> segment, int? index = null)
            {
                if (child == null || parent == null)
                    return;

                var seg = BuildSegment(segment, index);
                lock (_lock)
                {
                    _parents[child.ID] = new ParentLink(parent.ID, seg);
                }
            }

            private static string BuildSegment(ReadOnlySpan<char> segment, int? index)
            {
                var name = segment.ToString();
                if (index.HasValue)
                    return $"{name}[{index.Value}]";
                return name;
            }

            public Container GetParent(Container child)
            {
                if (child == null) return null;
                lock (_lock)
                {
                    if (_parents.TryGetValue(child.ID, out var link))
                    {
                        return _table.GetValueOrDefault(link.ParentId);
                    }
                }
                return null;
            }

            public bool TryGetParentLink(Container child, out Container parent, out string segment)
            {
                parent = null;
                segment = null;
                if (child == null) return false;

                lock (_lock)
                {
                    if (_parents.TryGetValue(child.ID, out var link))
                    {
                        parent = _table.GetValueOrDefault(link.ParentId);
                        segment = link.Segment;
                        return parent != null && segment != null;
                    }
                }
                return false;
            }


            public void Register(Container container)
            {
                if (container is null) throw new ArgumentNullException(nameof(container));
                lock (_lock)
                {
                    if (container.ID != ID.Wild)
                        throw new InvalidOperationException("Container already registered.");

                    Assign_NoLock(container);
                }
            }

            public void Unregister(ref ContainerReference idRef)
            {
                // already unregistered or is null
                if (idRef == 0UL) return;

                var container = GetContainer(idRef);
                if (container != null)
                {
                    Unregister(container);
                }
                idRef = 0UL; // mark as unregistered
            }

            public void Unregister(Container container)
            {
                if (container is null) return;
                if (container._id == ID.Wild || container._id == 0UL) return;

                var descendants = new List<Container>();
                CollectDescendants(container, descendants);
                
                // Capture parent links for notification before they are removed
                var parentNotifications = new List<(Container Parent, string Segment)>(descendants.Count);
                foreach (var child in descendants)
                {
                    if (TryGetParentLink(child, out var parent, out var segment) && parent != null && !string.IsNullOrEmpty(segment))
                    {
                        parentNotifications.Add((parent, segment));
                    }
                }

                lock (_lock)
                {
                    foreach (var c in descendants)
                    {
                        ulong id = c._id;
                        // Removing parent link immediately prevents stale state if unregister crashes later
                        _parents.Remove(id);

                        if (_table.Remove(id))
                        {
                            _freed.Enqueue(id);
                        }
                        c._id = 0UL; // mark as unregistered
                    }
                }

                // Dispose first
                foreach (var c in descendants)
                {
                    c.Dispose();
                }

                // Notify all descendants
                foreach (var c in descendants)
                {
                    // NOTE: c is Disposed. Accessing c.Memory will throw.
                    // Subscribers must only check c.ID (which is 0) or reference equality.
                    StorageWriteEventRegistry.Notify(c, null, ValueType.Unknown, isDeleted: true);
                }

                // Notify parents about deleted child fields
                foreach (var (parent, segment) in parentNotifications)
                {
                    // If parent is also being deleted (part of descendants), its ID is now 0.
                    // We skip notifying it about child deletion to avoid double notifications (it already got self-deletion).
                    if (parent.ID == 0) continue;

                    StorageWriteEventRegistry.Notify(parent, segment, ValueType.Unknown, isDeleted: true);
                }
                
                // Finally return to pool
                foreach (var c in descendants)
                {
                    pool.Return(c);
                }
            }

            private void CollectDescendants(Container root, List<Container> list)
            {
                CollectDescendants(root, list, new HashSet<ulong>());
            }

            private void CollectDescendants(Container root, List<Container> list, HashSet<ulong> visited)
            {
                if (root == null || root.ID == ID.Wild || root.ID == 0UL) return;
                if (!visited.Add(root.ID)) return;

                // Depth-first post-order traversal
                // 1. Children
                for (int i = 0; i < root.FieldCount; i++)
                {
                    ref var field = ref root.GetFieldHeader(i);
                    if (!field.IsRef) continue;

                    var ids = root.GetRefSpan(in field);
                    for (int k = 0; k < ids.Length; k++)
                    {
                        ulong cid = ids[k];
                        if (cid == 0UL) continue;
                        
                        // Careful: GetContainer locks. We should minimize locking or lock inside.
                        // But this is recursive.
                        var child = GetContainer(cid);
                        if (child != null)
                        {
                            CollectDescendants(child, list, visited);
                        }
                    }
                }
                // 2. Self
                list.Add(root);
            }


            public Container GetContainer(ContainerReference id)
            {
                if (id == 0UL) return null;
                lock (_lock) return _table.GetValueOrDefault(id);
            }


            public ContainerReference AssignNewID(Container container)
            {
                if (container is null) throw new ArgumentNullException(nameof(container));
                lock (_lock)
                {
                    Assign_NoLock(container);
                    return container.ID;
                }
            }

            private void Assign_NoLock(Container container)
            {
                container._disposed = false;
                ulong id = Next_NoLock();
                container._id = id;
                _table[id] = container;
            }

            private ulong Next_NoLock()
            {
                if (_freed.Count > 0) return _freed.Dequeue();
                return _next++;
            }




            #region Create

            public Container CreateAt(ref ContainerReference position) => CreateAt(ref position, ContainerLayout.Empty);

            public Container CreateAt(ref ContainerReference position, ContainerLayout layout)
            {
                // 1) If an old tracked container exists in the slot, unregister it first.
                var old = GetContainer(position);
                if (old != null)
                    Unregister(old);

                // 2) Create a new container and register it (assign a unique tracked ID).
                var created = CreateWild(layout, true);
                Register(created);

                // 3) Bind atomically: write ID into the slot.
                position = created.ID;
                return created;
            }








            /// <summary>
            /// Create a wild container, which means that container is not tracked by anything
            /// </summary>
            /// <param name="position"></param>
            /// <param name="schema"></param>
            /// <exception cref="ArgumentNullException"></exception>
            public Container CreateWildWith(in AllocatedMemory data)
            {
                var container = pool.Rent();
                container.Initialize(data);
                container._id = ID.Wild;
                return container;
            }




            /// <summary>
            /// Create a wild container, which means that container is not tracked by anything
            /// </summary>
            /// <param name="position"></param>
            /// <param name="schema"></param> 
            public Container CreateWild() => CreateWild(ContainerHeader.Size);

            /// <summary>
            /// Create a wild container, which means that container is not tracked by anything
            /// </summary>
            /// <param name="position"></param>
            /// <param name="schema"></param> 
            public Container CreateWild(int size)
            {
                var container = pool.Rent();
                container.Initialize(size);
                container._id = ID.Wild;
                return container;
            }


            /// <summary>
            /// Create a wild container, which means that container is not tracked by anything
            /// </summary>
            /// <param name="position"></param>
            /// <param name="schema"></param>
            /// <exception cref="ArgumentNullException"></exception>
            public Container CreateWild(ReadOnlySpan<byte> span)
            {
                var container = CreateWild(span.Length);
                span.CopyTo(container.Span);
                return container;
            }

            public Container CreateWild(ContainerLayout layout) => CreateWild(layout, true);
            public Container CreateWild(ContainerLayout layout, bool zero)
            {
                var container = CreateWild(layout.TotalLength);
                layout.Span.CopyTo(container.Span);
                // clear data segment
                if (zero) container.DataSegment.Clear();
                return container;
            }

            public void Return(Container container)
            {
                container.Dispose();
                pool.Return(container);
            }
            #endregion


#if UNITY_EDITOR
            /// <summary>
            /// Debug-only helper used by editor tools to take a snapshot of all live containers.
            /// </summary>
            internal void DebugCopyLiveContainers(Dictionary<ulong, Container> target)
            {
                if (target == null) throw new ArgumentNullException(nameof(target));

                lock (_lock)
                {
                    target.Clear();
                    foreach (var kv in _table)
                    {
                        // _table: Dictionary<ulong, Container>
                        target.Add(kv.Key, kv.Value);
                    }
                }
            }
#endif
        }
    }

    /// <summary>
    /// A tiny, generic object pool with only the essentials:
    /// - Rent(): get an instance (create via factory if empty)
    /// - Return(): give it back (optionally reset, optionally cap size)
    /// - Count: how many are currently cached
    /// </summary>
    public sealed class ObjectPool<T>
    {
        private readonly ConcurrentStack<T> _stack;
        private readonly Func<T> _factory;
        private readonly Action<T> _reset;
        private readonly int _maxSize; // 0 or negative => unbounded
        private readonly object _lock = new object();

        /// <summary>
        /// Create a pool.
        /// </summary>
        /// <param name="factory">How to create a new instance when the pool is empty.</param>
        /// <param name="reset">
        /// Optional reset action invoked when an instance is returned to the pool
        /// (e.g., clear lists, zero fields).
        /// </param>
        /// <param name="initialCapacity">Initial underlying stack capacity (optional).</param>
        /// <param name="maxSize">
        /// Optional cap for the number of cached instances (0 or negative = unbounded).
        /// Returned instances beyond this cap are simply dropped.
        /// </param>
        public ObjectPool(Func<T> factory, Action<T> reset = null, int maxSize = 0)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _reset = reset;
            _stack = new();
            _maxSize = maxSize;
        }

        /// <summary>Current cached count inside the pool.</summary>
        public int Count => _stack.Count;

        /// <summary>
        /// Get an instance from the pool or create a new one via factory when empty.
        /// </summary>
        public T Rent()
        {
            if (_stack.TryPop(out var t))
            {
                return t;
            }
            return _factory();
        }

        /// <summary>
        /// Return an instance to the pool. If maxSize is set and reached, the instance is dropped.
        /// </summary>
        public void Return(T instance)
        {
            if (instance == null) return;
            _reset?.Invoke(instance);

            if (_maxSize > 0 && _stack.Count >= _maxSize)
                return;

            lock (_lock)
            {
                if (_maxSize <= 0 || _stack.Count < _maxSize)
                {
                    _stack.Push(instance);
                }
            }
        }

        /// <summary>
        /// Clears all cached instances (they become eligible for GC).
        /// </summary>
        public void Clear() => _stack.Clear();
    }

}
