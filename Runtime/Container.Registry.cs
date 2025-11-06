using System;
using System.Collections.Generic;

namespace Amlos.Container
{
    internal partial class Container
    {
        private static Container _empty;

        /// <summary>
        /// The empty container instance
        /// </summary>
        public static Container Empty => _empty ??= CreateEmptyHeaderBytes();


        internal class Registry
        {
            private static readonly ObjectPool<Container> pool = new ObjectPool<Container>(() => new Container(), c => Shared.Unregister(c));

            public static class ID
            {
                public const ulong Empty = 0UL;
                public const ulong FirstUser = 1UL;
                public const ulong Wild = ulong.MaxValue;
            }

            public static Registry Shared { get; } = new Registry();

            private ulong _next = ID.FirstUser; // 0 reserved for "null", 1 reserved for "empty"
            private readonly Queue<ulong> _freed = new();
            private readonly object _lock = new();
            private readonly Dictionary<ulong, Container> _table = new();

            public void SetEmpty(Container container)
            {
                if (container is null) throw new ArgumentNullException(nameof(container));
                lock (_lock)
                {
                    container._id = ID.Empty;
                    _table[ID.Empty] = container;
                }
            }

            public void Register(Container container)
            {
                if (container is null) throw new ArgumentNullException(nameof(container));
                lock (_lock)
                {
                    if (container.ID != ID.Wild)
                        throw new InvalidOperationException("Container already registered.");

                    container._disposed = false;
                    ulong id = Next_NoLock();
                    container._id = id;
                    _table[id] = container;
                }
            }

            public void Unregister(ref ulong idRef)
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
                // this is the empty container, do nothing
                if (container == Empty) return;
                // should not happen, but just in case
                if (container is null) return;

                // 1) Remove self from registry and recycle its id under lock.
                ulong id;
                lock (_lock)
                {
                    id = container._id;
                    if (id == 0UL) return;           // already unregistered / never registered
                    if (!_table.Remove(id)) return;   // not found -> treat as done
                    _freed.Enqueue(id);               // recycle id now
                    container._id = 0UL;              // mark as unregistered
                }

                // 2) Without holding the lock, walk each ref field and recurse immediately.
                //    No snapshot, no allocations. Assumes callers do not modify these fields
                for (int i1 = 0; i1 < container.FieldCount; i1++)
                {
                    var field = container.View[i1];
                    if (!field.IsRef) continue;

                    // This Span<ulong> views the parent's buffer. We only read it, not modify.
                    var ids = container.GetRefSpan(i1);
                    for (int i = 0; i < ids.Length; i++)
                    {
                        ulong cid = ids[i];
                        if (cid == 0UL) continue;

                        // Lookup is short critical section inside GetContainer (locks _lock briefly).
                        var child = GetContainer(cid);
                        if (child != null)
                        {
                            // Depth-first direct recursion, still no allocations.
                            Unregister(child);
                        }
                    }
                }

                // 3) Finally, dispose the container to return its pooled byte[] etc.
                container.Dispose();
            }


            public Container GetContainer(ContainerReference id)
            {
                if (id == 0UL) return null;
                lock (_lock) return _table.GetValueOrDefault(id);
            }

            private ulong Next_NoLock()
            {
                if (_freed.Count > 0) return _freed.Dequeue();
                return _next++;
            }




            #region Create

            public Container CreateAt(ref ContainerReference position, int size = 100)
            {
                // 1) If an old tracked container exists in the slot, unregister it first.
                var old = GetContainer(position);
                if (old != null && old.ID != ID.Empty)
                    Unregister(old);

                // 2) Create a new container and register it (assign a unique tracked ID).
                var created = pool.Rent();
                created.Initialize(size);
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
            public Container CreateAt(ref ulong position, Schema_Old schema, ReadOnlySpan<byte> span)
            {
                if (schema is null) throw new ArgumentNullException(nameof(schema));

                // 1) If an old tracked container exists in the slot, unregister it first.
                var old = Registry.Shared.GetContainer(position);
                if (old != null)
                    Registry.Shared.Unregister(old);

                // 2) Create a new container and register it (assign a unique tracked ID).
                var created = new Container(schema, span);
                Registry.Shared.Register(created);

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
            #endregion

        }


        private static Container CreateEmptyHeaderBytes()
        {
            // container with only header bytes
            var container = new Container(ContainerHeader.Size);
            ContainerHeader.WriteEmptyHeader(container._buffer, Registry.ID.Empty, Version);
            Registry.Shared.SetEmpty(container);
            return container;
        }


    }

    /// <summary>
    /// A tiny, generic object pool with only the essentials:
    /// - Rent(): get an instance (create via factory if empty)
    /// - Return(): give it back (optionally reset, optionally cap size)
    /// - Count: how many are currently cached
    /// Notes:
    /// * Not thread-safe by design (keep it simple). Wrap with a lock if needed.
    /// * LIFO via Stack<T> for better cache locality.
    /// </summary>
    public sealed class ObjectPool<T>
    {
        private readonly Stack<T> _stack;
        private readonly Func<T> _factory;
        private readonly Action<T> _reset;
        private readonly int _maxSize; // 0 or negative => unbounded

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
        public ObjectPool(Func<T> factory, Action<T>? reset = null, int initialCapacity = 0, int maxSize = 0)
        {
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
            _reset = reset;
            _stack = initialCapacity > 0 ? new Stack<T>(initialCapacity) : new Stack<T>();
            _maxSize = maxSize;
        }

        /// <summary>Current cached count inside the pool.</summary>
        public int Count => _stack.Count;

        /// <summary>
        /// Get an instance from the pool or create a new one via factory when empty.
        /// </summary>
        public T Rent() => _stack.Count > 0 ? _stack.Pop() : _factory();

        /// <summary>
        /// Return an instance to the pool. If maxSize is set and reached, the instance is dropped.
        /// </summary>
        public void Return(T instance)
        {
            if (instance == null) return;
            _reset?.Invoke(instance);

            if (_maxSize > 0 && _stack.Count >= _maxSize)
                return; // drop excess to keep pool small

            _stack.Push(instance);
        }

        /// <summary>
        /// Clears all cached instances (they become eligible for GC).
        /// </summary>
        public void Clear() => _stack.Clear();
    }

}
