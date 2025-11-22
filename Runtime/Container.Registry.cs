#nullable enable
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
            private readonly Dictionary<ulong, ulong> _parentMap = new();


            internal bool TryGetParent(Container child, out Container? parent)
            {
                parent = null;
                if (child == null) return false;

                lock (_lock)
                {
                    if (_parentMap.TryGetValue(child.ID, out var parentId))
                    {
                        parent = _table.GetValueOrDefault(parentId);
                        return parent != null;
                    }
                }
                return false;
            }


            public void Register(Container child, Container parent)
            {
                ThrowHelper.ThrowIfNull(child, nameof(child));
                ThrowHelper.ThrowIfNull(parent, nameof(parent));
                lock (_lock)
                {
                    if (child.ID != ID.Wild)
                        throw new InvalidOperationException("Container already registered.");

                    Assign_NoLock(child);
                    _parentMap[child.ID] = parent.ID;
                }
            }

            public void RegisterRoot(Container container)
            {
                ThrowHelper.ThrowIfNull(container, nameof(container));
                if (container is null) throw new ArgumentNullException(nameof(container));
                lock (_lock)
                {
                    if (container.ID != ID.Wild)
                        throw new InvalidOperationException("Container already registered.");

                    Assign_NoLock(container);
                }
            }




            public void Unregister(Container container)
            {
                if (container is null) return;
                if (container._id == ID.Wild || container._id == ID.Empty || container._disposed) return;

                try
                {
                    Traverse(container, c =>
                    {
                        c.MarkDispose();
                    });
                }
                catch (StackOverflowException)
                {
                    throw new InvalidOperationException("Cyclic container reference detected during unregister.");
                }

                using TempString str = new TempString(container.Name);
                Traverse(container, c =>
                {
                    // NOTE: c is Disposed. Accessing c.Memory will throw.
                    // Subscribers must only check c.ID (which is 0) or reference equality.
                    StorageEventRegistry.NotifyDispose(c, c.Generation);
                    lock (_lock)
                    {
                        var id = c._id;
                        // Removing parent link immediately prevents stale state if unregister crashes later
                        _parentMap.Remove(id);
                        if (_table.Remove(id))
                            _freed.Enqueue(id);
                        c._id = ID.Empty; // mark as unregistered 
                    }
                    c.Dispose();
                    // return to pool
                    pool.Return(c);
                });
            }

            //public void Unregister(Container container, Container parent, FieldType fieldType)
            //{
            //    if (container is null) return;
            //    if (container._id == ID.Wild || container._id == ID.Empty) return;

            //    try
            //    {
            //        Traverse(container, c =>
            //        {
            //            c.MarkDispose();
            //        });
            //    }
            //    catch (StackOverflowException)
            //    {
            //        throw new InvalidOperationException("Cyclic container reference detected during unregister.");
            //    }

            //    using TempString str = new TempString(container.Name);
            //    Traverse(container, c =>
            //    {
            //        // NOTE: c is Disposed. Accessing c.Memory will throw.
            //        // Subscribers must only check c.ID (which is 0) or reference equality.
            //        StorageEventRegistry.NotifyDispose(c, c.Generation);
            //        lock (_lock)
            //        {
            //            var id = c._id;
            //            // Removing parent link immediately prevents stale state if unregister crashes later
            //            _parentMap.Remove(id);
            //            if (_table.Remove(id))
            //                _freed.Enqueue(id);
            //            c._id = ID.Empty; // mark as unregistered 
            //        }
            //        c.Dispose();
            //        // return to pool
            //        pool.Return(c);
            //    });


            //    // Notify parents about deleted child fields 
            //    if (parent != null && parent.ID != ID.Empty)
            //        StorageEventRegistry.NotifyFieldDelete(parent, str.ToString(), fieldType);
            //}

            public void RegisterParent(Container child, Container parent)
            {
                if (child == null || parent == null)
                    return;

                lock (_lock)
                {
                    _parentMap[child.ID] = parent.ID;
                }
            }



            private void Traverse(Container root, Action<Container> action)
            {
                if (root == null || root.ID == ID.Wild || root.ID == 0UL) return;

                // Depth-first post-order traversal
                // 1. Children
                for (int i = 0; i < root.FieldCount; i++)
                {
                    ref var field = ref root.GetFieldHeader(i);
                    if (!field.IsRef) continue;

                    var ids = root.GetFieldData<ContainerReference>(in field);
                    for (int k = 0; k < ids.Length; k++)
                    {
                        var cid = ids[k];
                        if (cid == ID.Empty) continue;
                        var child = _table.GetValueOrDefault(cid);
                        if (child != null)
                        {
                            Traverse(child, action);
                        }
                    }
                }
                // 2. Self
                action(root);
            }


            public Container? GetContainer(ContainerReference id)
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

            public Container CreateRoot(ref ContainerReference position, ContainerLayout layout)
            {
                // 1) If an old tracked container exists in the slot, unregister it.
                var old = GetContainer(position);

                // 2) Create a new container and register it (assign a unique tracked ID).
                var created = CreateWild(layout, "", true);
                RegisterRoot(created);

                // 3) Bind atomically: write ID into the slot.
                position = created.ID;

                // 4) Unregister old, if needed
                if (old != null)
                    Unregister(old);
                return created;
            }

            public Container CreateAt(ref ContainerReference position, Container parent, ContainerLayout layout, ReadOnlySpan<char> name)
            {
                // 1) If an old tracked container exists in the slot, unregister it.
                var old = GetContainer(position);

                // 2) Create a new container and register it (assign a unique tracked ID).
                var created = CreateWild(layout, name, true);
                Register(created, parent);

                // 3) Bind atomically: write ID into the slot.
                position = created.ID;

                // 4) Unregister old, if needed
                if (old != null)
                    Unregister(old);
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

            public Container CreateWild(ContainerLayout layout, ReadOnlySpan<char> name) => CreateWild(layout, name, true);
            public Container CreateWild(ContainerLayout layout, ReadOnlySpan<char> name, bool zero)
            {
                var container = CreateWild(layout.TotalLength);
                layout.Span.CopyTo(container.Span);
                // clear data segment
                if (zero) container.DataSegment.Clear();
                container.Rename(name);
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


        /// <summary>
        /// A buffer for recording delete/write event
        /// </summary>
        public struct UnregisterBuffer : IDisposable
        {
            private static readonly ObjectPool<List<Entry>> pool = new ObjectPool<List<Entry>>(() => new List<Entry>());

            struct Entry
            {
                public FieldType FieldType;
                public ContainerReference Reference;
                public bool IsDeleted;

                public Entry(ContainerReference r, FieldType fieldType, bool isFieldDeleted) : this()
                {
                    this.Reference = r;
                    this.FieldType = fieldType;
                    this.IsDeleted = isFieldDeleted;
                }

                public Entry(string name, FieldType fieldType) : this()
                {
                    this.FieldType = fieldType;
                    this.IsDeleted = true;
                }
            }

            readonly Container parent;
            List<Entry>? buffer;

            public readonly bool IsDisposed => buffer != null;

            private UnregisterBuffer(Container parent, List<Entry> entries)
            {
                this.parent = parent;
                this.buffer = entries;
                this.buffer.Clear();
            }

            public void Dispose()
            {
                if (buffer != null)
                {
                    pool.Return(buffer!);
                    buffer = null;
                }
            }


            public readonly void Add(ContainerReference r, FieldType fieldType, bool isFieldDlete = false)
            {
                buffer!.Add(new Entry(r, fieldType, isFieldDlete));
            }

            public readonly void Add(ReadOnlySpan<ContainerReference> rs, bool isArray, bool isFieldDlete = false)
            {
                for (int i = 0; i < rs.Length; i++)
                {
                    if (rs[i] == Container.Registry.ID.Empty) continue;
                    buffer!.Add(new Entry(rs[i], isArray ? TypeUtil<ContainerReference>.ArrayFieldType : TypeUtil<ContainerReference>.ScalarFieldType, isFieldDlete));
                }
            }

            public readonly void AddArray(ReadOnlySpan<ContainerReference> rs, bool isFieldDlete = false)
            {
                for (int i = 0; i < rs.Length; i++)
                {
                    if (rs[i] == Container.Registry.ID.Empty) continue;
                    buffer!.Add(new Entry(rs[i], TypeUtil<ContainerReference>.ArrayFieldType, isFieldDlete));
                }
            }

            public readonly void Send(bool quiet = false)
            {
                foreach (var item in buffer!)
                {
                    string? fieldName = null;
                    ContainerReference reference = item.Reference;
                    if (reference == 0)
                        continue;
                    Container? container = Registry.Shared.GetContainer(reference);
                    if (container == null)
                        continue;

                    if (item.IsDeleted)
                        fieldName = container.Name.ToString();
                    Registry.Shared.Unregister(container);
                    if (!quiet && fieldName != null)
                    {
                        StorageObject.NotifyFieldDelete(parent, fieldName!, item.FieldType);
                    }
                }
                buffer.Clear();
            }

            public static UnregisterBuffer New(Container parent)
            {
                return new UnregisterBuffer(parent, pool.Rent());
            }
        }

        public struct ScalarDeleteBuffer : IDisposable
        {
            private static readonly ObjectPool<List<Entry>> pool = new ObjectPool<List<Entry>>(() => new List<Entry>());

            struct Entry
            {
                public string? FieldName;
                public FieldType FieldType;

                public Entry(string name, FieldType fieldType) : this()
                {
                    this.FieldName = name;
                    this.FieldType = fieldType;
                }
            }

            readonly Container parent;
            List<Entry>? buffer;

            public readonly bool IsDisposed => buffer != null;

            private ScalarDeleteBuffer(Container parent, List<Entry> entries)
            {
                this.parent = parent;
                this.buffer = entries;
                this.buffer.Clear();
            }

            public void Dispose()
            {
                if (buffer != null)
                {
                    pool.Return(buffer!);
                    buffer = null;
                }
            }


            public readonly void Add(string fieldName, FieldType fieldType)
            {
                buffer!.Add(new Entry(fieldName, fieldType));
            }

            public readonly void Send(bool quiet = false)
            {
                if (!quiet)
                {
                    foreach (var item in buffer!)
                    {
                        string? fieldName = item.FieldName;
                        StorageObject.NotifyFieldDelete(parent, fieldName!, item.FieldType);
                    }
                }
                buffer!.Clear();
            }

            public static ScalarDeleteBuffer New(Container parent)
            {
                return new ScalarDeleteBuffer(parent, pool.Rent());
            }
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
        private readonly Action<T>? _reset;
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
        public ObjectPool(Func<T> factory, Action<T>? reset = null, int maxSize = 0)
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
