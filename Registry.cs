using System;
using System.Collections.Generic;

namespace Amlos.Container
{
    internal partial class Container
    {
        internal ulong _id; // assigned by registry

        /// <summary> object id </summary>
        internal ulong ID => _id;

        internal class Registry
        {
            public static Registry Shared { get; } = new Registry();

            private ulong _next = 1; // 0 reserved for "null"
            private readonly Queue<ulong> _freed = new();
            private readonly object _lock = new();
            private readonly Dictionary<ulong, Container> _table = new();

            public void Register(Container container)
            {
                if (container is null) throw new ArgumentNullException(nameof(container));
                lock (_lock)
                {
                    if (container._id != 0UL)
                        throw new InvalidOperationException("Container already registered.");

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
                if (container is null) return;

                // 1) Remove self from registry and recycle its id under lock.
                ulong id;
                lock (_lock)
                {
                    id = container._id;
                    if (id == 0UL) return;           // already unregistered / never registered
                    if (!_table.Remove(id)) return;   // not found -> treat as done
                    _freed.Enqueue(id);               // recycle id now (parent id，与子无关)
                    container._id = 0UL;              // mark as unregistered (幂等保护)
                }

                // 2) Without holding the lock, walk each ref field and recurse immediately.
                //    No snapshot, no allocations. Assumes callers不会在此期间修改这些ref字段。
                foreach (var field in container.Schema.Fields)
                {
                    if (!field.IsRef) continue;

                    // This Span<ulong> views the parent's buffer. We only read it, not modify.
                    var ids = container.GetRefSpan(field);
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


            public Container GetContainer(ulong id)
            {
                if (id == 0UL) return null;
                lock (_lock) return _table.GetValueOrDefault(id);
            }

            private ulong Next_NoLock()
            {
                if (_freed.Count > 0) return _freed.Dequeue();
                return _next++;
            }
        }
    }
}
