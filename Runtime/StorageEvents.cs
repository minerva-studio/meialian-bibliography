#nullable enable
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Minerva.DataStorage
{
    public enum StorageEvent
    {
        None = 0,
        Write = 1,
        Rename = 2,
        Delete = 3,
        Dispose = 4,
    }

    /// <summary>
    /// Describes a write event raised for a specific field within a container.
    /// </summary>
    public readonly struct StorageEventArgs
    {
        /// <summary>Event type.</summary>
        public StorageEvent Event { get; }
        /// <summary>The object receiving this invocation (default if deleted).</summary>
        public StorageObject Target { get; }
        /// <summary>Path of the written or deleted field relative to <see cref="Target"/>. Will be the new name on the rename event</summary>
        public string Path { get; }
        /// <summary>Value type recorded for the field after the write.</summary>
        public FieldType FieldType { get; }

        internal StorageEventArgs(StorageEvent e, StorageObject target, string path, FieldType fieldType)
        {
            Event = e;
            Target = target;
            Path = path;
            FieldType = fieldType;
        }

        public override string ToString()
        {
            return $"Type: {Event}, Path: {Path}, FieldType: {FieldType}";
        }
    }

    /// <summary>
    /// Delegate invoked when a subscribed field is written.
    /// </summary>
    /// <param name="args">Context for the write.</param>
    public delegate void StorageMemberHandler(in StorageEventArgs args);


    /// <summary>
    /// Represents a registered subscription that can be disposed to stop notifications.
    /// </summary>
    public sealed class StorageSubscription : IDisposable
    {
        private readonly Action _disposeAction;
        private bool _disposed;

        internal StorageSubscription(Action disposeAction)
        {
            _disposeAction = disposeAction ?? throw new ArgumentNullException(nameof(disposeAction));
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _disposeAction();
        }
    }

    /// <summary>
    /// Central registry that maps containers to their subscription lists.
    /// </summary>
    internal static class StorageEventRegistry
    {
        private static readonly ConditionalWeakTable<Container, ContainerSubscriptions> _table = new();

        public static StorageSubscription Subscribe(Container container, string fieldName, StorageMemberHandler handler)
        {
            ThrowHelper.ThrowIfNull(container, nameof(container));
            ThrowHelper.ThrowIfNull(fieldName, nameof(fieldName));
            ThrowHelper.ThrowIfNull(handler, nameof(handler));

            var slot = _table.GetValue(container, static c => new ContainerSubscriptions(c.Generation));
            slot.EnsureGeneration(container.Generation);
            // Subscribe to container if no field specified or container is an array
            if (fieldName == "" || container.IsArray) return slot.AddContainerSubscriber(handler);
            return slot.AddFieldSubscriber(fieldName, handler);
        }

        public static StorageSubscription SubscribeToContainer(Container container, StorageMemberHandler handler)
        {
            ThrowHelper.ThrowIfNull(container, nameof(container));
            ThrowHelper.ThrowIfNull(handler, nameof(handler));

            var slot = _table.GetValue(container, static c => new ContainerSubscriptions(c.Generation));
            slot.EnsureGeneration(container.Generation);
            return slot.AddContainerSubscriber(handler);
        }

        public static bool HasSubscribers(Container container)
        {
            var current = container;
            while (current != null)
            {
                if (_table.TryGetValue(current, out var slot) && slot.HasSubscribersForGeneration(current.Generation))
                    return true;
                if (!Container.Registry.Shared.TryGetParent(current, out current))
                    break;
            }
            return false;
        }

        public static void RemoveFieldSubscriptions(Container container, string fieldName)
        {
            if (container == null || string.IsNullOrEmpty(fieldName))
                return;

            if (_table.TryGetValue(container, out var slot))
                slot.RemoveField(fieldName);
        }






        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NotifyDispose(Container container, int generation)
        {
            if (container == null)
                return;

            if (_table.TryGetValue(container, out var slot))
            {
                slot.NotifyDispose(generation);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NotifyFieldWrite(Container container, int fieldIndex)
        {
            if (!HasSubscribers(container))
                return;
            ref var header = ref container.GetFieldHeader(fieldIndex);
            var fieldName = container.GetFieldName(in header).ToString();
            NotifyFieldWrite(container, fieldName, header.FieldType);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NotifyFieldWrite(Container container, string fieldName, FieldType fieldType)
            => NotifyField(container, fieldName, fieldType, StorageEvent.Write);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NotifyFieldDelete(Container container, string fieldName, FieldType fieldType)
        {
            if (!HasSubscribers(container))
                return;

            NotifyField(container, fieldName, fieldType, StorageEvent.Delete);
            RemoveFieldSubscriptions(container, fieldName);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NotifyFieldRename(Container source, string fieldName, string newName, FieldType fieldType)
        {
            if (_table.TryGetValue(source, out var slot) && slot.TryPrepareGeneration(source.Generation))
                slot.Rename(fieldName, newName);

            NotifyField(source, newName, fieldType, StorageEvent.Rename);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void NotifyField(Container source, string fieldName, FieldType fieldType, StorageEvent type)
        {
            if (source == null)
                return;

            // local 
            {
                if (_table.TryGetValue(source, out var slot) && slot.TryPrepareGeneration(source.Generation))
                {
                    // If we are deleting the container itself (origin) we broadcast to all fields. 
                    var target = new StorageObject(source);
                    var args = new StorageEventArgs(type, target, fieldName, fieldType);
                    slot.Notify(in args, fieldName);
                }
            }

            // upward
            Container current = source;
            using TempString str = TempString.Create(!source.IsArray ? fieldName ?? "" : "");
            while (Container.Registry.Shared.TryGetParent(current, out var parent))
            {
                Span<char> name = current.NameSpan;
                if (str.Length > 0 && str[0] != '[') str.Prepend('.');
                if (parent.IsArray) str.Prepend(name[name.LastIndexOf('[')..]);
                else str.Prepend(name);

                if (_table.TryGetValue(parent, out var slot) && slot.TryPrepareGeneration(parent.Generation))
                {
                    // If we are deleting the container itself (origin) we broadcast to all fields. 
                    string path = str.ToString();
                    var target = new StorageObject(parent);
                    var args = new StorageEventArgs(type, target, path, fieldType);
                    slot.Notify(in args, path);
                }
                current = parent;
            }
        }
    }

    /// <summary>
    /// Holds all subscriptions (field-level and container-level) for a single Container.
    /// </summary>
    internal sealed class ContainerSubscriptions
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, List<Subscriber>> _byField = new(StringComparer.Ordinal);
        private readonly List<Subscriber> _containerSubscribers = new();
        private int _nextId = 1;
        private int _subscriptionCount;
        private int _generation;

        public ContainerSubscriptions(int generation)
        {
            _generation = generation;
        }

        public bool HasAny => Volatile.Read(ref _subscriptionCount) > 0;

        public void EnsureGeneration(int generation)
        {
            if (generation == Volatile.Read(ref _generation))
                return;
            ResetForGeneration(generation);
        }

        public bool TryPrepareGeneration(int generation)
        {
            if (generation == Volatile.Read(ref _generation))
                return true;
            ResetForGeneration(generation);
            return false;
        }

        public bool HasSubscribersForGeneration(int generation)
        {
            EnsureGeneration(generation);
            return HasAny;
        }

        private void ResetForGeneration(int generation)
        {
            Subscriber[]? fieldSnapshot = null;
            lock (_gate)
            {
                if (_generation == generation)
                    return;

                fieldSnapshot = CollectEvents_NoLock(string.Empty);

                _byField.Clear();
                _containerSubscribers.Clear();
                _subscriptionCount = 0;
                _nextId = 1;
                _generation = generation;
            }

            StorageEventArgs baseArgs = new(StorageEvent.Dispose, default, string.Empty, FieldType.ScalarUnknown);
            Notify(in baseArgs, fieldSnapshot);
        }

        public StorageSubscription AddFieldSubscriber(string fieldName, StorageMemberHandler handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            lock (_gate)
            {
                if (!_byField.TryGetValue(fieldName, out var list))
                {
                    list = new List<Subscriber>();
                    _byField[fieldName] = list;
                }

                var subscriber = new Subscriber(_nextId++, handler);
                list.Add(subscriber);
                Interlocked.Increment(ref _subscriptionCount);
                return new StorageSubscription(() => RemoveFieldSubscriber(fieldName, subscriber.Id));
            }
        }

        public StorageSubscription AddContainerSubscriber(StorageMemberHandler handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            lock (_gate)
            {
                var subscriber = new Subscriber(_nextId++, handler);
                _containerSubscribers.Add(subscriber);
                Interlocked.Increment(ref _subscriptionCount);
                return new StorageSubscription(() => RemoveContainerSubscriber(subscriber.Id));
            }
        }

        private void RemoveFieldSubscriber(string fieldName, int id)
        {
            lock (_gate)
            {
                if (!_byField.TryGetValue(fieldName, out var list))
                    return;

                for (int i = list.Count - 1; i >= 0; i--)
                {
                    if (list[i].Id == id)
                    {
                        list.RemoveAt(i);
                        Interlocked.Decrement(ref _subscriptionCount);
                        break;
                    }
                }

                if (list.Count == 0)
                {
                    _byField.Remove(fieldName);
                }
            }
        }

        private void RemoveContainerSubscriber(int id)
        {
            lock (_gate)
            {
                for (int i = _containerSubscribers.Count - 1; i >= 0; i--)
                {
                    if (_containerSubscribers[i].Id == id)
                    {
                        _containerSubscribers.RemoveAt(i);
                        Interlocked.Decrement(ref _subscriptionCount);
                        break;
                    }
                }
            }
        }

        public void RemoveField(string fieldName)
        {
            lock (_gate)
            {
                if (!_byField.Remove(fieldName, out var list) || list.Count == 0)
                    return;

                _subscriptionCount -= list.Count;
                if (_subscriptionCount < 0) _subscriptionCount = 0;
            }
        }




        public void NotifyDispose(int generation)
        {
            // missed the generation change, then don't notify
            if (generation != this._generation) return;
            // reset for next generation, and invoke everything
            ResetForGeneration(generation + 1);
        }

        public void Rename(string oldName, string newName)
        {
            lock (_gate)
            {
                if (_byField.TryGetValue(oldName, out var list))
                {
                    _byField.Remove(oldName);
                    _byField[newName] = list;
                }
            }
        }

        public void Notify(in StorageEventArgs baseArgs, string path)
        {
            Subscriber[] fieldSnapshot;
            lock (_gate)
            {
                fieldSnapshot = CollectEvents_NoLock(path);
            }
            Notify(in baseArgs, fieldSnapshot);
        }




        private Subscriber[] CollectEvents_NoLock(string fieldName)
        {
            int length = 0;
            Subscriber[] result;
            List<Subscriber>? list = null;
            bool broadcast = false;
            // 1. Specific field subscribers
            if (!string.IsNullOrEmpty(fieldName))
            {
                if (_byField.TryGetValue(fieldName, out list))
                    length += list.Count;
            }
            // broadcast if no field name specified
            else if (_byField.Count > 0)
            {
                // 3. Broadcast to all fields 
                broadcast = true;
                foreach (var kvp in _byField)
                {
                    length += kvp.Value.Count;
                }
            }
            // 2. Container subscribers 
            length += _containerSubscribers.Count;

            if (length == 0)
                return Array.Empty<Subscriber>();

            // copy
            result = new Subscriber[length];
            int index = _containerSubscribers.Count;
            _containerSubscribers.CopyTo(result, 0);

            if (list != null)
            {
                list.CopyTo(result, index);
                index += list.Count;
            }
            if (broadcast)
            {
                foreach (var item in _byField)
                {
                    List<Subscriber> value = item.Value;
                    for (int i = 0; i < value.Count; i++)
                    {
                        result[index++] = value[i];
                    }
                }
            }

            return result;
        }

        private static void Notify(in StorageEventArgs baseArgs, Subscriber[] fieldSnapshot)
        {
            // Fire specific field subscribers
            if (fieldSnapshot.Length > 0)
            {
                for (int i = 0; i < fieldSnapshot.Length; i++)
                    fieldSnapshot[i].Handler(in baseArgs);
            }
        }



        private readonly struct Subscriber
        {
            public int Id { get; }
            public StorageMemberHandler Handler { get; }

            public Subscriber(int id, StorageMemberHandler handler)
            {
                Id = id;
                Handler = handler ?? throw new ArgumentNullException(nameof(handler));
            }
        }
    }

    /// <summary>
    /// A buffer for recording delete/write event
    /// </summary>
    internal struct UnregisterBuffer : IDisposable
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
                Container? container = Container.Registry.Shared.GetContainer(reference);
                if (container == null)
                    continue;

                if (item.IsDeleted)
                    fieldName = container.NameSpan.ToString();
                Container.Registry.Shared.Unregister(container);

                if (!quiet && fieldName != null)
                    StorageEventRegistry.NotifyFieldDelete(parent, fieldName!, item.FieldType);
            }
            buffer.Clear();
        }

        public static UnregisterBuffer New(Container parent)
        {
            return new UnregisterBuffer(parent, pool.Rent());
        }
    }

    internal struct FieldDeleteEventBuffer : IDisposable
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

        private FieldDeleteEventBuffer(Container parent, List<Entry> entries)
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
                    StorageEventRegistry.NotifyFieldDelete(parent, fieldName!, item.FieldType);
                }
            }
            buffer!.Clear();
        }

        public static FieldDeleteEventBuffer New(Container parent)
        {
            return new FieldDeleteEventBuffer(parent, pool.Rent());
        }
    }

}
