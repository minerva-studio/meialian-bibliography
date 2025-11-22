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
        Rename = 2, // placeholder
        Delete = 3,
        Dispose = 4,
    }

    /// <summary>
    /// Describes a write event raised for a specific field within a container.
    /// </summary>
    public readonly struct StorageEventArgs
    {
        public StorageEvent Event { get; }
        /// <summary>The container receiving this invocation (default if deleted).</summary>
        public StorageObject Target { get; }

        /// <summary>Path of the written or deleted field relative to <see cref="Target"/>.</summary>
        public string Path { get; }

        /// <summary>Value type recorded for the field after the write.</summary>
        public ValueType FieldType { get; }

        internal StorageEventArgs(StorageEvent e, StorageObject target, string path, ValueType fieldType)
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
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (string.IsNullOrEmpty(fieldName)) throw new ArgumentException("Field name cannot be null or empty.", nameof(fieldName));
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            var slot = _table.GetValue(container, static c => new ContainerSubscriptions(c.Generation));
            slot.EnsureGeneration(container.Generation);
            return slot.AddFieldSubscriber(fieldName, handler);
        }

        public static StorageSubscription SubscribeToContainer(Container container, StorageMemberHandler handler)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (handler == null) throw new ArgumentNullException(nameof(handler));

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
        public static void NotifyFieldWrite(Container container, string fieldName, ValueType fieldType)
            => NotifyField(container, fieldName, fieldType, StorageEvent.Write);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void NotifyFieldDelete(Container container, string fieldName, ValueType fieldType)
            => NotifyField(container, fieldName, fieldType, StorageEvent.Delete);

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

        public static void NotifyField(Container source, string fieldName, ValueType fieldType, StorageEvent type)
        {
            if (source == null)
                return;

            // local
            if (_table.TryGetValue(source, out var slot) && slot.TryPrepareGeneration(source.Generation))
            {
                // If we are deleting the container itself (origin) we broadcast to all fields. 
                var target = new StorageObject(source);
                var args = new StorageEventArgs(type, target, fieldName, fieldType);
                slot.Notify(in args, fieldName);
            }

            NotifyUpward(source, fieldName, fieldType, type);
        }

        private static void NotifyUpward(Container source, string fieldName, ValueType fieldType, StorageEvent type)
        {
            if (source == null)
                return;

            Container current = source;
            using TempString str = new(fieldName ?? "");
            while (Container.Registry.Shared.TryGetParent(current, out var parent))
            {
                str.Prepend('.');
                str.Prepend(current.Name);
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
            Subscriber[] fieldSnapshot = null;
            Subscriber[] containerSnapshot = null;

            lock (_gate)
            {
                if (_generation == generation)
                    return;

                CollectEvents_NoLock(string.Empty, out fieldSnapshot, out containerSnapshot);

                _byField.Clear();
                _containerSubscribers.Clear();
                _subscriptionCount = 0;
                _nextId = 1;
                _generation = generation;
            }

            StorageEventArgs baseArgs = new(StorageEvent.Dispose, default, string.Empty, ValueType.Unknown);
            Notify(in baseArgs, fieldSnapshot, containerSnapshot);
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

        public void Notify(in StorageEventArgs baseArgs, string path)
        {
            Subscriber[] fieldSnapshot;
            Subscriber[] containerSnapshot;

            lock (_gate)
            {
                CollectEvents_NoLock(path, out fieldSnapshot, out containerSnapshot);
            }
            Notify(in baseArgs, fieldSnapshot, containerSnapshot);
        }

        private static void Notify(in StorageEventArgs baseArgs,
            Subscriber[] fieldSnapshot,
            Subscriber[] containerSnapshot)
        {

            // Fire specific field subscribers
            if (fieldSnapshot.Length > 0)
            {
                for (int i = 0; i < fieldSnapshot.Length; i++)
                    fieldSnapshot[i].Handler(in baseArgs);
            }

            // Fire container subscribers
            if (containerSnapshot.Length > 0)
            {
                for (int i = 0; i < containerSnapshot.Length; i++)
                    containerSnapshot[i].Handler(in baseArgs);
            }
        }

        private void CollectEvents_NoLock(string fieldName, out Subscriber[] fieldSnapshot, out Subscriber[] containerSnapshot)
        {
            fieldSnapshot = Array.Empty<Subscriber>();
            containerSnapshot = Array.Empty<Subscriber>();

            // 1. Specific field subscribers
            if (!string.IsNullOrEmpty(fieldName))
            {
                if (_byField.TryGetValue(fieldName, out var list) && list.Count > 0)
                    fieldSnapshot = list.ToArray();
            }
            // broadcast if no field name specified
            else if (_byField.Count > 0)
            {
                // 3. Broadcast to all fields
                int size = 0;
                foreach (var kvp in _byField)
                {
                    size += kvp.Value.Count;
                }
                int index = 0;
                fieldSnapshot = new Subscriber[size];
                foreach (var item in _byField)
                {
                    List<Subscriber> value = item.Value;
                    for (int i = 0; i < value.Count; i++)
                    {
                        fieldSnapshot[index++] = value[i];
                    }
                }
            }

            // 2. Container subscribers
            if (_containerSubscribers.Count > 0)
            {
                containerSnapshot = _containerSubscribers.ToArray();
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
}
