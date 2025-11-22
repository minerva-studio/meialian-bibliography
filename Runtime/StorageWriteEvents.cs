using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Describes a write event raised for a specific field within a container.
    /// </summary>
    public readonly struct StorageFieldWriteEventArgs
    {
        /// <summary>The container receiving this invocation (default if deleted).</summary>
        public StorageObject Target { get; }

        /// <summary>Path of the written or deleted field relative to <see cref="Target"/>.</summary>
        public string FieldName { get; }

        /// <summary>Value type recorded for the field after the write.</summary>
        public ValueType FieldType { get; }

        internal StorageFieldWriteEventArgs(StorageObject target, string fieldName, ValueType fieldType)
        {
            Target = target;
            FieldName = fieldName;
            FieldType = fieldType;
        }
    }

    /// <summary>
    /// Delegate invoked when a subscribed field is written.
    /// </summary>
    /// <param name="args">Context for the write.</param>
    public delegate void StorageFieldWriteHandler(in StorageFieldWriteEventArgs args);


    /// <summary>
    /// Represents a registered subscription that can be disposed to stop notifications.
    /// </summary>
    public sealed class StorageWriteSubscription : IDisposable
    {
        private readonly Action _disposeAction;
        private bool _disposed;

        internal StorageWriteSubscription(Action disposeAction)
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
    internal static class StorageWriteEventRegistry
    {
        private static readonly ConditionalWeakTable<Container, ContainerSubscriptions> _table = new();

        public static StorageWriteSubscription Subscribe(Container container, string fieldName, StorageFieldWriteHandler handler)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (string.IsNullOrEmpty(fieldName)) throw new ArgumentException("Field name cannot be null or empty.", nameof(fieldName));
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            var slot = _table.GetValue(container, static c => new ContainerSubscriptions(c.Generation));
            slot.EnsureGeneration(container.Generation);
            return slot.AddFieldSubscriber(fieldName, handler);
        }

        public static StorageWriteSubscription SubscribeToContainer(Container container, StorageFieldWriteHandler handler)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            var slot = _table.GetValue(container, static c => new ContainerSubscriptions(c.Generation));
            slot.EnsureGeneration(container.Generation);
            return slot.AddContainerSubscriber(handler);
        }

        public static void NotifyField(Container container, string fieldName, ValueType fieldType)
            => Notify(container, fieldName, fieldType, isDeleted: false);

        public static void Notify(Container origin, string fieldName, ValueType fieldType, bool isDeleted = false)
        {
            if (origin == null)
                return;

            using TempString str = new(fieldName ?? "");

            var current = origin;
            bool isOrigin = true;
            do
            {
                if (_table.TryGetValue(current, out var slot) && slot.TryPrepareGeneration(current.Generation))
                {
                    string fieldKey = isOrigin ? fieldName : string.Empty;

                    // If we are deleting the container itself (origin) we broadcast to all fields.
                    var target = isDeleted ? default : new StorageObject(current);
                    string path = str.ToString();
                    bool broadcastToFields = isDeleted && isOrigin;

                    var args = new StorageFieldWriteEventArgs(target, path, fieldType);
                    slot.Notify(in args, fieldKey, broadcastToFields);
                }

                // travrse up to root now
                if (!Container.Registry.Shared.TryGetParent(current, out var parent))
                    break;

                str.Prepend('.');
                str.Prepend(current.Name);

                current = parent;
                isOrigin = false;
            }
            while (current != null);
        }

        public static void NotifyDispose(Container container, int generation)
        {
            if (container == null)
                return;

            if (_table.TryGetValue(container, out var slot))
            {
                slot.NotifyDispose(generation);
            }
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
            List<(string Key, Subscriber[] Subs)> broadcastSnapshot = null;

            lock (_gate)
            {
                if (_generation == generation)
                    return;

                CollectEvents_NoLock(string.Empty, broadcast: true, out fieldSnapshot, out containerSnapshot, out broadcastSnapshot);

                _byField.Clear();
                _containerSubscribers.Clear();
                _subscriptionCount = 0;
                _nextId = 1;
                _generation = generation;
            }

            StorageFieldWriteEventArgs baseArgs = new(default, string.Empty, ValueType.Unknown);
            Notify(in baseArgs, fieldSnapshot, containerSnapshot, broadcastSnapshot);
        }

        public StorageWriteSubscription AddFieldSubscriber(string fieldName, StorageFieldWriteHandler handler)
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
                return new StorageWriteSubscription(() => RemoveFieldSubscriber(fieldName, subscriber.Id));
            }
        }

        public StorageWriteSubscription AddContainerSubscriber(StorageFieldWriteHandler handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));

            lock (_gate)
            {
                var subscriber = new Subscriber(_nextId++, handler);
                _containerSubscribers.Add(subscriber);
                Interlocked.Increment(ref _subscriptionCount);
                return new StorageWriteSubscription(() => RemoveContainerSubscriber(subscriber.Id));
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

        public void Notify(in StorageFieldWriteEventArgs baseArgs, string path, bool broadcast)
        {
            Subscriber[] fieldSnapshot;
            Subscriber[] containerSnapshot;
            List<(string Key, Subscriber[] Subs)> broadcastSnapshot;

            lock (_gate)
            {
                CollectEvents_NoLock(path, broadcast, out fieldSnapshot, out containerSnapshot, out broadcastSnapshot);
            }
            Notify(in baseArgs, fieldSnapshot, containerSnapshot, broadcastSnapshot);
        }

        private static void Notify(in StorageFieldWriteEventArgs baseArgs,
            Subscriber[] fieldSnapshot,
            Subscriber[] containerSnapshot,
            List<(string Key, Subscriber[] Subs)> broadcastSnapshot)
        {

            // Fire specific field subscribers
            if (fieldSnapshot.Length > 0)
            {
                for (int i = 0; i < fieldSnapshot.Length; i++)
                    fieldSnapshot[i].Handler(in baseArgs);
            }

            // Fire broadcast field subscribers
            if (broadcastSnapshot != null)
            {
                foreach (var (key, subs) in broadcastSnapshot)
                {
                    // If broadcasting, we update the FieldName in the args to match the field 
                    // so subscribers know which field is being deleted.
                    var fieldArgs = new StorageFieldWriteEventArgs(baseArgs.Target, key, baseArgs.FieldType);
                    for (int i = 0; i < subs.Length; i++)
                    {
                        subs[i].Handler(in fieldArgs);
                    }
                }
            }

            // Fire container subscribers
            if (containerSnapshot.Length > 0)
            {
                for (int i = 0; i < containerSnapshot.Length; i++)
                    containerSnapshot[i].Handler(in baseArgs);
            }
        }

        private void CollectEvents_NoLock(string path, bool broadcast, out Subscriber[] fieldSnapshot, out Subscriber[] containerSnapshot, out List<(string Key, Subscriber[] Subs)> broadcastSnapshot)
        {
            fieldSnapshot = Array.Empty<Subscriber>();
            containerSnapshot = Array.Empty<Subscriber>();

            // We might need to snapshot all fields if broadcasting
            broadcastSnapshot = null;

            // 1. Specific field subscribers
            if (!string.IsNullOrEmpty(path) && _byField.TryGetValue(path, out var list) && list.Count > 0)
            {
                fieldSnapshot = list.ToArray();
            }

            // 2. Container subscribers
            if (_containerSubscribers.Count > 0)
            {
                containerSnapshot = _containerSubscribers.ToArray();
            }

            // 3. Broadcast to all fields
            if (broadcast && _byField.Count > 0)
            {
                broadcastSnapshot = new List<(string, Subscriber[])>(_byField.Count);
                foreach (var kvp in _byField)
                {
                    if (kvp.Value.Count > 0)
                    {
                        broadcastSnapshot.Add((kvp.Key, kvp.Value.ToArray()));
                    }
                }
            }
        }



        private readonly struct Subscriber
        {
            public int Id { get; }
            public StorageFieldWriteHandler Handler { get; }

            public Subscriber(int id, StorageFieldWriteHandler handler)
            {
                Id = id;
                Handler = handler ?? throw new ArgumentNullException(nameof(handler));
            }
        }
    }
}
