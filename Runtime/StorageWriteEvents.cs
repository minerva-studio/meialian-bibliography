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
        /// <summary>The object that owns the written field.</summary>
        public StorageObject Target { get; }

        /// <summary>Name of the field that was written.</summary>
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
    /// Disposable handle that represents a subscription to a field write event.
    /// </summary>
    public sealed class StorageWriteSubscription : IDisposable
    {
        private readonly ContainerSubscriptions _owner;
        private readonly string _fieldName;
        private readonly int _id;
        private bool _disposed;

        internal StorageWriteSubscription(ContainerSubscriptions owner, string fieldName, int id)
        {
            _owner = owner;
            _fieldName = fieldName;
            _id = id;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _owner?.Remove(_fieldName, _id);
        }
    }

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
            return slot.Add(fieldName, handler);
        }

        public static void Notify(Container container, string fieldName, ValueType fieldType)
        {
            if (container == null || string.IsNullOrEmpty(fieldName))
                return;

            if (_table.TryGetValue(container, out var slot))
            {
                if (!slot.TryPrepareGeneration(container.Generation))
                    return;
                slot.Notify(container, fieldName, fieldType);
            }
        }

        public static bool HasSubscribers(Container container)
        {
            return container != null
                && _table.TryGetValue(container, out var slot)
                && slot.HasSubscribersForGeneration(container.Generation);
        }
    }

    internal sealed class ContainerSubscriptions
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, List<Subscriber>> _byField = new(StringComparer.Ordinal);
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
            lock (_gate)
            {
                if (_generation == generation)
                    return;

                _byField.Clear();
                _subscriptionCount = 0;
                _nextId = 1;
                _generation = generation;
            }
        }

        public StorageWriteSubscription Add(string fieldName, StorageFieldWriteHandler handler)
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
                return new StorageWriteSubscription(this, fieldName, subscriber.Id);
            }
        }

        public void Remove(string fieldName, int id)
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

        public void Notify(Container container, string fieldName, ValueType fieldType)
        {
            Subscriber[] snapshot;
            lock (_gate)
            {
                if (!_byField.TryGetValue(fieldName, out var list) || list.Count == 0)
                    return;
                snapshot = list.ToArray();
            }

            var args = new StorageFieldWriteEventArgs(new StorageObject(container), fieldName, fieldType);

            for (int i = 0; i < snapshot.Length; i++)
            {
                snapshot[i].Handler(in args);
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

