using System;
using System.Collections.Generic;
using System.Linq;

namespace Amlos.Container
{
    /// <summary>
    /// Value-type key for interning schemas *before* allocating a Schema instance.
    /// - Order-sensitive.
    /// - Compares Stride and each field's Name/Length/Offset.
    /// - Hash rule mirrors Schema.GetHashCode (stable & deterministic).
    /// </summary>
    public readonly struct SchemaKey : IEquatable<SchemaKey>
    {
        public static readonly SchemaKey Empty = new SchemaKey(Array.Empty<FieldDescriptor_Old>(), 0);

        public readonly IReadOnlyList<FieldDescriptor_Old> Fields;
        public readonly int Stride;

        public SchemaKey(IReadOnlyList<FieldDescriptor_Old> fields, int stride)
        {
            Fields = fields ?? throw new ArgumentNullException(nameof(fields));
            Stride = stride;
        }

        public bool Equals(SchemaKey other)
        {
            if (Stride != other.Stride) return false;
            if (Fields.Count != other.Fields.Count) return false;

            for (int i = 0; i < Fields.Count; i++)
            {
                var a = Fields[i];
                var b = other.Fields[i];
                if (!string.Equals(a.Name, b.Name, StringComparison.Ordinal)) return false;
                if (a.Length != b.Length || a.Offset != b.Offset) return false;
            }
            return true;
        }

        public override bool Equals(object obj) => obj is SchemaKey k && Equals(k);
        public override int GetHashCode() => Schema_Old.GetHashCode(Stride, Fields);
        public static bool operator ==(SchemaKey x, SchemaKey y) => x.Equals(y);
        public static bool operator !=(SchemaKey x, SchemaKey y) => !x.Equals(y);
    }

    /// <summary>
    /// Schema intern pool based on SchemaKey (no separate fingerprint).
    /// - If an equivalent schema exists, returns the existing instance.
    /// - Otherwise creates a new Schema and stores it.
    /// Thread-safe via a single lock (fast, low-contention).
    /// </summary>
    public sealed class SchemaPool
    {
        public static SchemaPool Shared { get; } = new SchemaPool();

        private readonly Dictionary<SchemaKey, Schema_Old> _map = new();
        private readonly object _gate = new();

        public int Count { get { lock (_gate) return _map.Count; } }

        public void Clear()
        {
            lock (_gate)
            {
                _map.Clear();
                _map.Add(SchemaKey.Empty, Schema_Old.Empty);
            }
        }

        public SchemaPool()
        {
            _map.Add(SchemaKey.Empty, Schema_Old.Empty);
        }

        /// <summary>
        /// Intern from a finalized field sequence (order-sensitive) and stride.
        /// Tries to avoid extra allocations when the input is already an array or a read-only array view.
        /// </summary>
        public Schema_Old Intern(IEnumerable<FieldDescriptor_Old> fields, int stride)
        {
            if (fields is null) throw new ArgumentNullException(nameof(fields));

            // Fast path: already an array
            if (fields is FieldDescriptor_Old[] arr)
                return Intern(arr, stride);

            // Safe path for IReadOnlyList<T>: try lookup first (no alloc on hit), copy on miss
            if (fields is IReadOnlyList<FieldDescriptor_Old> list)
            {
                // Create a temporary key over the list (note: list must not mutate while locked)
                var tempKey = new SchemaKey(list, stride);
                lock (_gate)
                {
                    if (_map.TryGetValue(tempKey, out var existing))
                        return existing; // hit: zero allocation

                    // miss: copy once to freeze layout, then insert
                    var frozen = list.ToArray();
                    var key = new SchemaKey(frozen, stride);
                    var created = new Schema_Old(frozen, stride);
                    _map.Add(key, created);
                    return created;
                }
            }

            // Fallback: enumerate once
            var materialized = fields.ToArray();
            return Intern(materialized, stride);
        }

        /// <summary>
        /// Intern using a finalized read-only list (array or immutable view preferred).
        /// </summary>
        public Schema_Old Intern(IReadOnlyList<FieldDescriptor_Old> fields, int stride)
        {
            if (fields is null) throw new ArgumentNullException(nameof(fields));
            var key = new SchemaKey(fields, stride);

            lock (_gate)
            {
                if (_map.TryGetValue(key, out var existing))
                    return existing;

                // If the incoming list is mutable (e.g., List<T>), freeze it into an array
                // to ensure key immutability inside the dictionary.
                var stable = fields is FieldDescriptor_Old[] fa ? fa : fields.ToArray();
                var stableKey = (ReferenceEquals(stable, fields)) ? key : new SchemaKey(stable, stride);

                var created = new Schema_Old(stable, stride);
                _map.Add(stableKey, created);
                return created;
            }
        }

        /// <summary>
        /// Intern an already constructed Schema.
        /// </summary>
        public Schema_Old Intern(Schema_Old schema)
        {
            if (schema is null) throw new ArgumentNullException(nameof(schema));
            var key = new SchemaKey(schema.Fields, schema.Stride);

            lock (_gate)
            {
                if (_map.TryGetValue(key, out var existing))
                    return existing;

                _map.Add(key, schema);
                return schema;
            }
        }

        /// <summary>
        /// Try to find an existing schema (order-sensitive) without creating one.
        /// </summary>
        public bool TryGetExisting(IReadOnlyList<FieldDescriptor_Old> fields, int stride, out Schema_Old schema)
        {
            schema = null;
            if (fields is null) return false;
            var key = new SchemaKey(fields, stride);
            lock (_gate) return _map.TryGetValue(key, out schema);
        }

        /// <summary>
        /// Try to find an existing schema from an IEnumerable (will avoid allocation for arrays/lists where possible).
        /// </summary>
        public bool TryGetExisting(IEnumerable<FieldDescriptor_Old> fields, int stride, out Schema_Old schema)
        {
            schema = null;
            if (fields is null) return false;

            if (fields is FieldDescriptor_Old[] arr)
                return TryGetExisting(arr, stride, out schema);

            if (fields is IReadOnlyList<FieldDescriptor_Old> list)
            {
                var key = new SchemaKey(list, stride);
                lock (_gate) return _map.TryGetValue(key, out schema);
            }

            // Fallback: enumerate once
            var materialized = fields.ToArray();
            return TryGetExisting(materialized, stride, out schema);
        }
    }
}
