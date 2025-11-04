using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Amlos.Container
{
    /// <summary>
    /// Mutable builder for constructing packed layouts with automatic, stable alignment.
    /// - No custom alignment knobs; alignment is inferred from field length via a stable rule.
    /// - Optional name-based canonicalization to make {a,b,c} == {c,b,a} (same layout).
    /// - Finalize by calling Build() to produce an immutable Schema; the builder can be reused.
    /// </summary>
    public sealed class SchemaBuilder
    {
        private readonly List<FieldDescriptor> _pending = new();
        private readonly bool _canonicalizeByName;

        /// <param name="canonicalizeByName">
        /// If true, fields are sorted by name before packing. This makes schema layout stable
        /// for the same set of fields regardless of insertion order (useful to deduplicate).
        /// If false, insertion order determines layout.
        /// </param>
        public SchemaBuilder(bool canonicalizeByName = true)
        {
            _canonicalizeByName = canonicalizeByName;
        }

        /// <summary>
        /// Add a field with a fixed byte length.
        /// Alignment is automatically chosen from the length (no custom override).
        /// </summary>
        public SchemaBuilder AddFieldFixed(string name, int length)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Field name cannot be null or empty.", nameof(name));

            return AddFieldFixed_Internal(name, length);
        }

        /// <summary>
        /// Add a field using sizeof(T). Alignment is auto-selected from the size.
        /// </summary>
        public SchemaBuilder AddFieldOf<T>(string name) where T : unmanaged => AddFieldFixed(name, Unsafe.SizeOf<T>());

        /// <summary>
        /// Add a field using sizeof(T). Alignment is auto-selected from the size.
        /// </summary>
        public SchemaBuilder AddArrayOf<T>(string name, int count) where T : unmanaged => AddFieldFixed(name, Unsafe.SizeOf<T>() * count);

        /// <summary>
        /// Add a reference field (8-byte slot).
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public SchemaBuilder AddRef(string name)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException(nameof(name));
            // one 8-byte reference slot:
            return AddFieldFixed_Internal(name, -FieldDescriptor.REF_SIZE);
        }

        /// <summary>
        /// Add a reference array field (count * 8-byte slots).
        /// </summary>
        /// <param name="name"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public SchemaBuilder AddRefArray(string name, int count)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException(nameof(name));
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
            long total = (long)FieldDescriptor.REF_SIZE * count;
            if (total > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(count), "Total byte length too large.");
            return AddFieldFixed_Internal(name, -(int)total);
        }

        private SchemaBuilder AddFieldFixed_Internal(string name, int length)
        {
            if (_pending.Any(p => string.Equals(p.Name, name, StringComparison.Ordinal)))
                throw new ArgumentException($"Field '{name}' already added to builder.", nameof(name));

            var fd = FieldDescriptor.Fixed(name, length);
            _pending.Add(fd);
            return this;
        }




        /// <summary>Remove a field by exact name (Ordinal). Returns true if removed.</summary>
        public bool RemoveField(string name)
        {
            if (string.IsNullOrEmpty(name)) return false;
            for (int i = 0; i < _pending.Count; i++)
            {
                if (string.Equals(_pending[i].Name, name, StringComparison.Ordinal))
                {
                    _pending.RemoveAt(i);
                    return true;
                }
            }
            return false;
        }

        /// <summary>Remove multiple fields by exact names (Ordinal). Returns number removed.</summary>
        public int RemoveFields(params string[] names)
        {
            if (names == null || names.Length == 0) return 0;
            int removed = 0;
            foreach (var n in names)
                if (RemoveField(n)) removed++;
            return removed;
        }

        /// <summary>Remove all fields matching a predicate. Returns number removed.</summary>
        public int RemoveWhere(Func<FieldDescriptor, bool> predicate)
        {
            if (predicate is null) return 0;
            int removed = 0;
            for (int i = _pending.Count - 1; i >= 0; i--)
            {
                if (predicate(_pending[i]))
                {
                    _pending.RemoveAt(i);
                    removed++;
                }
            }
            return removed;
        }





        /// <summary>
        /// Compute offsets with a deterministic alignment rule and produce an immutable Schema.
        /// </summary>
        public Schema Build()
        {
            if (_pending.Count == 0)
                return SchemaPool.Shared.Intern(Schema.Empty);

            // Prepare a working list so the builder can be reused after Build().
            var work = _pending.ToArray();

            // Optional canonicalization by name (maximizes schema reuse across insertion orders).
            if (_canonicalizeByName)
                Array.Sort(work, (a, b) => StringComparer.Ordinal.Compare(a.Name, b.Name));

            int stride = 0;
            for (int i = 0; i < work.Length; i++)
            {
                ref var p = ref work[i];
                int absLength = p.AbsLength;
                int align = p.IsRef ? FieldDescriptor.REF_SIZE : AutoAlign(absLength);
                stride += AlignPad(stride, align);
                p = p.WithOffset(stride);
                stride += absLength;
            }

            return SchemaPool.Shared.Intern(work, stride);
        }

        /// <summary>
        /// Stable alignment rule derived from field length:
        ///  - len >= 8 -> 8
        ///  - len >= 4 -> 4
        ///  - len >= 2 -> 2
        ///  - else      -> 1
        /// This mirrors a common "natural alignment" heuristic and is deterministic.
        /// </summary>
        private static int AutoAlign(int length)
        {
            if (length >= 8) return 8;
            if (length >= 4) return 4;
            if (length >= 2) return 2;
            return 1;
        }

        /// <summary>
        /// Returns the number of padding bytes required to align 'offset' to 'align'.
        /// </summary>
        private static int AlignPad(int offset, int align)
        {
            if (align <= 1) return 0;
            int mod = offset % align;
            return mod == 0 ? 0 : (align - mod);
        }




        /// <summary>
        /// Create a builder pre-populated from an existing schema (names+lengths only).
        /// Offsets are not copied; Build() recomputes offsets using the same alignment rule,
        /// so the result is equivalent if canonicalization flag matches.
        /// </summary>
        public static SchemaBuilder FromSchema(Schema schema, bool canonicalizeByName = true)
        {
            var b = new SchemaBuilder(canonicalizeByName);
            foreach (var f in schema.Fields)
                b.AddFieldFixed(f.Name, f.Length);
            return b;
        }
    }
}
