using System;
using System.Collections.Generic;
using System.Linq;

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
        public unsafe SchemaBuilder AddFieldOf<T>(string name) where T : unmanaged => AddFieldFixed(name, sizeof(T));

        public SchemaBuilder AddRef(string name)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException(nameof(name));
            // one 8-byte reference slot:
            return AddFieldFixed_Internal(name, -FieldDescriptor.REF_SIZE);
        }

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
    }
}
