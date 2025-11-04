using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Amlos.Container
{
    /// <summary>
    /// Immutable schema describing a packed byte layout:
    /// - Each field has Name, Length (in bytes), and Offset (byte offset within the record).
    /// - Schema instances are immutable once created.
    /// - No field kind/type is stored here by design; callers must use the correct read/write API.
    /// </summary>
    public sealed class Schema : IEquatable<Schema>
    {
        public static readonly int ALIGN = 8;
        public static readonly Schema Empty = new Schema(Array.Empty<FieldDescriptor>(), 0);

        private readonly FieldDescriptor[] _fields;               // immutable copy
        private readonly Dictionary<string, int> _indexByName;    // O(1) lookups by name
        private FixedBytePool pool;

        /// <summary>Number of fields; also the size in bytes of the per-container hint header.</summary>
        public int HeaderSize => _fields.Length;

        /// <summary>Start of data region: AlignUp(HeaderSize, ALIGN).</summary>
        public int DataBase => AlignUp(HeaderSize, ALIGN);

        /// <summary> Total byte width of one record (sum of packed fields + alignment padding). </summary>
        public int Stride { get; }

        /// <summary>
        /// Per-schema fixed byte pool (exact size = Stride). May be empty-size for Stride==0.
        /// </summary>
        public FixedBytePool Pool => pool ??= new FixedBytePool(Stride);
        /// <summary>Fields in final, packed layout order.</summary>
        public IReadOnlyList<FieldDescriptor> Fields => _fields;



        internal Schema(IEnumerable<FieldDescriptor> fields, int stride)
        {
            _fields = (fields as FieldDescriptor[]) ?? fields.ToArray(); // value-type array copy
            Stride = stride;

            _indexByName = new Dictionary<string, int>(StringComparer.Ordinal);
            for (int i = 0; i < _fields.Length; i++)
            {
                var name = _fields[i].Name ?? throw new ArgumentNullException(nameof(FieldDescriptor.Name));
                if (_indexByName.ContainsKey(name))
                    throw new ArgumentException($"Duplicate field name in schema: '{name}'.");
                _indexByName[name] = i;
            }
        }

        /// <summary>
        /// Get index of the field
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public int IndexOf(string name) => _indexByName.TryGetValue(name, out var i) ? i : -1;

        /// <summary>
        /// Try to determine whether a field exists by name (O(1)).
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool ContainsField(string name) => _indexByName.ContainsKey(name);

        /// <summary>Try to find a field by name (O(1)).</summary>
        public bool TryGetField(string name, out FieldDescriptor field)
        {
            if (_indexByName.TryGetValue(name, out int idx))
            {
                field = _fields[idx];
                return true;
            }
            field = default;
            return false;
        }

        /// <summary>Get a field by name or throw if missing.</summary>
        public FieldDescriptor GetField(string name)
        {
            if (!TryGetField(name, out var fd))
                throw new KeyNotFoundException($"Field '{name}' not found in schema.");
            return fd;
        }



        #region Equality / Hashing
        public bool Equals(Schema other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            if (Stride != other.Stride) return false;
            if (_fields.Length != other._fields.Length) return false;

            for (int i = 0; i < _fields.Length; i++)
            {
                // FieldDescriptor.Equals compares Name/Length/Offset
                if (!_fields[i].Equals(other._fields[i])) return false;
            }
            return true;
        }

        public override bool Equals(object obj) => obj is Schema s && Equals(s);

        public override int GetHashCode()
        {
            var h = new HashCode();
            h.Add(Stride);
            h.Add(_fields.Length);
            foreach (var f in _fields) h.Add(f);
            return h.ToHashCode();
        }

        public static bool operator ==(Schema lhs, Schema rhs) => ReferenceEquals(lhs, rhs) || (lhs?.Equals(rhs) ?? false);

        public static bool operator !=(Schema lhs, Schema rhs) => !(lhs == rhs);

        public static int GetHashCode(int stride, IReadOnlyList<FieldDescriptor> field)
        {
            var hc = new HashCode();
            hc.Add(stride);
            for (int i = 0; i < field.Count; i++)
                hc.Add(field[i]); // FieldDescriptor.GetHashCode already combines Name/Offset/Length
            return hc.ToHashCode();
        }
        #endregion




        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int AlignUp(int x, int a)
        {
            if (a <= 1) return x;
            int m = x % a;
            return m == 0 ? x : x + (a - m);
        }
    }
}
