using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    public ref struct FieldHandle
    {
        internal readonly Container Container;
        public readonly ReadOnlySpan<char> Name;
        public readonly int Generation;

        /// <summary> 
        /// Schema version when this member is created.
        /// </summary>
        private int _schemaVersion;
        /// <summary>
        /// cached field index
        /// </summary>
        private int _cachedFieldIndex;


        public readonly bool IsDisposed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Container == null || Container.IsDisposed(Generation);
        }

        public int Index
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Container.EnsureNotDisposed(Generation);
                if (_cachedFieldIndex < 0 || Container.SchemaVersion != _schemaVersion)
                {
                    _cachedFieldIndex = Container.IndexOf(Name);
                    _schemaVersion = Container.SchemaVersion;
                }
                return _cachedFieldIndex;
            }
        }

        internal readonly int CachedFieldIndex => _cachedFieldIndex;

        internal ref FieldHeader Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref Container.GetFieldHeader(Index);
        }

        internal FieldHandle(Container container, ReadOnlySpan<char> fieldName) : this(container, fieldName, container.Generation)
        {
        }
        internal FieldHandle(Container container, ReadOnlySpan<char> fieldName, int generation)
        {
            ThrowHelper.ThrowIfOverlap(container.Span, MemoryMarshal.AsBytes(fieldName));
            Generation = generation;
            Container = container;
            Name = fieldName;
            _schemaVersion = container.SchemaVersion;
            _cachedFieldIndex = -1;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int EnsureNotDisposed()
        {
            Container.EnsureNotDisposed(Generation);
            return Index;
        }

        internal int EnsureValid()
        {
            if (Container == null || IsDisposed)
                ThrowHelper.ThrowDisposed(nameof(FieldHandle));
            int index = Index;
            if (index < 0)
                ThrowHelper.ThrowDisposed(nameof(FieldHandle));
            return index;
        }

        public struct Persistent
        {
            internal readonly Container Container;
            public readonly string Name;
            public readonly int Generation;
            /// <summary> 
            /// Schema version when this member is created.
            /// </summary>
            private int _schemaVersion;
            /// <summary>
            /// cached field index
            /// </summary>
            private int _cachedFieldIndex;


            public readonly bool IsDisposed
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Container.IsDisposed(Generation);
            }
            public int Index
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    Container.EnsureNotDisposed(Generation);
                    if (_cachedFieldIndex < 0 || Container.SchemaVersion != _schemaVersion)
                    {
                        _cachedFieldIndex = Container.IndexOf(Name);
                        _schemaVersion = Container.SchemaVersion;
                    }
                    return _cachedFieldIndex;
                }
            }
            internal readonly int CachedFieldIndex => _cachedFieldIndex;

            internal ref FieldHeader Header
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => ref Container.GetFieldHeader(Index);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int EnsureNotDisposed()
            {
                Container.EnsureNotDisposed(Generation);
                return Index;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int EnsureValid()
            {
                if (Container == null || IsDisposed)
                    ThrowHelper.ThrowDisposed(nameof(FieldHandle));
                int index = Index;
                if (index < 0)
                    ThrowHelper.ThrowDisposed(nameof(FieldHandle));
                return index;
            }

            internal Persistent(Container container, string fieldName)
            {
                Container = container;
                Generation = container.Generation;
                Name = fieldName;
                _schemaVersion = container.SchemaVersion;
                _cachedFieldIndex = -1;
            }

            internal Persistent(FieldHandle handle) : this()
            {
                Container = handle.Container;
                Name = handle.Name.ToString();
                Generation = handle.Generation;
                _schemaVersion = handle.Container.SchemaVersion;
                _cachedFieldIndex = -1;
            }

            public static implicit operator int(Persistent handle) => handle.Index;
            public static implicit operator FieldHandle(Persistent handle) => new(handle.Container, handle.Name, handle.Generation);
        }
    }
}
