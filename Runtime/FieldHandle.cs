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



        internal FieldHandle(Container container, ReadOnlySpan<char> fieldName)
        {
            ThrowHelper.ThrowIfOverlap(container.Span, MemoryMarshal.AsBytes(fieldName));
            Container = container;
            Generation = container.Generation;
            _schemaVersion = container.SchemaVersion;
            Name = fieldName;
            _cachedFieldIndex = -1;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int EnsureNotDisposed()
        {
            Container.EnsureNotDisposed(Generation);
            return Index;
        }

        public static unsafe implicit operator FieldHeader*(FieldHandle handle)
        {
            var index = handle.Index;
            return (FieldHeader*)Unsafe.AsPointer(ref handle.Container.GetFieldHeader(index));
        }
    }

    public struct PersistentFieldHandle
    {
        private readonly Container _container;
        private readonly string _fieldName;
        private readonly int _generation;
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
            get => _container.IsDisposed(_generation);
        }

        public int Index
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                _container.EnsureNotDisposed(_generation);
                if (_cachedFieldIndex < 0 || _container.SchemaVersion != _schemaVersion)
                {
                    _cachedFieldIndex = _container.IndexOf(_fieldName);
                    _schemaVersion = _container.SchemaVersion;
                }
                return _cachedFieldIndex;
            }
        }



        internal PersistentFieldHandle(Container container, string fieldName)
        {
            _container = container;
            _schemaVersion = container.SchemaVersion;
            _generation = container.Generation;
            _fieldName = fieldName;
            _cachedFieldIndex = -1;
        }

        public static implicit operator int(PersistentFieldHandle handle) => handle.Index;
    }
}
