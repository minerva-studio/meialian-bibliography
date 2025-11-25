using System;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Deferred path query DSL. Does NOT create or mutate automatically.
    /// Use Ensure()/Exist() terminal objects to apply creation or checks.
    /// </summary>
    public struct StorageQuery : IDisposable
    {
        private StorageObject _root;
        private TempString _segments;

        // In-place expectation state
        private bool _expectFailed;
        private string _expectError;

        internal StorageQuery(StorageObject root)
        {
            _root = root;
            _segments = new TempString(0);
            _expectFailed = false;
            _expectError = null;
        }

        internal StorageQuery(StorageObject root, string first)
        {
            _root = root;
            _segments = new TempString(first.Length);
            _segments.Append(first);
            _expectFailed = false;
            _expectError = null;
        }

        /// <summary>Complete path as string.</summary>
        public string Path => _segments.ToString();

        /// <summary>True when at least one segment added.</summary>
        public bool HasSegments => _segments.Length > 0;

        /// <summary>True if any Expect() check failed.</summary>
        public bool ExpectFailed => _expectFailed;

        /// <summary>Error message from first failed Expect().</summary>
        public string ExpectError => _expectError;

        /// <summary>
        /// Append a path segment (may itself include '[index]' or dots you intend literally).
        /// </summary>
        public StorageQuery Location(ReadOnlySpan<char> path)
        {
            if (path.Length == 0) throw new ArgumentException("Segment cannot be empty", nameof(path));
            if (_segments.Length > 0) _segments.Append('.');
            _segments.Append(path);
            return this;
        }

        /// <summary>
        /// Append an index to the last segment: turns 'items' into 'items[3]'.
        /// </summary>
        public StorageQuery Index(int index)
        {
            if (index < 0) throw new ArgumentOutOfRangeException(nameof(index));
            if (_segments.Length == 0) throw new InvalidOperationException("Cannot apply index before any segment.");
            _segments.Append('[');
            Span<char> chars = stackalloc char[11];
            index.TryFormat(chars, out int written);
            _segments.Append(chars[..written]);
            _segments.Append(']');
            return this;
        }

        /// <summary>Get member view for this path.</summary>
        public StorageMember Member(bool createIfMissing = true)
        {
            EnsureRootValid();
            var path = Path;
            return createIfMissing ? _root.GetMember(path) : (_root.TryGetMember(path, out var m) ? m : default);
        }

        /// <summary>Try get member view (non-creating).</summary>
        public bool TryMember(out StorageMember member)
        {
            EnsureRootValid();
            return _root.TryGetMember(Path, out member);
        }

        /// <summary>Read scalar of type T (throws if not found or incompatible).</summary>
        public T Read<T>() where T : unmanaged
        {
            EnsureRootValid();
            return _root.ReadPath<T>(Path);
        }

        /// <summary>Try read scalar T.</summary>
        public bool TryRead<T>(out T value) where T : unmanaged
        {
            EnsureRootValid();
            return _root.TryReadPath<T>(Path, out value);
        }

        /// <summary>Write scalar T (creates intermediate nodes).</summary>
        public void Write<T>(T value) where T : unmanaged
        {
            EnsureRootValid();
            _root.WritePath(Path, value);
        }

        /// <summary>Read string.</summary>
        public string ReadString()
        {
            EnsureRootValid();
            return _root.ReadStringPath(Path);
        }

        /// <summary>Write string.</summary>
        public void WriteString(string value)
        {
            EnsureRootValid();
            _root.WritePath(Path, value);
        }

        /// <summary>Get object if path resolves to object reference; returns null object if missing.</summary>
        public StorageObject GetObjectOrDefault()
        {
            EnsureRootValid();
            return _root.GetObjectByPath(Path, createIfMissing: false);
        }

        /// <summary>Get array view (throws if not array).</summary>
        public StorageArray Array<T>() where T : unmanaged
        {
            EnsureRootValid();
            return _root.GetArrayByPath<T>(Path.AsSpan(), false);
        }

        /// <summary>Try get array view.</summary>
        public bool TryArray<T>(out StorageArray array) where T : unmanaged
        {
            EnsureRootValid();
            return _root.TryGetArrayByPath<T>(_segments.Span, out array);
        }

        /// <summary>Subscribe to writes for this path.</summary>
        public StorageSubscription Subscribe(StorageMemberHandler handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            EnsureRootValid();
            return _root.Subscribe(Path, handler);
        }

        /// <summary>Enter ensure semantics (creation allowed).</summary>
        public EnsureStatement Ensure()
        {
            var e = new EnsureStatement(_root, Path);
            _segments.Dispose();
            return e;
        }

        /// <summary>Enter exist semantics (no creation).</summary>
        public ExistStatement Exist()
        {
            var e = new ExistStatement(_root, Path);
            _segments.Dispose();
            return e;
        }

        public ExpectStatement Expect()
        {
            if (!HasSegments) throw new InvalidOperationException("Expect() requires at least one Location().");
            string full = Path;
            int dot = full.LastIndexOf('.');
            string last = dot >= 0 ? full.Substring(dot + 1) : full;

            int idxStart = last.LastIndexOf('[');
            int idxEnd = last.LastIndexOf(']');
            int index = -1;
            if (idxStart >= 0 && idxEnd > idxStart)
            {
                if (int.TryParse(last.AsSpan(idxStart + 1, idxEnd - idxStart - 1), out var parsed))
                {
                    index = parsed;
                    last = last.Substring(0, idxStart);
                }
            }
            var exp = new ExpectStatement(this, index);
            return exp;
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureRootValid()
        {
            if (_root.IsNull || _root.IsDisposed)
                throw new ObjectDisposedException("Root StorageObject invalid.");
        }

        public void Dispose()
        {
            _segments.Dispose();
            _root = default;
        }



        // ===== In-place Expect DSL =====



        internal void FailExpect(string message)
        {
            if (!_expectFailed)
            {
                _expectFailed = true;
                _expectError = message;
            }
        }

        public override string ToString() => Path;

        public readonly struct ExpectStatement
        {
            private readonly StorageQuery _query;
            private readonly int _index;

            internal ExpectStatement(StorageQuery query, int index)
            {
                _query = query;
                _index = index;
            }

            // Helper: returns member for full path (may include [index] which is not a field)
            private bool TryGetFullPathMember(out StorageMember member)
            {
                member = default;
                if (_query.ExpectFailed) return false;
                return _query._root.TryGetMember(_query.Path, out member);
            }

            private StorageQuery Fail(string msg, bool strict)
            {
                var q = _query;
                if (strict) q.FailExpect(msg);
                return q;
            }

            private StorageQuery Pass() => _query;

            /// <summary>Accept anything (never fails).</summary>
            public StorageQuery Any()
            {
                return _query;
            }

            /// <summary>Expect an object (non-array ref).</summary>
            public StorageQuery Object(bool strict = true)
            {
                if (_query.ExpectFailed) return _query;
                if (!TryGetFullPathMember(out var m))
                    return Fail($"Expectation failed: '{_query.Path}' missing.", strict);

                if (!(m.ValueType == ValueType.Ref && !m.IsArray))
                    return Fail($"Expect Object: '{_query.Path}' not object.", strict);
                return Pass();
            }

            /// <summary>Expect an object array (ref array).</summary>
            public StorageQuery ObjectArray(bool strict = true)
            {
                if (_query.ExpectFailed) return _query;
                if (!TryGetFullPathMember(out var m))
                    return Fail($"Expectation failed: '{_query.Path}' missing.", strict);

                if (!(m.IsArray && m.ValueType == ValueType.Ref))
                    return Fail($"Expect ObjectArray: '{_query.Path}' not object array.", strict);
                return Pass();
            }

            /// <summary>Expect object array element at index (requires Index() before Expect()).</summary>
            public StorageQuery ObjectElement(bool strict = true)
            {
                if (_query.ExpectFailed) return _query;
                if (_index < 0)
                    return Fail("Expect ObjectElement requires index.", strict);

                string full = _query.Path;
                int bracket = full.LastIndexOf('[');
                if (bracket < 0)
                    return Fail("Malformed indexed path.", strict);
                string parentPath = full.Substring(0, bracket);

                if (!_query._root.TryGetMember(parentPath, out var parent) ||
                    !(parent.IsArray && parent.ValueType == ValueType.Ref))
                    return Fail($"Expect ObjectElement: parent '{parentPath}' not object array.", strict);

                if (_index >= parent.ArrayLength)
                    return Fail($"Expect ObjectElement: index {_index} out of range.", strict);

                var arr = parent.AsArray();
                if (!arr.TryGetObject(_index, out var child) || child.IsNull)
                    return Fail($"Expect ObjectElement: element {_index} is null.", strict);

                return Pass();
            }

            /// <summary>Expect scalar of T (non-ref, non-array).</summary>
            public StorageQuery Scalar<T>(bool strict = true) where T : unmanaged
            {
                if (_query.ExpectFailed) return _query;
                if (!TryGetFullPathMember(out var m))
                    return Fail($"Expectation failed: '{_query.Path}' missing.", strict);

                if (m.IsArray || m.ValueType == ValueType.Ref)
                    return Fail($"Expect Scalar<{typeof(T).Name}>: '{_query.Path}' not scalar.", strict);

                var expected = TypeData.Of<T>().ValueType;
                if (m.ValueType != expected)
                    return Fail($"Expect Scalar<{expected}>: actual {m.ValueType}.", strict);

                return Pass();
            }

            /// <summary>Expect value array of T (including char16 for string).</summary>
            public StorageQuery ValueArray<T>(bool strict = true) where T : unmanaged
            {
                if (_query.ExpectFailed) return _query;
                if (!TryGetFullPathMember(out var m))
                    return Fail($"Expectation failed: '{_query.Path}' missing.", strict);

                // value array: is array && not ref
                if (!m.IsArray || m.ValueType == ValueType.Ref)
                    return Fail($"Expect ValueArray<{typeof(T).Name}>: '{_query.Path}' not value array.", strict);

                var expected = TypeData.Of<T>().ValueType;
                if (m.ValueType != expected)
                    return Fail($"Expect ValueArray<{expected}>: actual {m.ValueType}.", strict);

                return Pass();
            }

            /// <summary>Expect char16 value array (string sugar).</summary>
            public StorageQuery String(bool strict = true)
            {
                if (_query.ExpectFailed) return _query;
                if (!TryGetFullPathMember(out var m))
                    return Fail($"Expectation failed: '{_query.Path}' missing.", strict);

                if (!m.IsArray || m.ValueType != ValueType.Char16)
                    return Fail($"Expect String: '{_query.Path}' not char16 array.", strict);

                return Pass();
            }
        }

        /// <summary>
        /// Ensure semantics: create if missing; optionally override if existing type mismatches.
        /// </summary>
        public readonly struct EnsureStatement
        {
            private readonly StorageObject _root;
            private readonly string _path;

            internal EnsureStatement(StorageObject root, string path)
            {
                _root = root;
                _path = path;
            }

            public T Is<T>(bool allowOverride = false) where T : unmanaged
            {
                if (_root.TryReadPath<T>(_path, out var v))
                    return v;

                if (_root.TryGetMember(_path, out var member))
                {
                    var targetType = TypeData.Of<T>();
                    if (!member.Type.CanCastTo(targetType, true))
                    {
                        if (!allowOverride)
                            throw new InvalidOperationException($"Ensure.Is<{typeof(T).Name}> failed: '{_path}' incompatible type '{member.Type}'.");
                        _root.WritePath(_path, default(T));
                        return default;
                    }
                    if (allowOverride)
                        _root.WritePath(_path, default(T));
                    return default;
                }

                _root.WritePath(_path, default(T));
                return default;
            }

            public StorageArray IsArray<T>(int minLength = 0, bool allowOverride = false) where T : unmanaged
            {
                if (_root.TryGetArrayByPath<T>(_path.AsSpan(), out var arr))
                {
                    if (minLength > 0) arr.EnsureLength(minLength);
                    return arr;
                }

                if (_root.TryGetMember(_path, out var member))
                {
                    bool ok = _root.TryGetArrayByPath<T>(_path.AsSpan(), out _);
                    if (!ok)
                    {
                        if (!allowOverride)
                            throw new InvalidOperationException($"Ensure.IsArray<{typeof(T).Name}> failed: '{_path}' incompatible.");
                        var overridden = _root.GetArrayByPath<T>(_path.AsSpan(), true);
                        if (minLength > 0) overridden.EnsureLength(minLength);
                        return overridden;
                    }
                }

                var created = _root.GetArrayByPath<T>(_path.AsSpan(), true, overrideExisting: allowOverride);
                if (minLength > 0) created.EnsureLength(minLength);
                return created;
            }

            public StorageObject IsObject(bool allowOverride = false)
            {
                if (_root.TryGetObjectByPath(_path.AsSpan(), out var obj))
                    return obj;

                if (_root.TryGetMember(_path, out var member))
                {
                    if (!allowOverride)
                        throw new InvalidOperationException($"Ensure.IsObject failed: '{_path}' exists but not object.");
                    return _root.GetObjectByPath(_path, true);
                }

                return _root.GetObjectByPath(_path, true);
            }

            public StorageArray IsObjectArray(int minLength = 0, bool allowOverride = false)
            {
                if (_root.TryGetArrayByPath(_path.AsSpan(), TypeData.Ref, out var arr))
                {
                    if (minLength > 0) arr.EnsureLength(minLength);
                    return arr;
                }

                if (_root.TryGetMember(_path, out var member))
                {
                    if (!allowOverride)
                        throw new InvalidOperationException($"Ensure.IsObjectArray failed: '{_path}' incompatible.");
                    var overridden = _root.GetArrayByPath(_path.AsSpan(), TypeData.Ref, true);
                    if (minLength > 0) overridden.EnsureLength(minLength);
                    return overridden;
                }

                var created = _root.GetArrayByPath(_path.AsSpan(), TypeData.Ref, true);
                if (minLength > 0) created.EnsureLength(minLength);
                return created;
            }

            public override string ToString() => $"Ensure({_path})";
        }

        /// <summary>
        /// Exist semantics: read / inspect only; never creates or overrides.
        /// </summary>
        public readonly struct ExistStatement
        {
            private readonly StorageObject _root;
            private readonly string _path;

            internal ExistStatement(StorageObject root, string path)
            {
                _root = root;
                _path = path;
            }

            public bool Has => _root.TryGetMember(_path, out _);

            public bool Scalar<T>(bool exact) where T : unmanaged =>
                _root.TryGetMember(_path, out var m) && m.Type.CanCastTo(TypeUtil<T>.Type, exact);

            public bool Scalar<T>(out T value) where T : unmanaged =>
                _root.TryReadPath<T>(_path, out value);

            public T ScalarOrDefault<T>() where T : unmanaged =>
                _root.TryReadPath<T>(_path, out var v) ? v : default;

            public bool ArrayOf<T>(out StorageArray array) where T : unmanaged =>
                _root.TryGetArrayByPath<T>(_path.AsSpan(), out array);

            public bool ArrayOfObject(out StorageArray array) =>
                _root.TryGetArrayByPath(_path.AsSpan(), TypeData.Ref, out array);

            public bool As<T>(bool exact = false) where T : unmanaged
            {
                if (!_root.TryGetMember(_path, out var member))
                    return false;
                var typeData = TypeData.Of<T>();
                return member.Type.CanCastTo(typeData, exact);
            }

            public EnsureStatement Ensure() => new EnsureStatement(_root, _path);

            public override string ToString() => $"Exist({_path})";
            public static implicit operator bool(ExistStatement exist) => exist.Has;
        }
    }

    public static class StorageExtensions
    {
        public static StorageQuery Query(this StorageObject root) => new StorageQuery(root);
        public static StorageQuery Query(this StorageObject root, string first) => new StorageQuery(root, first);
        public static StorageQuery Location(this StorageObject root, string segment) => new StorageQuery(root).Location(segment);
        public static StorageQuery.EnsureStatement Ensure(this StorageObject root, string path) => new StorageQuery.EnsureStatement(root, path);
        public static StorageQuery.ExistStatement Exist(this StorageObject root, string path) => new StorageQuery.ExistStatement(root, path);

        public static void Demo(StorageObject root)
        {
            // Inline expectations (in-place check)
            var q = root.Query()
                        .Location("player").Expect().Object()
                        .Location("stats").Expect().Object()
                        .Location("hp").Expect().Scalar<int>();

            if (q.ExpectFailed)
            {
                // handle error
                Console.WriteLine(q.ExpectError);
            }
            else
            {
                int hp = q.Read<int>(); // full path already points to hp
            }

            // Object array element expectation
            var q2 = root.Query()
                         .Location("world").Expect().Object()
                         .Location("entities").Expect().ObjectArray()
                         .Index(2).Expect().Object(); // entity #2

            if (!q2.ExpectFailed)
            {
                // do something with entity #2
            }
        }
    }
}