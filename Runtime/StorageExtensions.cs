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
        private Result _result;

        internal StorageQuery(StorageObject root)
        {
            _root = root;
            _segments = new TempString(0);
            _result = Result.Succeeded;
        }

        internal StorageQuery(StorageObject root, string first)
        {
            _root = root;
            _segments = new TempString(first.Length);
            _segments.Append(first);
            _result = Result.Succeeded;
        }

        internal StorageQuery(StorageObject root, TempString path)
        {
            _root = root;
            _segments = path;
            _result = Result.Succeeded;
        }


        /// <summary>Complete path as string.</summary>
        public string Path => _segments.ToString();

        /// <summary>True when at least one segment added.</summary>
        public readonly bool HasSegments => _segments.Length > 0;

        /// <summary>True if any Expect() check failed.</summary>
        public readonly bool Failed => !_result;

        /// <summary>Error message from first failed Expect().</summary>
        public readonly string Error => _result.ErrorMessage;

        public readonly ReadOnlySpan<char> NameSpan => _segments.Span;




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

        /// <summary>Subscribe to writes for this path.</summary>
        public StorageSubscription Subscribe(StorageMemberHandler handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            EnsureRootValid();
            return _root.Subscribe(Path, handler);
        }




        #region Convert Senmantic

        /// <summary>
        /// Enter ensure semantics (creation allowed).
        /// </summary>
        /// <remarks>
        /// This operation finalized the query. <br/>
        /// Do NOT reuse the same StorageQuery after calling Ensure()/Exist() (they dispose internal state);
        /// but after Expect() you may keep chaining. 
        /// </remarks>
        public EnsureStatement Ensure()
        {
            var e = new EnsureStatement(_root, Path);
            _segments.Dispose();
            return e;
        }

        /// <summary>
        /// Enter exist semantics (no creation).
        /// </summary>
        /// <remarks>
        /// This operation finalized the query. <br/>
        /// Do NOT reuse the same StorageQuery after calling Ensure()/Exist() (they dispose internal state);
        /// but after Expect() you may keep chaining. 
        /// </remarks>
        public ExistStatement Exist()
        {
            var e = new ExistStatement(_root, Path);
            _segments.Dispose();
            return e;
        }

        /// <summary>
        /// Begin persistent semantics: returns a Persistent object that holds the path.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// Remember to Dispose() the returned Persistent when done. otherwise object pool leaks.
        /// </remarks>
        public Persistent Persist()
        {
            var perisistent = new Persistent(_root, NameSpan);
            Dispose();
            return perisistent;
        }

        /// <summary>
        /// Begin the non-intrusive Expect DSL for the current accumulated path.  
        /// </summary>
        /// <remarks> 
        /// Usage pattern(chaining):
        /// <code>
        ///   root.Query()
        ///       .Location("player").Expect().Object()
        ///       .Location("stats").Expect().Object()
        ///       .Location("hp").Expect().Scalar&lt;int&gt;();
        /// </code>
        /// Do NOT reuse the same StorageQuery after calling Ensure()/Exist() (they dispose internal state);
        /// but after Expect() you may keep chaining.
        /// </remarks>
        public readonly ExpectStatement Expect()
        {
            var exp = new ExpectStatement(this);
            return exp;
        }


        #endregion





        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly void EnsureRootValid()
        {
            if (_root.IsNull || _root.IsDisposed)
                ThrowHelper.ThrowDisposed("Root StorageObject invalid.");
        }

        public void Dispose()
        {
            _segments.Dispose();
            _root = default;
        }

        internal void SetFail(string message) => _result = _result && Result.Failed(message);

        public override string ToString() => $"Query({Path})";

        public static implicit operator Result(StorageQuery query) => query._result;
        public static implicit operator bool(StorageQuery query) => query._result;



        /// <summary>
        /// Frozen reusable path view.
        /// Compared to plain string:
        /// - Holds pooled TempString for zero-allocation repeated reads/writes.
        /// - Exposes Span-based access (NameSpan).
        /// - Must be explicitly disposed to return buffer.
        /// Use this for high-frequency repeated operations, not for one-shot calls.
        /// </summary>
        public struct Persistent : IDisposable
        {
            private readonly StorageObject _root;
            private TempString _path;

            internal Persistent(StorageObject root, ReadOnlySpan<char> path)
            {
                _root = root;
                _path = new TempString(path);
            }

            public readonly ReadOnlySpan<char> NameSpan => _path.Span;

            /// <summary>Get member view for this path.</summary>
            public readonly StorageMember GetMember(bool createIfMissing = true)
            {
                EnsureRootValid();
                var path = NameSpan;
                return createIfMissing ? _root.GetMember(path) : (_root.TryGetMember(path, out var m) ? m : default);
            }

            /// <summary>Try get member view (non-creating).</summary>
            public readonly bool TryGetMember(out StorageMember member)
            {
                EnsureRootValid();
                return _root.TryGetMember(NameSpan, out member);
            }

            /// <summary>Read scalar of type T (throws if not found or incompatible).</summary>
            public readonly T Read<T>() where T : unmanaged
            {
                EnsureRootValid();
                return _root.ReadPath<T>(NameSpan);
            }

            /// <summary>Try read scalar T.</summary>
            public readonly bool TryRead<T>(out T value) where T : unmanaged
            {
                EnsureRootValid();
                return _root.TryReadPath<T>(NameSpan, out value);
            }

            /// <summary>Write scalar T (creates intermediate nodes).</summary>
            public readonly void Write<T>(T value) where T : unmanaged
            {
                EnsureRootValid();
                _root.WritePath(NameSpan, value);
            }

            /// <summary>Read string.</summary>
            public readonly string ReadString()
            {
                EnsureRootValid();
                return _root.ReadStringPath(NameSpan);
            }

            /// <summary>Write string.</summary>
            public readonly void WriteString(string value)
            {
                EnsureRootValid();
                _root.WritePath(NameSpan, value);
            }

            /// <summary>Get object if path resolves to object reference; returns null object if missing.</summary>
            public readonly StorageObject GetObjectOrDefault()
            {
                EnsureRootValid();
                return _root.GetObjectByPath(NameSpan, createIfMissing: false);
            }

            /// <summary>Get array view (throws if not array).</summary>
            public readonly StorageArray Array<T>() where T : unmanaged
            {
                EnsureRootValid();
                return _root.GetArrayByPath<T>(NameSpan, false);
            }

            /// <summary>Try get array view.</summary>
            public readonly bool TryArray<T>(out StorageArray array) where T : unmanaged
            {
                EnsureRootValid();
                return _root.TryGetArrayByPath<T>(NameSpan, out array);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private readonly void EnsureRootValid()
            {
                if (_root.IsNull || _root.IsDisposed)
                    ThrowHelper.ThrowDisposed("Root StorageObject invalid.");
            }




            #region Convert Senmantic

            /// <summary>
            /// Enter ensure semantics (creation allowed).
            /// </summary> 
            public EnsureStatement Ensure()
            {
                var e = new EnsureStatement(_root, _path.ToString());
                return e;
            }

            /// <summary>
            /// Enter exist semantics (no creation).
            /// </summary> 
            public ExistStatement Exist()
            {
                var e = new ExistStatement(_root, _path.ToString());
                return e;
            }

            #endregion
            public void Dispose()
            {
                _path.Dispose();
            }
        }




        /// <summary> 
        /// Expect semantics <br/>
        /// What Expect does: <br/>
        /// 1. Non‑mutating: Never creates or alters containers/fields; only inspects the existing structure. <br/>
        /// 2. Declarative checks: Each specific predicate (Object/ObjectArray/ObjectElement/Scalar&lt;T&gt;/ValueArray&lt;T&gt;/String/Any)
        ///    returns the underlying StorageQuery so you can continue with Location(), Index(), or another Expect(). <br/>
        /// 3. Short‑circuit on first strict failure: The first failing strict (strict=true) predicate sets 
        ///    StorageQuery.ExpectFailed = true and stores the message in ExpectError. Remaining predicates are no‑ops. <br/>
        /// 4. Soft checks: Passing strict=false on a predicate suppresses recording failure (the check is effectively ignored if it fails). <br/>
        /// 5. Indexed element assertions: For object array elements, use Index(n).Expect().ObjectElement(); it validates parent is an object array, 
        ///    bounds, and non-null element. <br/>
        /// 6. Post‑expect operations: You can still call Read/Write/Ensure/Exist after any number of Expect() calls; Expect does not finalize
        ///    the query or dispose its internal path buffer. <br/>
        /// 7. Error retrieval: Inspect ExpectFailed / ExpectError instead of relying on exceptions (only blatantly invalid usage throws,
        ///    e.g. Index() before any Location()). <br/>
        ///
        /// When to use: <br/>
        /// - Incrementally validating a deep path while constructing it. <br/>
        /// - Pre-flight structure/type checks before performing mutations (e.g. only write if all expectations pass). <br/>
        ///
        /// Compared with Ensure / Exist: <br/>
        /// - Ensure: Creates (and can override) and finalizes the query (returns EnsureStatement). <br/>
        /// - Exist : Read-only presence/type inspection; finalizes the query (returns ExistStatement). <br/>
        /// - Expect: Lightweight assertions, does NOT finalize; purely diagnostic. <br/>
        /// </summary>
        public readonly struct ExpectStatement
        {
            private readonly StorageQuery _query;

            internal ExpectStatement(StorageQuery query)
            {
                _query = query;
            }

            // Helper: returns member for full path (may include [index] which is not a field)
            private bool TryGetMember(out StorageMember member)
            {
                member = default;
                if (_query.Failed) return false;
                return _query._root.TryGetMember(_query.NameSpan, out member);
            }

            private StorageQuery Fail(string msg, bool strict)
            {
                var q = _query;
                if (strict) q.SetFail(msg);
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
                if (_query.Failed) return _query;
                if (!TryGetMember(out var m))
                    return Fail($"Expectation failed: '{_query.Path}' missing.", strict);

                if (!(m.ValueType == ValueType.Ref && !m.IsArray))
                    return Fail($"Expect Object: '{_query.Path}' not object.", strict);
                return Pass();
            }

            /// <summary>Expect an object array (ref array).</summary>
            public StorageQuery ObjectArray(bool strict = true)
            {
                if (_query.Failed) return _query;
                if (!TryGetMember(out var m))
                    return Fail($"Expectation failed: '{_query.Path}' missing.", strict);

                if (!(m.IsArray && m.ValueType == ValueType.Ref))
                    return Fail($"Expect ObjectArray: '{_query.Path}' not object array.", strict);
                return Pass();
            }

            /// <summary>Expect object array element at index (requires Index() before Expect()).</summary>
            public StorageQuery ObjectElement(bool strict = true)
            {
                if (_query.Failed) return _query;
                int index = ReadIndex();
                if (index < 0)
                    return Fail("Expect ObjectElement requires index.", strict);

                var full = _query.NameSpan;
                int bracket = full.LastIndexOf('[');
                if (bracket < 0)
                    return Fail("Malformed indexed path.", strict);
                var parentPath = full[..bracket];

                if (!_query._root.TryGetMember(parentPath, out var parent) ||
                    !(parent.IsArray && parent.ValueType == ValueType.Ref))
                    return Fail($"Expect ObjectElement: parent '{parentPath.ToString()}' not object array.", strict);

                if (index >= parent.ArrayLength)
                    return Fail($"Expect ObjectElement: index {index} out of range.", strict);

                var arr = parent.AsArray();
                if (!arr.TryGetObject(index, out var child) || child.IsNull)
                    return Fail($"Expect ObjectElement: element {index} is null.", strict);

                return Pass();
            }

            /// <summary>Expect scalar of T (non-ref, non-array).</summary>
            public StorageQuery Scalar<T>(bool strict = true) where T : unmanaged
            {
                if (_query.Failed) return _query;
                if (!TryGetMember(out var m))
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
                if (_query.Failed) return _query;
                if (!TryGetMember(out var m))
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
                if (_query.Failed) return _query;
                if (!TryGetMember(out var m))
                    return Fail($"Expectation failed: '{_query.Path}' missing.", strict);

                if (!m.IsArray || m.ValueType != ValueType.Char16)
                    return Fail($"Expect String: '{_query.Path}' not char16 array.", strict);

                return Pass();
            }

            private int ReadIndex()
            {
                if (!_query.HasSegments) throw new InvalidOperationException("Expect() requires at least one Location().");
                var full = _query.NameSpan;
                int idxStart = full.LastIndexOf('[');
                int idxEnd = full.LastIndexOf(']');
                if (idxStart >= 0 && idxEnd > idxStart && int.TryParse(full.Slice(idxStart + 1, idxEnd - idxStart - 1), out var parsed))
                {
                    return parsed;
                }
                return -1;
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

            public void Is<T>(T value, bool allowOverride = false) where T : unmanaged
            {
                if (_root.TryGetMember(_path, out var member))
                {
                    var targetType = TypeData.Of<T>();
                    if (!member.Type.CanCastTo(targetType, true))
                    {
                        if (!allowOverride)
                            throw new InvalidOperationException($"Ensure.Is<{typeof(T).Name}> failed: '{_path}' incompatible type '{member.Type}'.");
                        _root.WritePath(_path, value);
                        return;
                    }
                    if (allowOverride)
                        _root.WritePath(_path, value);
                    return;
                }
                _root.WritePath(_path, value);
            }

            public void Is(string value, bool allowOverride = false) => IsArray<char>(value.Length, allowOverride).Write(value);

            public StorageArray IsArray<T>(int minLength = 0, bool allowOverride = false) where T : unmanaged
            {
                if (_root.TryGetArrayByPath<T>(_path.AsSpan(), out var arr))
                {
                    if (minLength > 0) arr.EnsureLength(minLength);
                    return arr;
                }

                if (_root.TryGetMember(_path, out var member))
                {
                    bool ok = member.StorageObject.TryGetArray<T>(member.Name, out arr);
                    if (!ok)
                    {
                        if (!allowOverride)
                            throw new InvalidOperationException($"Ensure.IsArray<{typeof(T).Name}> failed: '{_path}' incompatible.");
                        arr = _root.GetArrayByPath<T>(_path.AsSpan(), true, overrideExisting: true);
                    }
                }
                else arr = _root.GetArrayByPath<T>(_path.AsSpan(), true, overrideExisting: allowOverride);

                if (minLength > 0) arr.EnsureLength(minLength);
                return arr;
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

            public bool Object() => _root.TryGetMember(_path, out var m) && m.ValueType == ValueType.Ref;

            public bool Object(out StorageObject storageObject) => _root.TryGetObjectByPath(_path.AsSpan(), out storageObject);

            public bool Scalar<T>(bool exact) where T : unmanaged =>
                _root.TryGetMember(_path, out var m) && m.Type.CanCastTo(TypeUtil<T>.Type, exact);

            public bool Scalar<T>(out T value) where T : unmanaged =>
                _root.TryReadPath<T>(_path, out value);

            public T ScalarOrDefault<T>() where T : unmanaged =>
                _root.TryReadPath<T>(_path, out var v) ? v : default;

            public bool ArrayOf<T>(out StorageArray array) where T : unmanaged
                => _root.TryGetArrayByPath<T>(_path.AsSpan(), out array);

            public bool ArrayOf(TypeData? type, out StorageArray array)
                => _root.TryGetArrayByPath(_path.AsSpan(), type, out array);

            public bool ArrayOfObject(out StorageArray array)
                => _root.TryGetArrayByPath(_path.AsSpan(), TypeData.Ref, out array);

            public bool ArrayOfAny(out StorageArray storageArray)
                => _root.TryGetArrayByPath(_path.AsSpan(), null, out storageArray);

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

        public readonly struct Result : IEquatable<Result>
        {
            public static readonly Result Succeeded = new(true, null);

            public readonly bool Success;
            public readonly string ErrorMessage;

            public Result(bool success, string errorMessage)
            {
                Success = success;
                ErrorMessage = errorMessage;
            }

            public static Result Failed(string message) => new Result(false, message);

            public bool Equals(Result other) => Success == other.Success && ErrorMessage == other.ErrorMessage;
            public override bool Equals(object obj) => obj is Result other && Equals(other);
            public override int GetHashCode() => HashCode.Combine(Success, ErrorMessage);
            public static bool operator ==(Result left, Result right) => left.Equals(right);
            public static bool operator !=(Result left, Result right) => !(left == right);
            public static Result operator &(Result left, Result right) => !left.Success ? left : right;
            public static Result operator |(Result left, Result right) => left.Success ? left : right;
            public static bool operator true(Result result) => result.Success;
            public static bool operator false(Result result) => !result.Success;

            public static implicit operator bool(Result result) => result.Success;

        }
    }

    public static class StorageExtensions
    {
        public static StorageQuery Query(this StorageObject root) => new StorageQuery(root);
        public static StorageQuery Query(this StorageObject root, string first) => new StorageQuery(root, first);
        public static StorageQuery Location(this StorageObject root, string segment) => new StorageQuery(root).Location(segment);
        public static StorageQuery.EnsureStatement Ensure(this StorageObject root, string path) => new StorageQuery.EnsureStatement(root, path);
        public static StorageQuery.ExistStatement Exist(this StorageObject root, string path) => new StorageQuery.ExistStatement(root, path);



        /// <summary>Read scalar of type T (throws if not found or incompatible).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T Read<T>(this StorageQuery storageQuery) where T : unmanaged
            => storageQuery.Exist().Scalar(out T value) ? value : default;

        /// <summary>Try read scalar T.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRead<T>(this StorageQuery storageQuery, out T value) where T : unmanaged
            => storageQuery.Exist().Scalar(out value);

        /// <summary>Read string.</summary>
        /// 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ReadString(this StorageQuery storageQuery)
            => storageQuery.Exist().ArrayOf<char>(out var arr) ? arr.ToString() : null;

        /// <summary>Get object if path resolves to object reference; returns null object if missing.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StorageObject GetObjectOrDefault(this StorageQuery storageQuery)
            => storageQuery.Exist().Object(out var obj) ? obj : default;

        /// <summary>Get array view (throws if not exist or not an array of T).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StorageArray Array<T>(this StorageQuery storageQuery) where T : unmanaged
            => storageQuery.Exist().ArrayOf<T>(out var arr) ? arr : throw new InvalidOperationException($"Path '{storageQuery.Path}' is not array of {typeof(T).Name}.");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StorageArray Array(this StorageQuery storageQuery, TypeData? typeData = null)
            => storageQuery.Exist().ArrayOf(typeData, out var arr) ? arr : throw new InvalidOperationException($"Path '{storageQuery.Path}' is not array of {(typeData?.ToString() ?? "any")}.");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StorageArray ArrayOfAny(this StorageQuery storageQuery)
            => storageQuery.Array(null);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StorageArray ArrayOfObject(this StorageQuery storageQuery)
            => storageQuery.Exist().ArrayOfObject(out var arr) ? arr : throw new InvalidOperationException($"Path '{storageQuery.Path}' is not object array.");

        /// <summary>Try get array view.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryArray<T>(this StorageQuery storageQuery, out StorageArray array) where T : unmanaged
            => storageQuery.Exist().ArrayOf(TypeUtil<T>.Type, out array);

        /// <summary>Try get array view.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryArray(this StorageQuery storageQuery, out StorageArray array, TypeData? type = null)
            => storageQuery.Exist().ArrayOf(type, out array);




        /// <summary>Write scalar T (creates intermediate nodes).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Write<T>(this StorageQuery storageQuery, T value) where T : unmanaged
            => storageQuery.Ensure().Is(value);

        /// <summary>Write string.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteString(this StorageQuery storageQuery, string value)
            => storageQuery.Ensure().Is(value);
    }
}