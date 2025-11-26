using System;
using System.Runtime.CompilerServices;
using static Minerva.DataStorage.StorageQuery;

namespace Minerva.DataStorage
{
    public interface IStorageQuery<T> : IStorageQuery where T : struct, IStorageQuery<T>
    {
        T Index(int index);
        T Location(ReadOnlySpan<char> path);
        T Previous();
    }

    public interface IStorageQuery
    {
        internal StorageObject Root { get; }

        /// <summary>
        /// Path span
        /// </summary>
        ReadOnlySpan<char> PathSpan { get; }
        /// <summary>
        /// Result of the query.
        /// </summary>
        Result Result { get; internal set; }
        /// <summary>
        /// Is query disposed.
        /// </summary>
        bool IsDisposed { get; }

        /// <summary>
        /// The implicit finalizer called when the query is discarded without being Persist()ed.
        /// </summary>
        internal void ImplicitCallFinalizer();


        public static bool operator true(IStorageQuery query) => query.Result;
        public static bool operator false(IStorageQuery query) => !query.Result;
    }

    /// <summary>
    /// Deferred path query DSL. Does NOT create or mutate automatically.
    /// Use Ensure()/Exist() terminal objects to apply creation or checks.
    /// </summary>
    public struct StorageQuery : IStorageQuery<StorageQuery>, IStorageQuery, IDisposable
    {
        private readonly StorageObject _root;
        private readonly TempString _segments;
        private readonly int _generation;

        // In-place expectation state
        private Result _result;

        internal StorageQuery(StorageObject root)
        {
            _root = root;
            _result = Result.Succeeded;
            _segments = TempString.Create();
            _generation = _segments.Generation;
        }

        internal StorageQuery(StorageObject root, string first)
        {
            _root = root;
            _result = Result.Succeeded;
            _segments = TempString.Create(first.Length);
            _segments.Append(first);
            _generation = _segments.Generation;
        }

        internal StorageQuery(StorageObject root, TempString path)
        {
            _root = root;
            _result = Result.Succeeded;
            _segments = path;
            _generation = _segments.Generation;
        }


        /// <summary>Complete path as string.</summary>
        public readonly string Path => _segments.ToString();

        /// <summary> Is query disposed. </summary>
        public readonly bool IsDisposed => _segments.IsDisposed || _segments.Generation != _generation;

        /// <summary> Result of query. </summary>
        public readonly Result Result => _result;



        readonly ReadOnlySpan<char> IStorageQuery.PathSpan => _segments.Span;
        readonly StorageObject IStorageQuery.Root => _root;
        Result IStorageQuery.Result { readonly get => _result; set => _result = _result && value; }




        /// <summary>
        /// Append a path segment (may itself include '[index]' or dots you intend literally).
        /// </summary>
        public readonly StorageQuery Location(ReadOnlySpan<char> path)
        {
            if (path.Length == 0) throw new ArgumentException("Segment cannot be empty", nameof(path));
            if (_segments.Length > 0) _segments.Append('.');
            _segments.Append(path);
            return this;
        }

        /// <summary>
        /// Append an index to the last segment: turns 'items' into 'items[3]'.
        /// </summary>
        public readonly StorageQuery Index(int index)
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

        /// <summary>
        /// Begin persistent semantics: returns a Persistent object that holds the path.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// This is also a finalizer: disposes the StorageQuery's internal buffer. <br/>
        /// Remember to Dispose() the returned Persistent when done. otherwise object pool leaks.
        /// </remarks>
        public readonly Persistent Persist()
        {
            var perisistent = new Persistent(_root, _segments.Span);
            Dispose();
            return perisistent;
        }

        /// <summary>
        /// Trace back to previous segment.
        /// </summary>
        /// <returns></returns>
        public readonly StorageQuery Previous()
        {
            int lastDot = _segments.Span.LastIndexOf('.');
            _segments.Length = lastDot >= 0 ? lastDot : 0;
            return this;
        }





        readonly void IStorageQuery.ImplicitCallFinalizer()
        {
            Dispose();
        }

        public readonly void Dispose()
        {
            _segments.Dispose();
        }


        public readonly override string ToString() => $"Query({_segments})";



        public static implicit operator Result(StorageQuery query) => query._result;
        public static implicit operator bool(StorageQuery query) => query._result;
        public static bool operator true(StorageQuery query) => query._result;
        public static bool operator false(StorageQuery query) => !query._result;





        /// <summary>
        /// Reusable path view. no implicit dispose on finalization.
        /// Compared to plain string:
        /// - Holds pooled TempString for zero-allocation repeated reads/writes.
        /// - Exposes Span-based access (NameSpan).
        /// - Must be explicitly disposed to return buffer.
        /// Use this for high-frequency repeated operations, not for one-shot calls.
        /// </summary>
        public struct Persistent : IStorageQuery<Persistent>, IStorageQuery, IDisposable
        {
            private readonly StorageObject _root;
            private readonly TempString _segments;
            private readonly int _generation;
            private Result _result;

            internal Persistent(StorageObject root)
            {
                _root = root;
                _result = Result.Succeeded;
                _segments = TempString.Create();
                _generation = _segments.Generation;
            }

            internal Persistent(StorageObject root, ReadOnlySpan<char> path)
            {
                _root = root;
                _result = Result.Succeeded;
                _segments = TempString.Create(path);
                _generation = _segments.Generation;
            }

            public readonly ReadOnlySpan<char> PathSpan => _segments.Span;
            /// <summary> Is query disposed. </summary> 
            public readonly bool IsDisposed => _segments.IsDisposed || _segments.Generation != _generation;

            readonly StorageObject IStorageQuery.Root => _root;
            Result IStorageQuery.Result { readonly get => _result; set => _result = _result && value; }

            /// <summary> Result of query. </summary>
            public readonly Result Result => _result;




            /// <summary>
            /// Append a path segment (may itself include '[index]' or dots you intend literally).
            /// </summary>
            public readonly Persistent Location(ReadOnlySpan<char> path)
            {
                if (path.Length == 0) throw new ArgumentException("Segment cannot be empty", nameof(path));
                if (_segments.Length > 0) _segments.Append('.');
                _segments.Append(path);
                return this;
            }

            /// <summary>
            /// Append an index to the last segment: turns 'items' into 'items[3]'.
            /// </summary>
            public readonly Persistent Index(int index)
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

            /// <summary>
            /// Trace back to previous segment.
            /// </summary>
            /// <returns></returns>
            public readonly Persistent Previous()
            {
                int lastDot = _segments.Span.LastIndexOf('.');
                _segments.Length = lastDot >= 0 ? lastDot : 0;
                return this;
            }


            readonly void IStorageQuery.ImplicitCallFinalizer()
            {
                // Nothing to do here
            }

            public readonly void Dispose()
            {
                _segments.Dispose();
            }


            public static implicit operator Result(Persistent query) => query._result;
            public static implicit operator bool(Persistent query) => query._result;
            public static bool operator true(Persistent query) => query._result;
            public static bool operator false(Persistent query) => !query._result;
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
        public readonly struct ExpectStatement<TQuery> where TQuery : struct, IStorageQuery<TQuery>
        {
            private readonly TQuery _query;

            internal ExpectStatement(TQuery query)
            {
                _query = query;
            }

            internal string Path => _query.PathSpan.ToString();

            // Helper: returns member for full path (may include [index] which is not a field)
            private bool TryGetMember(out StorageMember member)
            {
                member = default;
                if (!_query.Result) return false;
                return _query.Root.TryGetMember(_query.PathSpan, out member);
            }

            private TQuery Fail(string msg, bool strict)
            {
                var q = _query;
                if (strict) q.Result = Result.Failed(msg);
                return q;
            }

            private TQuery Pass() => _query;

            /// <summary>Accept anything (never fails).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery Any() => _query;

            /// <summary>Expect scalar of T (non-ref, non-array).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public QueryResult<TQuery, TValue> Scalar<TValue>(bool strict = true) where TValue : unmanaged
            {
                TQuery next;
                TValue value = default;
                if (!_query.Result) next = _query;
                else if (!TryGetMember(out var m))
                    next = Fail($"Expectation failed: '{Path}' missing.", strict);
                else if (m.IsArray || m.ValueType == ValueType.Ref)
                    next = Fail($"Expect Scalar<{typeof(TValue).Name}>: '{Path}' not scalar.", strict);
                else
                {
                    var expected = TypeData.Of<TValue>().ValueType;
                    if (m.ValueType != expected)
                        next = Fail($"Expect Scalar<{expected}>: actual {m.ValueType}.", strict);
                    else
                    {
                        value = m.AsScalar().Read<TValue>();
                        next = Pass();
                    }
                }
                return next.Result.Success
                    ? new QueryResult<TQuery, TValue>(next, value)
                    : new QueryResult<TQuery, TValue>(next, next.Result.ErrorMessage);
            }

            /// <summary>Expect char16 value array (string sugar).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public QueryResult<TQuery, string> String(bool strict = true)
            {
                TQuery next;
                string value = default;
                if (!_query.Result) next = _query;
                else if (!TryGetMember(out var m))
                    next = Fail($"Expectation failed: '{Path}' missing.", strict);
                else if (!m.IsArray)
                    next = Fail($"Expect String: '{Path}' not an array.", strict);
                else if (m.AsArray().Type != ValueType.Char16)
                    next = Fail($"Expect String: '{Path}' not char16 array.", strict);
                else next = Pass();

                return next.Result.Success
                    ? new QueryResult<TQuery, string>(next, value: value)
                    : new QueryResult<TQuery, string>(next, next.Result.ErrorMessage);
            }

            /// <summary>Expect an object.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public QueryResult<TQuery, StorageObject> Object(bool strict = true)
            {
                TQuery next = Object(out var value, strict);
                return next.Result.Success
                    ? new QueryResult<TQuery, StorageObject>(next, value)
                    : new QueryResult<TQuery, StorageObject>(next, next.Result.ErrorMessage);
            }

            /// <summary>Expect an object.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery Object(out StorageObject storageObject, bool strict = true)
            {
                storageObject = default;
                if (!_query.Result) return _query;
                else if (!TryGetMember(out var m))
                    return Fail($"Expectation failed: '{Path}' missing.", strict);
                else if (!(m.ValueType == ValueType.Ref))
                    return Fail($"Expect Object: '{Path}' not object.", strict);
                else
                {
                    storageObject = m.AsObject();
                    return Pass();
                }
            }

            /// <summary>Expect object array element at index (requires Index() before Expect()).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public QueryResult<TQuery, StorageObject> ObjectElement(bool strict = true)
            {
                var next = ObjectElement(strict, out var storageObject);
                return next.Result.Success
                    ? new QueryResult<TQuery, StorageObject>(next, storageObject)
                    : new QueryResult<TQuery, StorageObject>(next, next.Result.ErrorMessage);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery ObjectElement(bool strict, out StorageObject storageObject)
            {
                storageObject = default;
                if (!_query.Result) return _query;
                int index = ReadIndex();
                if (index < 0)
                    return Fail("Expect ObjectElement requires index.", strict);

                var full = _query.PathSpan;
                int bracket = full.LastIndexOf('[');
                if (bracket < 0)
                    return Fail("Malformed indexed path.", strict);
                var parentPath = full[..bracket];

                if (!_query.Root.TryGetMember(parentPath, out var parent) ||
                    !(parent.IsArray && parent.ValueType == ValueType.Ref))
                    return Fail($"Expect ObjectElement: parent '{parentPath.ToString()}' not object array.", strict);

                if (index >= parent.ArrayLength)
                    return Fail($"Expect ObjectElement: index {index} out of range.", strict);

                var arr = parent.AsArray();
                if (!arr.TryGetObject(index, out storageObject) || storageObject.IsNull)
                    return Fail($"Expect ObjectElement: element {index} is null.", strict);

                return Pass();
            }

            /// <summary>Expect an object array (ref array).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery ObjectArray(bool strict = true) => ObjectArray(out _, strict);

            /// <summary>Expect an object array (ref array).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery ObjectArray(out StorageArray storageArray, bool strict = true)
            {
                storageArray = default;
                if (!_query.Result) return _query;
                if (!TryGetMember(out var m))
                    return Fail($"Expectation failed: '{Path}' missing.", strict);

                if (!(m.IsArray && m.ValueType == ValueType.Ref))
                    return Fail($"Expect ObjectArray: '{Path}' not object array.", strict);
                storageArray = m.AsArray();
                return Pass();
            }

            /// <summary>Expect value array of T (including char16 for string).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery ValueArray<TValue>(bool strict = true) where TValue : unmanaged => ValueArray<TValue>(out _, strict);

            /// <summary>Expect value array of T (including char16 for string).</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery ValueArray<TValue>(out StorageArray storageArray, bool strict = true) where TValue : unmanaged
            {
                storageArray = default;
                if (!_query.Result) return _query;
                if (!TryGetMember(out var m))
                    return Fail($"Expectation failed: '{Path}' missing.", strict);

                // is array
                if (!m.IsArray)
                    return Fail($"Expect ValueArray<{typeof(TValue).Name}>: '{Path}' not value array.", strict);

                var arr = m.AsArray();
                // not ref
                if (arr.Type == ValueType.Ref)
                    return Fail($"Expect ValueArray<{typeof(TValue).Name}>: '{Path}' is object array.", strict);

                if (!arr.IsConvertibleTo<TValue>())
                    return Fail($"Expect ValueArray<{typeof(TValue).Name}>: actual {m.ValueType}.", strict);

                storageArray = arr;
                return Pass();
            }

            private int ReadIndex()
            {
                if (_query.PathSpan.IsEmpty) throw new InvalidOperationException("Expect() requires at least one Location().");
                var full = _query.PathSpan;
                int idxStart = full.LastIndexOf('[');
                int idxEnd = full.LastIndexOf(']');
                if (idxStart >= 0 && idxEnd > idxStart && int.TryParse(full.Slice(idxStart + 1, idxEnd - idxStart - 1), out var parsed))
                {
                    return parsed;
                }
                return -1;
            }

            /// <summary>
            /// Finalize to ExistStatement for further operations.
            /// </summary>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ExistStatement A() => _query.Exist();
        }

        /// <summary>
        /// Ensure semantics: create if missing; optionally override if existing type mismatches.
        /// </summary>
        public readonly struct EnsureStatement<TQuery> where TQuery : struct, IStorageQuery<TQuery>
        {
            private readonly TQuery _query;

            internal EnsureStatement(TQuery query)
            {
                _query = query;
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public QueryResult<TQuery, T> Scalar<T>(bool allowOverride = false) where T : unmanaged
            {
                var value = MakeNew().Scalar<T>(allowOverride);
                return _query.CreateResult(value);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public QueryResult<TQuery, T> Scalar<T>(T value, bool allowOverride = false) where T : unmanaged
            {
                MakeNew().Scalar<T>(value, allowOverride);
                return _query.CreateResult(value);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public QueryResult<TQuery, string> String(bool allowOverride = false)
            {
                var s = MakeNew().String(allowOverride);
                return _query.CreateResult(s);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public QueryResult<TQuery, string> String(string value, bool allowOverride = false)
            {
                MakeNew().String(value, allowOverride);
                return _query.CreateResult(value);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public QueryResult<TQuery, StorageObject> Object(bool allowOverride = false)
            {
                var value = MakeNew().Object(allowOverride);
                return new QueryResult<TQuery, StorageObject>(_query, value);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery Object(out StorageObject storageObject, bool allowOverride = false)
            {
                storageObject = MakeNew().Object(allowOverride);
                return _query;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery Array<T>(int minLength = 0, bool allowOverride = false) where T : unmanaged
            {
                MakeNew().Array<T>(minLength, allowOverride);
                return _query;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery Array<T>(out StorageArray array, int minLength = 0, bool allowOverride = false) where T : unmanaged
            {
                array = MakeNew().Array<T>(minLength, allowOverride);
                return _query;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery ObjectArray(int minLength = 0, bool allowOverride = false)
            {
                MakeNew().ObjectArray(minLength, allowOverride);
                return _query;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public TQuery ObjectArray(out StorageArray array, int minLength = 0, bool allowOverride = false)
            {
                array = MakeNew().ObjectArray(minLength, allowOverride);
                return _query;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private MakeStatement MakeNew() => new(_query.Root, _query.PathSpan);


            /// <summary>
            /// Finalize to MakeStatement for further operations.
            /// </summary>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public MakeStatement Is() => _query.Make();

            public override string ToString() => $"Ensure({_query.PathSpan.ToString()})";
        }

        /// <summary>
        /// Make semantics: terminate the query, create value at given path if missing.
        /// </summary>
        public readonly ref struct MakeStatement
        {
            private readonly StorageObject _root;
            private readonly ReadOnlySpan<char> _path;

            internal MakeStatement(StorageObject root, ReadOnlySpan<char> path)
            {
                _root = root;
                _path = path;
            }

            internal MakeStatement(StorageObject root, string path)
            {
                _root = root;
                _path = path;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public T Scalar<T>(bool allowOverride = false) where T : unmanaged
            {
                if (_root.TryGetMember(_path, out var member))
                {
                    var targetType = TypeData.Of<T>();
                    // exact match
                    if (targetType == member.Type)
                        return member.AsScalar().Read<T>();
                    if (!allowOverride)
                        throw new InvalidOperationException($"Ensure.Is<{typeof(T).Name}> failed: '{_path.ToString()}' incompatible type '{member.Type}'.");
                    _root.WritePath(_path, default(T));
                    return default;
                }

                _root.WritePath(_path, default(T));
                return default;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Scalar<T>(T value, bool allowOverride = false) where T : unmanaged
            {
                if (_root.TryGetMember(_path, out var member))
                {
                    var targetType = TypeData.Of<T>();
                    // exact match
                    if (targetType == member.Type)
                    {
                        member.Write(value);
                        return;
                    }
                    if (!allowOverride)
                        throw new InvalidOperationException($"Ensure.Is<{typeof(T).Name}> failed: '{_path.ToString()}' incompatible type '{member.Type}'.");
                    _root.WritePath(_path, value);
                    return;
                }
                _root.WritePath(_path, value);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public string String(bool allowOverride = false) => Array<char>(0, allowOverride).AsString();

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void String(string value, bool allowOverride = false) => Array<char>(value.Length, allowOverride).Write(value);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StorageArray Array<T>(int minLength = 0, bool allowOverride = false) where T : unmanaged
            {
                if (_root.TryGetArrayByPath(_path, null, out var arr))
                {
                    if (!arr.IsConvertibleTo<T>())
                    {
                        if (!allowOverride)
                            throw new InvalidOperationException($"Ensure.IsArray<{typeof(T).Name}> failed: '{_path.ToString()}' incompatible.");
                        arr.Rescheme(TypeData.Of<T>(), minLength);
                    }
                    if (minLength > 0) arr.EnsureLength(minLength);
                    return arr;
                }

                if (_root.TryGetMember(_path, out var member))
                {
                    if (!allowOverride)
                        throw new InvalidOperationException($"Ensure.IsArray<{typeof(T).Name}> failed: '{_path.ToString()}' incompatible.");
                    member.ChangeFieldType(TypeData.Of<T>(), minLength);
                    arr = member.AsArray();
                    return arr;
                }
                else arr = _root.GetArrayByPath<T>(_path, true, overrideExisting: allowOverride);

                if (minLength > 0) arr.EnsureLength(minLength);
                return arr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StorageObject Object(bool allowOverride = false)
            {
                if (_root.TryGetObjectByPath(_path, out var obj))
                    return obj;

                if (_root.TryGetMember(_path, out var member))
                {
                    if (!allowOverride)
                        throw new InvalidOperationException($"Ensure.IsObject failed: '{_path.ToString()}' exists but not object.");
                    member.ChangeFieldType(TypeData.Ref, null);
                    return member.AsObject();
                }

                return _root.GetObjectByPath(_path, true);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StorageArray ObjectArray(int minLength = 0, bool allowOverride = false)
            {
                if (_root.TryGetArrayByPath(_path, null, out var arr))
                {
                    if (!arr.IsConvertibleTo(TypeData.Ref))
                    {
                        if (!allowOverride)
                            ThrowHelper.ThrowInvalidOperation($"Ensure.IsArray<Object> failed: '{_path.ToString()}' incompatible.");
                        arr.Rescheme(TypeData.Ref, minLength);
                    }
                    if (minLength > 0) arr.EnsureLength(minLength);
                    return arr;
                }

                if (_root.TryGetMember(_path, out var member))
                {
                    if (!allowOverride)
                        throw new InvalidOperationException($"Ensure.IsArray<Object> failed: '{_path.ToString()}' incompatible.");
                    member.ChangeFieldType(TypeData.Ref, minLength);
                    arr = member.AsArray();
                    return arr;
                }
                else arr = _root.GetArrayByPath(_path, TypeData.Ref, true, overrideExisting: allowOverride);

                if (minLength > 0) arr.EnsureLength(minLength);
                return arr;
            }

            public override string ToString() => $"Ensure({_path.ToString()})";
        }

        /// <summary>
        /// Exist semantics: read / inspect only; never creates or overrides.
        /// </summary>
        public readonly ref struct ExistStatement
        {
            private readonly StorageObject _root;
            private readonly string _path;

            /// <summary>
            /// Is exist in early-fail state (invalid root or path).
            /// </summary>
            private readonly bool Failed => _root.IsNull || _path == null;

            internal ExistStatement(StorageObject root, string path)
            {
                _root = root;
                _path = path;
            }

            public bool Has => !Failed && _root.TryGetMember(_path, out _);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Object() => !Failed && _root.TryGetMember(_path, out var m) && m.ValueType == ValueType.Ref;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Object(out StorageObject storageObject)
            {
                if (Failed)
                {
                    storageObject = default;
                    return false;
                }
                return _root.TryGetObjectByPath(_path.AsSpan(), out storageObject);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Scalar<T>(bool exact = false) where T : unmanaged =>
                !Failed && _root.TryGetMember(_path, out var m) && m.Type.CanCastTo(TypeUtil<T>.Type, exact);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Scalar<T>(out T value, bool exact = false) where T : unmanaged
            {
                value = default;
                if (Failed)
                    return false;
                if (!_root.TryGetMember(_path, out var m))
                    return false;
                if (!m.Type.CanCastTo(TypeUtil<T>.Type, exact))
                    return false;
                value = m.Read<T>();
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool String(out string value)
            {
                value = default;
                if (!ArrayOf<char>(out var arr))
                    return false;
                value = arr.AsString();
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ArrayOf<T>(out StorageArray array) where T : unmanaged
            {
                if (Failed)
                {
                    array = default;
                    return false;
                }
                return _root.TryGetArrayByPath<T>(_path.AsSpan(), out array);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ArrayOf(TypeData? type, out StorageArray array)
            {
                if (Failed)
                {
                    array = default;
                    return false;
                }
                return _root.TryGetArrayByPath(_path.AsSpan(), type, out array);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ArrayOfObject(out StorageArray array)
            {
                if (Failed)
                {
                    array = default;
                    return false;
                }
                return _root.TryGetArrayByPath(_path.AsSpan(), TypeData.Ref, out array);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ArrayOfAny(out StorageArray storageArray)
            {
                if (Failed)
                {
                    storageArray = default;
                    return false;
                }
                return _root.TryGetArrayByPath(_path.AsSpan(), null, out storageArray);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool As<T>(bool exact = false) where T : unmanaged
            {
                if (Failed) return false;
                if (!_root.TryGetMember(_path, out var member))
                    return false;
                var typeData = TypeData.Of<T>();
                return member.Type.CanCastTo(typeData, exact);
            }

            public override string ToString() => $"Exist({_path})";
            public static implicit operator bool(ExistStatement exist) => exist.Has;
        }
    }

    /// <summary>
    /// Query context, used for implicit finalization of queries in using blocks.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal readonly struct QueryImplicitDisposeContext<T> : IDisposable where T : struct, IStorageQuery
    {
        public readonly T Query;
        public QueryImplicitDisposeContext(T query)
        {
            Query = query;
        }

        public readonly void Dispose() => Query.ImplicitCallFinalizer();
    }


    /// <summary>
    /// A result tied to a specific query.
    /// </summary>
    /// <typeparam name="TQuery"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public readonly ref struct QueryResult<TQuery, TResult>
        where TQuery : struct, IStorageQuery<TQuery>
    {
        public readonly TQuery Query;
        public readonly Result<TResult> Result;

        public QueryResult(TQuery query, TResult value) : this()
        {
            Query = query;
            Result = value;
        }

        public QueryResult(TQuery query, Result<TResult> value) : this()
        {
            Query = query;
            Result = value;
        }

        public QueryResult(TQuery query, string error) : this()
        {
            Query = query;
            Result = DataStorage.Result.Failed<TResult>(error);
        }

        /// <summary>
        /// Retrieves the value from the query result and implicitly finalizes the query when the method returns.
        /// </summary>
        /// <param name="suppress">
        /// If true, do not throw when the underlying result indicates failure; instead return the default value
        /// for <typeparamref name="TResult"/> or the default contained value from <see cref="Result{TResult}.Value"/>.
        /// If false (the default), call <see cref="Result{TResult}.GetValueOrThrow"/> to throw an exception on failure.
        /// </param>
        /// <returns>
        /// The contained result value when the query succeeded. When the query failed and <paramref name="suppress"/>
        /// is true, returns the default value for <typeparamref name="TResult"/>.
        /// </returns>
        /// <remarks>
        /// This method is a finalizing operation: 
        /// After calling this method the associated query instance is considered finalized and must not be reused.
        /// </remarks>
        /// <exception cref="InvalidOperationException"> When <paramref name="suppress"/> is false and the underlying <see cref="Result{TResult}"/> indicates failure, calling <see cref="Result.GetValueOrThrow"/> may throw an exception. The concrete exception type is determined by the implementation of <see cref="Result{T}.GetValueOrThrow"/>.
        /// </exception>
        public TResult ExistOrThrow(bool suppress = false)
        {
            using QueryImplicitDisposeContext<TQuery> queryContext = new(Query);
            return suppress ? Result.Value : Result.GetValueOrThrow();
        }

        /// <summary>
        /// Attempt to retrieve the result value associated with this query and implicitly finalize the query.
        /// </summary>
        /// <param name="value">
        /// Output parameter that receives the result value when the query succeeded;
        /// otherwise it is set to the default value of <typeparamref name="TResult"/>.
        /// </param>
        /// <returns>
        /// <c>true</c> if the underlying <see cref="Result{TResult}"/> indicates success; otherwise <c>false</c>.
        /// </returns>
        /// <remarks>
        /// This method is a finalizing operation: 
        /// After calling this method the associated query instance is considered finalized and must not be reused.
        /// </remarks>
        public bool Exist(out TResult value)
        {
            using QueryImplicitDisposeContext<TQuery> queryContext = new(Query);
            if (Result.Success)
            {
                value = Result.Value;
                return true;
            }
            value = default;
            return false;
        }
    }

    public static class StorageExtensions
    {
        public static StorageQuery Query(this StorageArray root)
        {
            var handle = root.Handle;
            return new StorageQuery(new StorageObject(handle.Container)).Location(handle.Name);
        }
        public static StorageQuery Query(this StorageArray root, int index)
        {
            var handle = root.Handle;
            return new StorageQuery(new StorageObject(handle.Container)).Location($"{handle.Name.ToString()}[{index}]");
        }
        public static StorageQuery Index(this StorageArray root, int index) => Query(root, index);
        public static StorageQuery Query(this StorageObject root) => new StorageQuery(root);
        public static StorageQuery Query(this StorageObject root, string path) => new StorageQuery(root, path);
        public static StorageQuery Location(this StorageObject root, string path) => new StorageQuery(root, path);
        public static TQuery Location<TQuery>(this QueryResult<TQuery, StorageObject> queryResult, string path) where TQuery : struct, IStorageQuery<TQuery> => queryResult.Query.Location(path);
        public static TQuery Then<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery<TQuery> => query.Previous();
        public static TQuery Then<TQuery, TValue>(this QueryResult<TQuery, TValue> result) where TQuery : struct, IStorageQuery<TQuery> => result.Query.Previous();
        public static MakeStatement Make(this StorageObject root, string path) => new MakeStatement(root, path);
        public static ExistStatement Exist(this StorageObject root, string path) => new ExistStatement(root, path);

        public static EnsureStatement<StorageQuery> Ensure(this StorageObject root, string path) => new StorageQuery(root, path).Ensure();
        public static ExpectStatement<StorageQuery> Expect(this StorageObject root, string path) => new StorageQuery(root, path).Expect();
        public static QueryResult<TQuery, TValue> CreateResult<TQuery, TValue>(this TQuery query, TValue value) where TQuery : struct, IStorageQuery<TQuery>
        {
            return new QueryResult<TQuery, TValue>(query, value);
        }

        /// <summary>Get member view for this path.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StorageMember GetMember<TQuery>(this TQuery query, bool createIfMissing = true) where TQuery : struct, IStorageQuery<TQuery>
        {
            query.EnsureRootValid();
            var path = query.PathSpan.ToString();
            return createIfMissing ? query.Root.GetMember(path) : (query.Root.TryGetMember(path, out var m) ? m : default);
        }

        /// <summary>Try get member view (non-creating).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryGetMember<TQuery>(this TQuery query, out StorageMember member) where TQuery : struct, IStorageQuery<TQuery>
        {
            member = default;
            query.EnsureRootValid();
            if (query.Result && query.Root.TryGetMember(query.PathSpan, out member))
            {
                member = new StorageMember(member.StorageObject, member.Name.ToArray(), member.ArrayIndex);
                return true;
            }
            else return false;
        }






        /// <summary>
        /// Enter exist semantics (no creation).
        /// </summary>
        /// <remarks>
        /// This operation finalized the query. <br/>
        /// For normal query, Do NOT reuse the same StorageQuery after calling Make()/Exist() (they dispose internal state);
        /// but after Expect()/Exist() you may keep chaining. 
        /// </remarks>
        public static ExistStatement Exist<T>(this T query) where T : struct, IStorageQuery
        {
            if (!query.Result)
            {
                return default;
            }
            using QueryImplicitDisposeContext<T> queryContext = new(query);
            var e = new ExistStatement(query.Root, query.PathSpan.ToString());
            return e;
        }

        /// <summary>
        /// Enter Make semantics (creation allowed).
        /// </summary>
        /// <remarks>
        /// This operation finalized the query. <br/>
        /// For normal query, Do NOT reuse the StorageQuery after calling Make()/Exist() (they dispose internal state);
        /// but after Expect()/Exist() you may keep chaining. 
        /// </remarks>
        public static MakeStatement Make<T>(this T query) where T : struct, IStorageQuery
        {
            if (!query.Result)
            {
                query.ImplicitCallFinalizer();
                query.Result.ThrowIfFailed();
            }

            using QueryImplicitDisposeContext<T> queryContext = new(query);
            var e = new MakeStatement(query.Root, query.PathSpan.ToString());
            return e;
        }

        /// <summary>
        /// Enter ensure semantics (creation allowed).
        /// </summary> 
        public static EnsureStatement<TQuery> Ensure<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery<TQuery>
        {
            if (!query.Result)
            {
                query.ImplicitCallFinalizer();
                query.Result.ThrowIfFailed();
            }

            var e = new EnsureStatement<TQuery>(query);
            return e;
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
        public static ExpectStatement<TQuery> Expect<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery<TQuery>
        {
            var exp = new ExpectStatement<TQuery>(query);
            return exp;
        }



        /// <summary>Subscribe to writes for this path.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StorageSubscription Subscribe<T>(this T query, StorageMemberHandler handler) where T : struct, IStorageQuery
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            query.EnsureRootValid();

            using QueryImplicitDisposeContext<T> queryContext = new(query);
            var sub = query.Root.Subscribe(query.PathSpan.ToString(), handler);
            return sub;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void EnsureRootValid<T>(this T query) where T : struct, IStorageQuery
        {
            if (query.IsDisposed || query.Root.IsNull || query.Root.IsDisposed)
                ThrowHelper.ThrowDisposed("Root StorageObject invalid.");
        }










        /// <summary> Read scalar of type T (throws if not found or incompatible).</summary>
        /// <remarks> This method is a finalizer (short for <see cref="Exist{TQuery}(TQuery)"/>.<see cref="ExistStatement.Scalar{T}(bool)"/>)</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T Read<T>(this StorageQuery storageQuery) where T : unmanaged
            => storageQuery.Exist().Scalar(out T value) ? value : default;

        /// <summary> Write scalar T (creates intermediate nodes).</summary>
        /// <remarks> This method is a finalizer (short for <see cref="Make{TQuery}(TQuery)"/>.<see cref="MakeStatement.Scalar{T}(bool)"/>)</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Write<T>(this StorageQuery storageQuery, T value) where T : unmanaged
            => storageQuery.Make().Scalar(value);

        /// <summary> Try read scalar T.</summary>
        /// <remarks> This method is a finalizer (short for <see cref="Exist{TQuery}(TQuery)"/>.<see cref="ExistStatement.Scalar{T}(out T)"/>)</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRead<T>(this StorageQuery storageQuery, out T value) where T : unmanaged
            => storageQuery.Exist().Scalar(out value);
    }
}