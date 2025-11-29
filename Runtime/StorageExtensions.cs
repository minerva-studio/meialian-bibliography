using Newtonsoft.Json.Linq;
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

    public interface INonterminalExpression
    {
        /// <summary>
        /// Result of the query.
        /// </summary>
        Result GetResult();

        /// <summary>
        /// The implicit finalizer called when the query is discarded without being Persist()ed.
        /// </summary>
        void ImplicitCallFinalizer();
    }

    public interface IStorageQuery : INonterminalExpression
    {
        internal StorageObject Root { get; }
        /// <summary>
        /// Path span
        /// </summary>
        ReadOnlySpan<char> PathSpan { get; }
        /// <summary>
        /// Is query disposed.
        /// </summary>
        bool IsDisposed { get; }
        /// <summary>
        /// Result of the query.
        /// </summary>
        Result Result { get; internal set; }

        public static bool operator true(IStorageQuery query) => query.Result.Success;
        public static bool operator false(IStorageQuery query) => !query.Result.Success;
    }

    /// <summary>
    /// Deferred path query DSL. Does NOT create or mutate automatically.
    /// Use Ensure()/Exist() terminal objects to apply creation or checks.
    /// </summary>
    public struct StorageQuery : IStorageQuery<StorageQuery>, IStorageQuery, IDisposable, INonterminalExpression
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

        public readonly StorageQuery this[int index] => Index(index);



        readonly ReadOnlySpan<char> IStorageQuery.PathSpan => _segments.Span;
        readonly StorageObject IStorageQuery.Root => _root;
        Result IStorageQuery.Result { readonly get => _result; set => _result = _result && value; }




        /// <summary>
        /// Append a path segment (may itself include '[index]' or dots you intend literally).
        /// </summary>
        public readonly StorageQuery Location(ReadOnlySpan<char> path)
        {
            if (path.Length == 0) return this;
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
            MoveBack(_segments);
            return this;
        }

        readonly void INonterminalExpression.ImplicitCallFinalizer()
        {
            Dispose();
        }

        /// <summary> Result of query. </summary>
        public readonly Result GetResult()
        {
            return _result;
        }

        public readonly void Dispose()
        {
            _segments.Dispose();
        }


        private static void MoveBack(TempString _segments)
        {
            int lastDot = _segments.Span.LastIndexOf('.');
            int lastBracket = _segments.Span.LastIndexOf('[');
            int lastSep = Math.Max(lastDot, lastBracket >= 0 && lastBracket > lastDot ? lastBracket - 1 : -1);
            _segments.Length = lastSep >= 0 ? lastSep : 0;
        }

        public readonly override string ToString() => $"Query({_segments}:{_result})";



        public static implicit operator Result(StorageQuery query) => query._result;
        public static implicit operator bool(StorageQuery query) => query._result;
        public static bool operator true(StorageQuery query) => query._result;
        public static bool operator false(StorageQuery query) => !query._result;




        public struct NestedQuery<TQuery> : IStorageQuery<NestedQuery<TQuery>>, IDisposable where TQuery : struct, IStorageQuery<TQuery>
        {
            private readonly TQuery _parent;
            private Result _result;
            private TempString _segments;

            public NestedQuery(TQuery query)
            {
                _parent = query;
                _result = query.Result;
                _segments = TempString.Create(query.PathSpan);
            }

            public NestedQuery(TQuery query, bool result)
            {
                _parent = query;
                _result = result ? Result.Succeeded : Result.Failed("Failed");
                _segments = TempString.Create(query.PathSpan);
            }

            public NestedQuery(TQuery query, Result result)
            {
                _parent = query;
                _result = result;
                _segments = TempString.Create(query.PathSpan);
            }

            public readonly TQuery MainQuery => _parent;
            public readonly ReadOnlySpan<char> PathSpan => _segments.Span;
            public readonly bool IsDisposed => _segments.IsDisposed;
            StorageObject IStorageQuery.Root => _parent.Root;
            Result IStorageQuery.Result { readonly get => _result; set => _result = value; }
            public readonly Result GetResult()
            {
                return _result;
            }

            public readonly void Dispose() => _segments.Dispose();

            readonly void INonterminalExpression.ImplicitCallFinalizer() => ImplicitCallFinalizer();
            internal readonly void ImplicitCallFinalizer() => Dispose();

            public readonly NestedQuery<TQuery> Index(int index)
            {
                _segments.Append('[');
                Span<char> chars = stackalloc char[11];
                index.TryFormat(chars, out int written);
                _segments.Append(chars[..written]);
                _segments.Append(']');
                return this;
            }

            public readonly NestedQuery<TQuery> Location(ReadOnlySpan<char> path)
            {
                if (path.Length == 0) return this;
                if (_segments.Length > 0) _segments.Append('.');
                _segments.Append(path);
                return this;
            }

            public readonly NestedQuery<TQuery> Previous()
            {
                MoveBack(_segments);
                return this;
            }



            public readonly override string ToString() => $"{_parent} -> Nested({_segments}:{_result})";
        }


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

            /// <summary> Result of query. </summary>
            public readonly Result GetResult()
            {
                return _result;
            }

            Result IStorageQuery.Result { readonly get => _result; set => _result = _result && value; }

            public readonly Persistent this[int index] => Index(index);




            /// <summary>
            /// Append a path segment (may itself include '[index]' or dots you intend literally).
            /// </summary>
            public readonly Persistent Location(ReadOnlySpan<char> path)
            {
                if (path.Length == 0) return this;
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
                MoveBack(_segments);
                return this;
            }


            readonly void INonterminalExpression.ImplicitCallFinalizer()
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

            public readonly override string ToString() => $"Persistent Query({_segments}:{_result})";
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
        public readonly struct ExpectStatement<TQuery> : INonterminalExpression where TQuery : struct, IStorageQuery<TQuery>
        {
            private readonly TQuery _query;

            internal ExpectStatement(TQuery query)
            {
                _query = query;
            }

            internal string Path => _query.PathSpan.ToString();
            public readonly Result GetResult()
            {
                return _query.Result;
            }

            // Helper: returns member for full path (may include [index] which is not a field)
            private bool TryGetMember(out StorageMember member)
            {
                member = default;
                if (!_query.Result.Success) return false;
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
                if (!_query.Result.Success) next = _query;
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
                        value = m.AsScalar<TValue>();
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
                if (!_query.Result.Success) next = _query;
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
                if (!_query.Result.Success) return _query;
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
                if (!_query.Result.Success) return _query;
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
                if (!_query.Result.Success) return _query;
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
                if (!_query.Result.Success) return _query;
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

            void INonterminalExpression.ImplicitCallFinalizer() => _query.ImplicitCallFinalizer();
        }

        /// <summary>
        /// Ensure semantics: create if missing; optionally override if existing type mismatches.
        /// </summary>
        public readonly struct EnsureStatement<TQuery> : INonterminalExpression where TQuery : struct, IStorageQuery<TQuery>
        {
            private readonly TQuery _query;

            internal EnsureStatement(TQuery query)
            {
                _query = query;
            }

            public readonly Result GetResult()
            {
                return _query.Result;
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


            void INonterminalExpression.ImplicitCallFinalizer() => _query.ImplicitCallFinalizer();

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
                        return member.AsScalar<T>();
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
            private readonly Result Failed => Result.From(_root.IsNull || _path == null, "Object is null");

            internal ExistStatement(StorageObject root, string path)
            {
                _root = root;
                _path = path ?? string.Empty;
            }

            public Result Has => !Failed && Result.From(_root.TryGetMember(_path, out _));

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result Object() => !Failed && Result.From(_root.TryGetMember(_path, out var m) && m.ValueType == ValueType.Ref);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result Object(out StorageObject storageObject)
            {
                storageObject = default;
                return !Failed && Result.From(_root.TryGetObjectByPath(_path.AsSpan(), out storageObject));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result Scalar<T>(bool exact = false) where T : unmanaged =>
                !Failed && Result.From(_root.TryGetMember(_path, out var m) && m.Type.CanCastTo(TypeUtil<T>.Type, exact));

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result Scalar<T>(out T value, bool exact = false) where T : unmanaged
            {
                value = default;
                if (Failed)
                    return Failed;
                if (!_root.TryGetMember(_path, out var m))
                    return Result.Failed("path not exist");
                if (!m.Type.CanCastTo(TypeUtil<T>.Type, exact))
                    return Result.Failed("type mismatch");
                value = m.Read<T>();
                return Result.Succeeded;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result String(out string value)
            {
                value = default;
                var r = !Array<char>(out var arr);
                if (!r) return r;
                value = arr.AsString();
                return Result.Succeeded;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result Array<T>(out StorageArray array) where T : unmanaged
            {
                array = default;
                return !Failed && Result.From(_root.TryGetArrayByPath<T>(_path.AsSpan(), out array));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result Array(TypeData? type, out StorageArray array)
            {
                array = default;
                return !Failed && Result.From(_root.TryGetArrayByPath(_path.AsSpan(), type, out array));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result ObjectArray(out StorageArray array)
            {
                array = default;
                return !Failed && Result.From(_root.TryGetArrayByPath(_path.AsSpan(), TypeData.Ref, out array));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result Array(out StorageArray array)
            {
                array = default;
                return !Failed && Result.From(_root.TryGetArrayByPath(_path.AsSpan(), null, out array));

            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result As<T>(bool exact = false) where T : unmanaged
            {
                if (Failed) return Failed;
                if (!_root.TryGetMember(_path, out var member))
                    return Result.Failed("path not exist");
                var typeData = TypeData.Of<T>();
                return Result.From(member.Type.CanCastTo(typeData, exact));
            }

            public override string ToString() => $"Exist({_path})";
            public static implicit operator bool(ExistStatement exist) => exist.Has;
        }

        public readonly ref struct DoStatement
        {
            private readonly StorageObject _root;
            private readonly string _path;

            /// <summary>
            /// Is exist in early-fail state (invalid root or path).
            /// </summary>
            private readonly bool Failed => _root.IsNull || _path == null;

            internal DoStatement(StorageObject root, string path)
            {
                _root = root;
                _path = path ?? string.Empty;
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result Delete(string fieldName)
            {
                if (Failed) return Result.Failed($"Delete failed: object does not exist");
                if (!_root.TryGetMember(fieldName, out var member))
                {
                    return Result.Failed($"Delete failed: field {fieldName} does not exist");
                }
                try
                {
                    member.StorageObject.Delete(fieldName);
                    return Result.Succeeded;
                }
                catch (Exception e)
                {
                    return Result.Failed($"Delete failed: {e.Message}");
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Result Rename(string source, string destination)
            {
                if (Failed) return Result.Failed($"MoveField failed: object does not exist");

                if (!_root.TryGetMember(source, out var member))
                {
                    return Result.Failed($"MoveField failed: field {source} does not exist");
                }
                try
                {
                    member.StorageObject.Move(source, destination);
                    return Result.Succeeded;
                }
                catch (Exception e)
                {
                    return Result.Failed($"MoveField failed: {e.Message}");
                }
            }


            public MakeStatement Make() => new(_root, _path);
            public ExistStatement Exist() => new(_root, _path);
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
    public readonly struct QueryResult<TQuery, TResult> : INonterminalExpression
        where TQuery : struct, IStorageQuery<TQuery>
    {
        public readonly TQuery Query;
        private readonly Result<TResult> result;


        public QueryResult(TQuery query, TResult value) : this()
        {
            Query = query;
            result = value;
        }

        public QueryResult(TQuery query, Result<TResult> value) : this()
        {
            Query = query;
            result = value;
        }

        public QueryResult(TQuery query, string error) : this()
        {
            Query = query;
            result = DataStorage.Result.Failed<TResult>(error);
        }

        public readonly Result<TResult> End()
        {
            Query.ImplicitCallFinalizer();
            return result;
        }
        public readonly Result<TResult> GetCurrentResult() => result;
        readonly Result INonterminalExpression.GetResult() => result;
        void INonterminalExpression.ImplicitCallFinalizer() => Query.ImplicitCallFinalizer();


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
            return suppress ? GetCurrentResult().Value : GetCurrentResult().GetValueOrThrow();
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
            if (GetCurrentResult().Success)
            {
                value = GetCurrentResult().Value;
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
        public static StorageQuery Query(this Storage storage) => new StorageQuery(storage.Root);
        public static StorageQuery Query(this StorageObject root, string path) => new StorageQuery(root, path);
        public static StorageQuery Query(this Storage storage, string path) => new StorageQuery(storage.Root, path);
        public static StorageQuery Location(this StorageObject root, string path) => new StorageQuery(root, path);
        public static StorageQuery Location(this Storage storage, string path) => new StorageQuery(storage.Root, path);
        public static TQuery Location<TQuery>(this QueryResult<TQuery, StorageObject> queryResult, string path) where TQuery : struct, IStorageQuery<TQuery> => queryResult.Query.Location(path);
        public static TQuery And<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery<TQuery> => query.Previous();
        public static TQuery And<TQuery, TValue>(this QueryResult<TQuery, TValue> result) where TQuery : struct, IStorageQuery<TQuery> => result.Query.Previous();
        public static MakeStatement Make(this StorageObject root, string path) => new MakeStatement(root, path);
        public static ExistStatement Exist(this StorageObject root, string path = null) => new ExistStatement(root, path);

        public static EnsureStatement<StorageQuery> Ensure(this StorageObject root, string path) => new StorageQuery(root, path).Ensure();
        public static EnsureStatement<StorageQuery> Ensure(this Storage storage, string path) => new StorageQuery(storage.Root, path).Ensure();
        public static ExpectStatement<StorageQuery> Expect(this StorageObject root, string path) => new StorageQuery(root, path).Expect();
        public static ExpectStatement<StorageQuery> Expect(this Storage storage, string path) => new StorageQuery(storage.Root, path).Expect();


        internal static QueryResult<TQuery, TValue> CreateResult<TQuery, TValue>(this TQuery query, TValue value) where TQuery : struct, IStorageQuery<TQuery>
        {
            return new QueryResult<TQuery, TValue>(query, value);
        }



        /// <summary>Get member view for this path.</summary>
        /// <remarks>A finalizer</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StorageMember GetMember<TQuery>(this TQuery query, bool createIfMissing = true) where TQuery : struct, IStorageQuery<TQuery>
        {
            query.EnsureRootValid();
            var path = query.PathSpan;
            using var context = new QueryImplicitDisposeContext<TQuery>(query);
            var reslut = createIfMissing ? query.Root.GetMember(path) : (query.Root.TryGetMember(path, out var m) ? m : default);
            return new StorageMember(reslut.StorageObject, reslut.Name.ToArray(), reslut.ArrayIndex);
        }

        /// <summary>Try get member view (non-creating).</summary>
        /// <remarks>A finalizer</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryGetMember<TQuery>(this TQuery query, out StorageMember member) where TQuery : struct, IStorageQuery<TQuery>
        {
            member = default;
            query.EnsureRootValid();
            using var context = new QueryImplicitDisposeContext<TQuery>(query);
            if (query.Result && query.Root.TryGetMember(query.PathSpan, out member))
            {
                member = new StorageMember(member.StorageObject, member.Name.ToArray(), member.ArrayIndex);
                return member.Exists;
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
        public static ExistStatement Exist<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery
        {
            if (!query.Result.Success)
            {
                return default;
            }
            using QueryImplicitDisposeContext<TQuery> queryContext = new(query);
            var e = new ExistStatement(query.Root, query.PathSpan.ToString());
            return e;
        }
        public static ExistStatement Exist<TQuery>(this TQuery root, string path) where TQuery : struct, IStorageQuery<TQuery> => root.Location(path).Exist();

        /// <summary>
        /// Enter Make semantics (creation allowed).
        /// </summary>
        /// <remarks>
        /// This operation finalized the query. <br/>
        /// For normal query, Do NOT reuse the StorageQuery after calling Make()/Exist() (they dispose internal state);
        /// but after Expect()/Exist() you may keep chaining. 
        /// </remarks>
        public static MakeStatement Make<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery
        {
            if (!query.Result.Success)
            {
                query.ImplicitCallFinalizer();
                query.Result.ThrowIfFailed();
            }

            using QueryImplicitDisposeContext<TQuery> queryContext = new(query);
            var e = new MakeStatement(query.Root, query.PathSpan.ToString());
            return e;
        }
        public static MakeStatement Make<TQuery>(this TQuery root, string path) where TQuery : struct, IStorageQuery<TQuery> => root.Location(path).Make();

        /// <summary>
        /// Enter ensure semantics (creation allowed).
        /// </summary> 
        public static EnsureStatement<TQuery> Ensure<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery<TQuery>
        {
            if (!query.Result.Success)
            {
                query.ImplicitCallFinalizer();
                query.Result.ThrowIfFailed();
            }

            var e = new EnsureStatement<TQuery>(query);
            return e;
        }
        public static EnsureStatement<TQuery> Ensure<TQuery>(this TQuery root, string path) where TQuery : struct, IStorageQuery<TQuery> => root.Location(path).Ensure();

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
        public static ExpectStatement<TQuery> Expect<TQuery>(this TQuery root, string path) where TQuery : struct, IStorageQuery<TQuery> => root.Location(path).Expect();

        /// <summary>
        /// Begin the Do statement for the current accumulated path.
        /// </summary>
        /// <typeparam name="TQuery"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static DoStatement Do<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery<TQuery>
        {
            if (!query.Result.Success)
            {
                query.ImplicitCallFinalizer();
                query.Result.ThrowIfFailed();
            }
            var d = new DoStatement(query.Root, query.PathSpan.ToString());
            return d;
        }
        public static DoStatement Do<TQuery>(this TQuery root, string path) where TQuery : struct, IStorageQuery<TQuery> => root.Location(path).Do();



        /// <summary>
        /// exist shortcut.
        /// </summary>
        /// <typeparam name="TQuery"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static Result Exists<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery => query.Exist().Has;
        /// <summary>
        /// Determines whether a storage location specified by the given path exists within the query context.
        /// </summary>
        /// <typeparam name="TQuery">The type of the storage query context. Must be a value type implementing <see
        /// cref="IStorageQuery{TQuery}"/>.</typeparam>
        /// <param name="root">The storage query context in which to check for the existence of the location.</param>
        /// <param name="path">The relative or absolute path of the storage location to check. Cannot be null.</param>
        /// <returns>A <see cref="Result"/> indicating whether the specified storage location exists. The <c>Has</c> property
        /// will be <see langword="true"/> if the location exists; otherwise, <see langword="false"/>.</returns>
        public static Result Exists<TQuery>(this TQuery root, string path) where TQuery : struct, IStorageQuery<TQuery> => root.Location(path).Exist().Has;

        /// <summary>
        /// Return an always-succeeded result to the parent query.
        /// </summary>
        /// <typeparam name="TQuery"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static TQuery Regardless<TQuery>(this TQuery query) where TQuery : struct, IStorageQuery<TQuery>
        {
            TQuery mainQuery = query;
            mainQuery.Result = Result.Succeeded;
            return mainQuery;
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


        /// <summary>
        /// Return to the parent query, setting its Result to the current nested query result.
        /// </summary>
        /// <typeparam name="TQuery"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static TQuery Return<TQuery>(this NestedQuery<TQuery> query) where TQuery : struct, IStorageQuery<TQuery>
        {
            TQuery mainQuery = query.MainQuery;
            mainQuery.Result = query.GetResult();
            return mainQuery;
        }

        /// <summary>
        /// End the current expression, returning its result.
        /// </summary>
        /// <typeparam name="TExpression"></typeparam>
        /// <param name="expr"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Result End<TExpression>(this TExpression expr) where TExpression : INonterminalExpression
        {
            expr.ImplicitCallFinalizer();
            return expr.GetResult();
        }

        /// <summary>
        /// Return to the parent query, discarding the current nested query result.
        /// </summary>
        /// <typeparam name="TQuery"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static TQuery End<TQuery>(this NestedQuery<TQuery> query) where TQuery : struct, IStorageQuery<TQuery>
        {
            query.ImplicitCallFinalizer();
            return query.MainQuery;
        }





        #region If Else

        public static TQuery If<TQuery, TResult1, TResult2, TResult3>(this TQuery query, Func<NestedQuery<TQuery>, TResult1> @if, Func<NestedQuery<TQuery>, TResult2> then = null, Func<NestedQuery<TQuery>, TResult3> @else = null)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult1 : struct, INonterminalExpression
            where TResult2 : struct, INonterminalExpression
            where TResult3 : struct, INonterminalExpression
            => query.If(@if).Then(then).Else(@else);

        public static TQuery If<TQuery, TResult1, TResult2, TResult3>(this TQuery query, string path, Func<NestedQuery<TQuery>, TResult1> @if, Func<NestedQuery<TQuery>, TResult2> then = null, Func<NestedQuery<TQuery>, TResult3> @else = null)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult1 : struct, INonterminalExpression
            where TResult2 : struct, INonterminalExpression
            where TResult3 : struct, INonterminalExpression
            => query.If(path, @if).Then(then).Else(@else);

        public static TQuery If<TQuery>(this TQuery query, Func<NestedQuery<TQuery>, Result> @if, Func<NestedQuery<TQuery>, Result> then = null, Func<NestedQuery<TQuery>, Result> @else = null)
            where TQuery : struct, IStorageQuery<TQuery>
            => query.If(@if).Then(then).Else(@else);

        public static TQuery If<TQuery>(this TQuery query, string path, Func<NestedQuery<TQuery>, Result> @if, Func<NestedQuery<TQuery>, Result> then = null, Func<NestedQuery<TQuery>, Result> @else = null)
            where TQuery : struct, IStorageQuery<TQuery>
            => query.If(path, @if).Then(then).Else(@else);




        public static NestedQuery<TQuery> If<TQuery, TResult>(this TQuery query, Func<NestedQuery<TQuery>, TResult> @if)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult : struct, INonterminalExpression
            => query.If(null, @if);

        public static NestedQuery<TQuery> If<TQuery, TResult>(this TQuery query, string path, Func<NestedQuery<TQuery>, TResult> @if)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult : struct, INonterminalExpression
        {
            using NestedQuery<TQuery> arg = new(query);
            return new NestedQuery<TQuery>(query, @if(arg.Location(path)).End());
        }

        public static NestedQuery<TQuery> If<TQuery>(this TQuery query, Func<NestedQuery<TQuery>, Result> @if) where TQuery : struct, IStorageQuery<TQuery>
            => query.If(null, @if);

        public static NestedQuery<TQuery> If<TQuery>(this TQuery query, string path, Func<NestedQuery<TQuery>, Result> @if) where TQuery : struct, IStorageQuery<TQuery>
        {
            using NestedQuery<TQuery> arg = new(query);
            return new NestedQuery<TQuery>(query, @if(arg.Location(path)));
        }




        public static NestedQuery<TQuery> Then<TQuery>(this NestedQuery<TQuery> query, Func<NestedQuery<TQuery>, Result> then)
            where TQuery : struct, IStorageQuery<TQuery>
            => query.Then(null, then);

        public static NestedQuery<TQuery> Then<TQuery>(this NestedQuery<TQuery> query, string path, Func<NestedQuery<TQuery>, Result> then)
            where TQuery : struct, IStorageQuery<TQuery>
        {
            if (!query.GetResult())
                return query;

            var result = then.Invoke(new NestedQuery<TQuery>(query.MainQuery).Location(path));
            return new NestedQuery<TQuery>(query.MainQuery, result);
        }

        public static NestedQuery<TQuery> Then<TQuery, TResult>(this NestedQuery<TQuery> query, Func<NestedQuery<TQuery>, TResult> then)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult : INonterminalExpression
            => query.Then(null, then);

        public static NestedQuery<TQuery> Then<TQuery, TResult>(this NestedQuery<TQuery> query, string path, Func<NestedQuery<TQuery>, TResult> then)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult : INonterminalExpression
        {
            if (!query.GetResult())
                return query;

            var result = then.Invoke(new NestedQuery<TQuery>(query.MainQuery).Location(path));
            return new NestedQuery<TQuery>(query.MainQuery, result.End());
        }



        /// <summary>
        /// Construct a reversed result of the current nested query.
        /// </summary>
        /// <typeparam name="TQuery"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public static NestedQuery<TQuery> Else<TQuery>(this NestedQuery<TQuery> query) where TQuery : struct, IStorageQuery<TQuery>
            => new NestedQuery<TQuery>(query.MainQuery, !query.GetResult());
        /// <summary>
        /// Construct a reversed result of the current nested query.
        /// </summary>
        /// <typeparam name="TQuery"></typeparam>
        /// <param name="query"></param>
        /// <param name="path"></param>
        /// <returns></returns>
        public static NestedQuery<TQuery> Else<TQuery>(this NestedQuery<TQuery> query, string path) where TQuery : struct, IStorageQuery<TQuery>
            => new NestedQuery<TQuery>(query.MainQuery, !query.GetResult()).Location(path);


        public static TQuery Else<TQuery>(this NestedQuery<TQuery> query, Func<NestedQuery<TQuery>, Result> @else)
            where TQuery : struct, IStorageQuery<TQuery>
            => query.Else().Then(@else).MainQuery;
        public static TQuery Else<TQuery>(this NestedQuery<TQuery> query, string path, Func<NestedQuery<TQuery>, Result> @else)
            where TQuery : struct, IStorageQuery<TQuery>
            => query.Else(path).Then(@else).MainQuery;
        public static TQuery Else<TQuery, TResult>(this NestedQuery<TQuery> query, Func<NestedQuery<TQuery>, TResult> @else)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult : INonterminalExpression
            => query.Else().Then(@else).MainQuery;
        public static TQuery Else<TQuery, TResult>(this NestedQuery<TQuery> query, string path, Func<NestedQuery<TQuery>, TResult> @else)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult : INonterminalExpression
        {
            if (query.GetResult())
                return query.MainQuery;

            var result = @else(query.Else(path)).End();
            TQuery mainQuery = query.MainQuery;
            mainQuery.Result = result;
            return mainQuery;
        }







        /// <summary>
        /// Else if branch, combined with If()/Then()/Else() DSL.
        /// </summary>
        /// <remarks>
        /// If(a).ElseIf(b) can be used as If(a or b)
        /// </remarks>
        /// <typeparam name="TQuery"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="query"></param>
        /// <param name="elseif"></param>
        /// <returns></returns>
        public static NestedQuery<TQuery> ElseIf<TQuery, TResult>(this NestedQuery<TQuery> query, Func<NestedQuery<TQuery>, TResult> elseif)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult : struct, INonterminalExpression
            => query.ElseIf(null, elseif);

        /// <summary>
        /// Else if branch, combined with If()/Then()/Else() DSL.
        /// </summary>
        /// <remarks>
        /// If(a).ElseIf(b) can be used as If(a or b)
        /// </remarks>
        /// <typeparam name="TQuery"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="query"></param>
        /// <param name="elseif"></param>
        /// <returns></returns>
        public static NestedQuery<TQuery> ElseIf<TQuery, TResult>(this NestedQuery<TQuery> query, string path, Func<NestedQuery<TQuery>, TResult> elseif)
            where TQuery : struct, IStorageQuery<TQuery>
            where TResult : struct, INonterminalExpression
        {
            if (query.GetResult())
                return query;

            using NestedQuery<TQuery> arg = new(query.MainQuery);
            return new NestedQuery<TQuery>(query.MainQuery, elseif(arg.Location(path)).End());
        }

        /// <summary>
        /// Else if branch, combined with If()/Then()/Else() DSL.
        /// </summary>
        /// <remarks>
        /// If(a).ElseIf(b) can be used as If(a or b)
        /// </remarks>
        /// <typeparam name="TQuery"></typeparam> 
        /// <param name="query"></param>
        /// <param name="elseif"></param>
        /// <returns></returns>
        public static NestedQuery<TQuery> ElseIf<TQuery>(this NestedQuery<TQuery> query, Func<NestedQuery<TQuery>, bool> elseif) where TQuery : struct, IStorageQuery<TQuery>
            => query.ElseIf(null, elseif);

        /// <summary>
        /// Else if branch, combined with If()/Then()/Else() DSL.
        /// </summary>
        /// <remarks>
        /// If(a).ElseIf(b) can be used as If(a or b)
        /// </remarks>
        /// <typeparam name="TQuery"></typeparam> 
        /// <param name="query"></param>
        /// <param name="elseif"></param>
        /// <returns></returns>
        public static NestedQuery<TQuery> ElseIf<TQuery>(this NestedQuery<TQuery> query, string path, Func<NestedQuery<TQuery>, bool> elseif) where TQuery : struct, IStorageQuery<TQuery>
        {
            if (query.GetResult())
                return query;

            var result = elseif(new NestedQuery<TQuery>(query.MainQuery).Location(path));
            return new NestedQuery<TQuery>(query.MainQuery, result);
        }

        /// <summary>
        /// Else if branch, combined with If()/Then()/Else() DSL.
        /// </summary>
        /// <remarks>
        /// If(a).ElseIf(b) can be used as If(a or b)
        /// </remarks>
        /// <typeparam name="TQuery"></typeparam>
        /// <param name="query"></param>
        /// <param name="elseif"></param>
        /// <returns></returns>
        public static NestedQuery<TQuery> ElseIf<TQuery>(this NestedQuery<TQuery> query, Func<NestedQuery<TQuery>, Result> elseif) where TQuery : struct, IStorageQuery<TQuery>
            => query.ElseIf(null, elseif);

        /// <summary>
        /// Else if branch, combined with If()/Then()/Else() DSL.
        /// </summary>
        /// <remarks>
        /// If(a).ElseIf(b) can be used as If(a or b)
        /// </remarks>
        /// <typeparam name="TQuery"></typeparam>
        /// <param name="query"></param>
        /// <param name="elseif"></param>
        /// <returns></returns>
        public static NestedQuery<TQuery> ElseIf<TQuery>(this NestedQuery<TQuery> query, string path, Func<NestedQuery<TQuery>, Result> elseif) where TQuery : struct, IStorageQuery<TQuery>
        {
            if (query.GetResult())
                return query;

            var result = elseif(new NestedQuery<TQuery>(query.MainQuery).Location(path));
            return new NestedQuery<TQuery>(query.MainQuery, result);
        }


        #endregion







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

        public static StorageObject GetObjectByPath(this Storage storage, string path, bool createIfMissing = true) => storage.Root.GetObjectByPath(path, createIfMissing);
        public static T ReadPath<T>(this Storage storage, string path) where T : unmanaged => storage.Root.ReadPath<T>(path);
        public static void WritePath<T>(this Storage storage, string path, T value) where T : unmanaged => storage.Root.WritePath<T>(path, value: value);
    }
}