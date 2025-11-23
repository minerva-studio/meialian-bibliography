using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    public static class StorageObjectExtesnion
    {
        /// <summary>
        /// Convenience overload for writing an inline array from any IEnumerable{T}.
        /// This is a wrapper around the span-based WriteArray{T} core API and should
        /// not be used in tight inner loops when the caller can provide a span/array directly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteArray<T>(this StorageObject storageObject, string fieldName, IEnumerable<T> values)
            where T : unmanaged => storageObject.GetObject(fieldName).WriteArray(values);

        /// <summary>
        /// Convenience overload for writing an inline array from any IEnumerable{T}.
        /// This is a wrapper around the span-based WriteArray{T} core API and should
        /// not be used in tight inner loops when the caller can provide a span/array directly.
        /// </summary>
        public static void WriteArray<T>(this StorageObject storageObject, IEnumerable<T> values)
            where T : unmanaged
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));

            // Fast path: already a T[] -> zero-copy to span.
            if (values is T[] array)
            {
                storageObject.WriteArray<T>(array.AsSpan());
                return;
            }

            // Fast path: ICollection<T> -> we know the Count and can copy once.
            if (values is ICollection<T> coll)
            {
                int count = coll.Count;
                if (count == 0)
                {
                    storageObject.WriteArray<T>(ReadOnlySpan<T>.Empty);
                    return;
                }

                var tmp = new T[count];
                coll.CopyTo(tmp, 0);
                storageObject.WriteArray<T>(tmp.AsSpan());
                return;
            }

            // Fallback: unknown size IEnumerable<T> -> buffer into a List<T>.
            // This is the most allocation-heavy path and should be avoided
            // in performance-critical code by passing a span or array instead.
            var list = new List<T>();
            foreach (var item in values)
                list.Add(item);

            if (list.Count == 0)
            {
                storageObject.WriteArray<T>(ReadOnlySpan<T>.Empty);
                return;
            }

            var data = list.ToArray();
            storageObject.WriteArray<T>(data.AsSpan());
        }


        public static StorageArray MakeObjectArray(this StorageObject storageObject, int length)
        {
            storageObject.MakeArray(TypeData.Ref, length);
            return storageObject.AsArray();
        }
    }
}

