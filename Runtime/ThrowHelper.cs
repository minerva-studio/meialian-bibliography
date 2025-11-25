using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    static class ThrowHelper
    {
        private const string ParamName = "value";

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowWriteError(int err, Type t, Container c, FieldType fieldType, bool allowRescheme)
        {
            throw err switch
            {
                1 => new ArgumentException($"Type {t.Name} cannot cast to {fieldType}.", ParamName),
                2 => new ArgumentException($"Type {t.Name} cannot cast to {fieldType} without rescheme.", ParamName),
                _ => new ArgumentException($"Type {t.Name} cannot cast to {fieldType}.", ParamName),
            };
        }

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static T ThrowDisposed<T>() => throw new ObjectDisposedException(nameof(Container));

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowDisposed(string message) => throw new ObjectDisposedException(message);


        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowIndexOutOfRange() => throw new ArgumentOutOfRangeException("index");

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowInvalidOperation() => throw new InvalidOperationException();

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ArgumentException(string v) => throw new ArgumentException(v);

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ArgumentException(string message, string paramName) => throw new ArgumentException(message, paramName);

        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ArgumentNull(string v) => throw new NotImplementedException(v);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNull(object obj, string name)
        {
            if (obj == null) throw new ArgumentNullException(name);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ThrowIfOverlap(Span<byte> span, ReadOnlySpan<byte> readOnlySpan)
        {
            if (span.Overlaps(readOnlySpan))
            {
                throw new ArgumentException("The provided span overlaps with the container's internal memory.");
            }
        }
    }
}
