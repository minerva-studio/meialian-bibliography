using System;
using System.Runtime.CompilerServices;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Forward-only path walker that parses "Segment" or "Segment[3]" pieces
    /// from a path like "Player.Inventory[2].Stats".
    /// Each call returns:
    ///   - name: segment name without index
    ///   - index: array index if "[index]" is present, otherwise -1
    ///   - return bool: whether this is the last segment in the path
    /// </summary>
    internal ref struct PathReader
    {
        // Path parsing syntax errors (used only internally for diagnostics / flow control)
        internal enum PathSyntaxError
        {
            None,
            EmptySegment,
            InvalidBracket,
            InvalidIndex,
        }

        private readonly ReadOnlySpan<char> _path;
        private readonly char _separator;
        private readonly bool _throw;
        private int _pos;

        public bool HasNext => _pos < _path.Length;

        public PathReader(ReadOnlySpan<char> path, char separator, bool throwException = true)
        {
            _path = path;
            _separator = separator;
            _pos = 0;
            _throw = throwException;
        }

        public bool MoveNext(out ReadOnlySpan<char> name, out int index)
        {
            var result = MoveNext(out name, out index, out var error);
            if (_throw && error != PathSyntaxError.None)
            {
                throw new ArgumentException($"Invalid path syntax: '{_path.ToString()}' ({error}).");
            }
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext(out ReadOnlySpan<char> name, out int index, out PathSyntaxError error)
        {
            name = default;
            index = -1;
            error = PathSyntaxError.None;

            if (!HasNext)
                return false; // no more segments

            int nameStart = _pos;
            int nameEnd = -1;
            int indexStart = -1;
            int indexEnd = -1;
            bool inIndex = false;

            while (_pos < _path.Length)
            {
                char c = _path[_pos];

                // Path separator ends the current segment (unless we are inside [index])
                if (c == _separator)
                {
                    if (inIndex)
                    {
                        error = PathSyntaxError.InvalidBracket;
                        return false;
                    }

                    // Trailing separator -> empty segment ("Foo.")
                    if (_pos == _path.Length - 1)
                    {
                        error = PathSyntaxError.EmptySegment;
                        return false;
                    }

                    break;
                }

                if (c == '[')
                {
                    // Do not allow nested "[" or multiple "[" per segment
                    if (inIndex || indexStart >= 0)
                    {
                        error = PathSyntaxError.InvalidBracket;
                        return false;
                    }

                    inIndex = true;
                    nameEnd = _pos;          // name ends right before '['
                    indexStart = _pos + 1;   // index starts after '['
                    _pos++;
                    continue;
                }

                if (c == ']')
                {
                    // ']' without matching '['
                    if (!inIndex || indexStart < 0)
                    {
                        error = PathSyntaxError.InvalidBracket;
                        return false;
                    }

                    inIndex = false;
                    indexEnd = _pos;         // index ends before ']'
                    _pos++;
                    continue;
                }

                _pos++;
            }

            // Reached end of path while inside "[index"
            if (inIndex)
            {
                error = PathSyntaxError.InvalidBracket;
                return false;
            }

            int segmentEnd = _pos; // exclusive end of this segment

            if (segmentEnd == nameStart)
            {
                // Empty segment (e.g., ".Foo" or "A..B")
                error = PathSyntaxError.EmptySegment;
                return false;
            }

            if (nameEnd < 0)
            {
                // If we saw an index, name ends right before the index;
                // otherwise name covers the whole segment.
                nameEnd = (indexStart >= 0 ? indexStart - 1 : segmentEnd);
            }

            if (nameEnd <= nameStart)
            {
                // Cases like "[0]" (no name) or "Foo[]" (empty index span)
                error = PathSyntaxError.EmptySegment;
                return false;
            }

            name = _path.Slice(nameStart, nameEnd - nameStart);

            if (indexStart >= 0)
            {
                if (indexEnd < 0 || indexEnd <= indexStart)
                {
                    error = PathSyntaxError.InvalidIndex;
                    return false;
                }

                var span = _path[indexStart..indexEnd];
                if (!int.TryParse(span, out index))
                {
                    error = PathSyntaxError.InvalidIndex;
                    return false;
                }
            }
            else
            {
                index = -1;
            }

            // Skip separator, prepare for next segment
            _pos++;
            return true;
        }
    }
}
