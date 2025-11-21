using System.Runtime.InteropServices;

namespace Minerva.DataStorage.Serialization
{
    [StructLayout(LayoutKind.Explicit)]
    public struct ElementValue
    {
        [FieldOffset(0)]
        ValueType type;
        [FieldOffset(4)]
        bool b;
        [FieldOffset(4)]
        long l;
        [FieldOffset(4)]
        double d;

        public bool BoolValue => type == ValueType.Bool ? b : (l != 0 || d != 0);
        public long IntValue => type == ValueType.Int64 ? l : (type == ValueType.Bool ? (b ? 1 : 0) : (long)d);
        public double FloatValue => type == ValueType.Float64 ? d : (type == ValueType.Bool ? (b ? 1 : 0) : l);


        public static implicit operator ElementValue(long value) => new ElementValue() { type = ValueType.Int64, l = value };
        public static implicit operator ElementValue(double value) => new ElementValue() { type = ValueType.Float64, d = value };
        public static implicit operator ElementValue(bool value) => new ElementValue() { type = ValueType.Bool, b = value };
    }
}
