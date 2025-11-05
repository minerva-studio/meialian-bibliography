namespace Amlos.Container
{
    public readonly struct FieldDescriptorInstance
    {
        public FieldDescriptor FieldDescriptor { get; }
        public int Index { get; }
        public string Name => FieldDescriptor.Name;
        public int Length => FieldDescriptor.Length;
        public int Offset => FieldDescriptor.Offset;


        public FieldDescriptorInstance(FieldDescriptor f, int index)
        {
            FieldDescriptor = f;
            Index = index;
        }

        public static implicit operator FieldDescriptor(FieldDescriptorInstance fieldDescriptorInstance)
        {
            return fieldDescriptorInstance.FieldDescriptor;
        }
    }
}
