namespace Amlos.Container
{
    internal static class StorageFactory
    {
        internal static StorageObject Get(ref ulong position, Schema defaultSchema)
        {
            ref var id = ref position;
            var child = Container.Registry.Shared.GetContainer(id);
            if (child is null)
            {
                Container.CreateAt(ref position, defaultSchema ?? Schema.Empty);
                child = Container.Registry.Shared.GetContainer(id);
            }
            return new StorageObject(child);
        }

        internal static StorageObject GetNoAllocate(ulong position)
        {
            var id = position;
            if (id == 0UL) return default; // null-like

            var child = Container.Registry.Shared.GetContainer(id);
            if (child is null) return default; // dangling -> treat as null

            return new StorageObject(child);
        }

        internal static bool TryGet(ulong position, out StorageObject obj)
        {
            obj = default;

            if (position == 0UL) return false;
            var c = Container.Registry.Shared.GetContainer(position);
            if (c == null) return false;
            obj = new StorageObject(c);
            return true;
        }

    }
}

