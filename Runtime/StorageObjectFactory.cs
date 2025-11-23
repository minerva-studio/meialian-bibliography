using System;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Wrapping <see cref="Container"/> to <see cref="StorageObject"/>
    /// </summary>
    internal static class StorageObjectFactory
    {
        internal static StorageObject GetOrCreate(this ref ContainerReference position, Container parent, ContainerLayout layout, ReadOnlySpan<char> name)
        {
            ref var id = ref position;
            var child = Container.Registry.Shared.GetContainer(id);
            if (child is null)
            {
                Container.Registry.Shared.CreateAt(ref position, parent, layout ?? ContainerLayout.Empty, name);
                child = Container.Registry.Shared.GetContainer(id);
            }
            return new StorageObject(child);
        }

        internal static StorageObject GetNoAllocate(this ContainerReference position)
        {
            var id = position;
            if (id == 0UL) return default;      // null-like

            var child = Container.Registry.Shared.GetContainer(id);
            if (child is null) return default;  // dangling -> treat as null

            return new StorageObject(child);
        }

        internal static bool TryGet(this ContainerReference position, out StorageObject obj)
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

