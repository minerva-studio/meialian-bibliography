using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Minerva.DataStorage
{
    // =========================
    // Low-level type metadata
    // =========================
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct ContainerReference
    {
        internal static short Size = (short)Unsafe.SizeOf<ContainerReference>();

        public ulong id;

        public Container Container
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => Container.Registry.Shared.GetContainer(id);
            set => id = value?.ID ?? Container.Registry.ID.Empty;
        }

        public void Unregister()
        {
            // already unregistered or is null
            if (id == Container.Registry.ID.Empty) return;

            var container = Container.Registry.Shared.GetContainer(id);
            if (container != null)
                Container.Registry.Shared.Unregister(container);

            id = Container.Registry.ID.Empty;  // mark as unregistered
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref ContainerReference FromSpan(Span<byte> span) => ref MemoryMarshal.Cast<byte, ContainerReference>(span)[0];

        public static implicit operator ContainerReference(ulong id) => new() { id = id };
        public static implicit operator ulong(ContainerReference cr) => cr.id;
        public static implicit operator ContainerReference(Container container) => new() { id = container?.ID ?? 0UL };
    }
}
