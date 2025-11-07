using NUnit.Framework;
using System;

namespace Amlos.Container.Tests
{
    [TestFixture]
    public class ContainerTreeTests
    {
        private ContainerLayout _nodeLayout_1Child;
        private ContainerLayout _nodeLayout_3Children;

        [SetUp]
        public void Setup()
        {
            // Layout A: one int value + one child ref slot ("child")
            {
                var ob = new ObjectBuilder();
                ob.SetScalar<int>("hp");
                // ref slot with default id=0 (8 bytes)
                ob.SetRef("child", 0UL);
                _nodeLayout_1Child = ob.BuildLayout();
            }

            // Layout B: one int + three child ref slots ("c0","c1","c2")
            {
                var ob = new ObjectBuilder();
                ob.SetScalar<int>("hp");
                ob.SetRef("c0", 0UL);
                ob.SetRef("c1", 0UL);
                ob.SetRef("c2", 0UL);
                _nodeLayout_3Children = ob.BuildLayout();
            }
        }

        [TearDown]
        public void Teardown()
        {
            // Nothing to do: tests individually unregister roots.
        }

        [Test]
        public void Build_SingleChild_Tree_And_Unregister_Recursively()
        {
            // root -> child -> grandchild (linked via "child" ref slot)
            var reg = Container.Registry.Shared;
            Span<ContainerReference> ids = stackalloc ContainerReference[3];

            var root = reg.CreateAt(ref ids[0], _nodeLayout_1Child);
            var child = reg.CreateAt(ref ids[1], _nodeLayout_1Child);
            var grand = reg.CreateAt(ref ids[2], _nodeLayout_1Child);

            // write root.hp, child.hp, grand.hp
            root.Write("hp", 10, allowRescheme: false);
            child.Write("hp", 20, allowRescheme: false);
            grand.Write("hp", 30, allowRescheme: false);

            // chain child links
            root.WriteObject("child", child);
            child.WriteObject("child", grand);

            // sanity: ids exist and can be resolved
            Assert.That(root.ID, Is.Not.EqualTo(0UL));
            Assert.That(child.ID, Is.Not.EqualTo(0UL));
            Assert.That(grand.ID, Is.Not.EqualTo(0UL));

            Assert.That(reg.GetContainer(root.ID), Is.SameAs(root));
            Assert.That(reg.GetContainer(child.ID), Is.SameAs(child));
            Assert.That(reg.GetContainer(grand.ID), Is.SameAs(grand));

            // read back
            Assert.That(root.Read<int>("hp"), Is.EqualTo(10));
            Assert.That(child.Read<int>("hp"), Is.EqualTo(20));
            Assert.That(grand.Read<int>("hp"), Is.EqualTo(30));

            // resolve via StorageObject navigation (optional smoke test)
            var soRoot = new StorageObject(root);
            var soChild = soRoot.GetObject("child");
            var soGrand = soChild.GetObject("child");
            Assert.That(soChild.Read<int>("hp"), Is.EqualTo(20));
            Assert.That(soGrand.Read<int>("hp"), Is.EqualTo(30));

            // Act: unregister the root -> should recursively free child & grand
            reg.Unregister(root);

            // Assert: registry no longer has any of them
            Assert.That(reg.GetContainer(root.ID), Is.Null);
            Assert.That(reg.GetContainer(child.ID), Is.Null);
            Assert.That(reg.GetContainer(grand.ID), Is.Null);

            // idempotency: calling again should be safe no-op
            reg.Unregister(root);
            reg.Unregister(child);
            reg.Unregister(grand);
        }

        [Test]
        public void Build_MultiChildren_Tree_And_Unregister_Recursively()
        {
            // root has three children via ("c0","c1","c2")
            var reg = Container.Registry.Shared;
            Span<ContainerReference> ids = stackalloc ContainerReference[4];

            var root = reg.CreateAt(ref ids[0], _nodeLayout_3Children);
            var c0 = reg.CreateAt(ref ids[1], _nodeLayout_1Child);
            var c1 = reg.CreateAt(ref ids[2], _nodeLayout_1Child);
            var c2 = reg.CreateAt(ref ids[3], _nodeLayout_1Child);

            // set hp
            root.Write("hp", 1, allowRescheme: false);
            c0.Write("hp", 10, allowRescheme: false);
            c1.Write("hp", 20, allowRescheme: false);
            c2.Write("hp", 30, allowRescheme: false);

            // link children
            root.WriteObject("c0", c0);
            root.WriteObject("c1", c1);
            root.WriteObject("c2", c2);

            // verify reachable
            Assert.That(reg.GetContainer(root.ID), Is.SameAs(root));
            Assert.That(reg.GetContainer(c0.ID), Is.SameAs(c0));
            Assert.That(reg.GetContainer(c1.ID), Is.SameAs(c1));
            Assert.That(reg.GetContainer(c2.ID), Is.SameAs(c2));

            // quick readback via StorageObject
            var soRoot = new StorageObject(root);
            Assert.That(soRoot.Read<int>("hp"), Is.EqualTo(1));
            Assert.That(new StorageObject(reg.GetContainer(root.GetRefNoRescheme("c0"))).Read<int>("hp"), Is.EqualTo(10));
            Assert.That(new StorageObject(reg.GetContainer(root.GetRefNoRescheme("c1"))).Read<int>("hp"), Is.EqualTo(20));
            Assert.That(new StorageObject(reg.GetContainer(root.GetRefNoRescheme("c2"))).Read<int>("hp"), Is.EqualTo(30));

            // Act
            reg.Unregister(root);

            // Assert: all gone
            Assert.That(reg.GetContainer(root.ID), Is.Null);
            Assert.That(reg.GetContainer(c0.ID), Is.Null);
            Assert.That(reg.GetContainer(c1.ID), Is.Null);
            Assert.That(reg.GetContainer(c2.ID), Is.Null);
        }

        [Test]
        public void Unregister_Ignores_Zero_Slots_And_Partial_Missing()
        {
            var reg = Container.Registry.Shared;
            Span<ContainerReference> ids = stackalloc ContainerReference[2];

            var root = reg.CreateAt(ref ids[0], _nodeLayout_3Children);
            var c0 = reg.CreateAt(ref ids[1], _nodeLayout_1Child);

            // only set c0; leave c1/c2 as 0
            root.WriteObject("c0", c0);

            // sanity
            Assert.That(reg.GetContainer(root.ID), Is.SameAs(root));
            Assert.That(reg.GetContainer(c0.ID), Is.SameAs(c0));

            // Act
            reg.Unregister(root);

            // Assert
            Assert.That(reg.GetContainer(root.ID), Is.Null);
            Assert.That(reg.GetContainer(c0.ID), Is.Null);

            // double-unregister no throw
            reg.Unregister(root);
        }

        [Test]
        public void Deep_Chain_And_Unregister_Does_Not_Loop_Or_Leak()
        {
            var reg = Container.Registry.Shared;
            const int depth = 64; // reasonable depth to avoid stack concerns in test runner

            Span<ContainerReference> ids = stackalloc ContainerReference[depth];

            var nodes = new Container[depth];
            for (int i = 0; i < depth; i++)
                nodes[i] = reg.CreateAt(ref ids[i], _nodeLayout_1Child);

            // link chain: nodes[i] -> nodes[i+1]
            for (int i = 0; i < depth - 1; i++)
                nodes[i].WriteObject("child", nodes[i + 1]);

            // sanity
            Assert.That(reg.GetContainer(nodes[0].ID), Is.SameAs(nodes[0]));
            Assert.That(reg.GetContainer(nodes[depth - 1].ID), Is.SameAs(nodes[depth - 1]));

            // Act
            reg.Unregister(nodes[0]);

            // Assert: all ids invalid now
            for (int i = 0; i < depth; i++)
                Assert.That(reg.GetContainer(nodes[i].ID), Is.Null);
        }
    }

    public static class ContainerExtension
    {
        // Convenience wrapper to match the old test semantics
        internal static void WriteNoRescheme<T>(this Container container, string fieldName, in T value) where T : unmanaged
        {
            container.Write(fieldName, value, allowRescheme: false);
        }
    }
}
