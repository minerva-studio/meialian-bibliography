using NUnit.Framework;
using System;

namespace Minerva.DataStorage.Tests
{
    [TestFixture]
    public class StorageObjectDisposedApiTests
    {
        private static StorageObject CreateDisposedRoot()
        {
            var storage = new Storage(ContainerLayout.Empty);
            var root = storage.Root;
            storage.Dispose(); // dispose underlying containers
            return root; // ref struct view referencing disposed container
        }

        private static void ExpectDisposed(Action action, string name)
        {
            try
            {
                action();
                Assert.Fail($"Expected ObjectDisposedException: {name}");
            }
            catch (ObjectDisposedException)
            {
                Assert.Pass();
            }
        }

        [Test]
        public void Write_Apis_Throw_When_Disposed()
        {
            var root = CreateDisposedRoot();
            ExpectDisposed(() => root.Write<int>("x", 1), "Write<T>(name)");
            ExpectDisposed(() => root.Write<int>(0, 1), "Write<T>(index)");
            ExpectDisposed(() => root.TryWrite<int>("x", 2), "TryWrite<T>(name)");
            ExpectDisposed(() => root.Override<int>("x", 3), "Override<T>(name,value)");
        }

        [Test]
        public void Read_Apis_Throw_When_Disposed()
        {
            var root = CreateDisposedRoot();
            ExpectDisposed(() => root.Read<int>("x"), "Read<T>(name)");
            ExpectDisposed(() => root.Read<int>(0), "Read<T>(index)");
            ExpectDisposed(() => root.TryRead<int>("x".AsSpan(), out _), "TryRead<T>(name)");
            ExpectDisposed(() => root.ReadOrDefault<int>("x"), "ReadOrDefault<T>(name)");
            ExpectDisposed(() => root.Read_Unsafe<int>("x"), "Read_Unsafe<T>(name)");
        }

        [Test]
        public void Path_Write_Apis_Throw_When_Disposed()
        {
            var root = CreateDisposedRoot();
            ExpectDisposed(() => root.WritePath<int>("a.b.c", 5), "WritePath<T>(path,value)");
            ExpectDisposed(() => root.WritePath("a.b.name", "abc"), "WritePath(string,string)");
            ExpectDisposed(() => root.WriteArrayPath("stats.speeds", new[] { 1f, 2f }), "WriteArrayPath<T>(path,array)");
        }

        [Test]
        public void Path_Read_Apis_Throw_When_Disposed()
        {
            var root = CreateDisposedRoot();
            ExpectDisposed(() => root.ReadPath<int>("a.b"), "ReadPath<T>(path)");
            ExpectDisposed(() => root.TryReadPath<int>("a.b", out _), "TryReadPath<T>(path)");
            ExpectDisposed(() => root.ReadStringPath("s.msg"), "ReadStringPath(path)");
            ExpectDisposed(() => root.ReadArrayPath<int>("arr.values"), "ReadArrayPath<T>(path)");
        }

        [Test]
        public void Object_And_Array_Apis_Throw_When_Disposed()
        {
            var root = CreateDisposedRoot();
            ExpectDisposed(() => root.GetObject("child"), "GetObject(name)");
            ExpectDisposed(() => root.GetObjectByPath("a.b"), "GetObjectByPath(path)");
            ExpectDisposed(() => root.GetArray("numbers"), "GetArray(name)");
            ExpectDisposed(() => root.AsArray(), "AsArray()");
            ExpectDisposed(() => root.MakeArray<int>(4), "MakeArray<T>(len)");
            ExpectDisposed(() => root.WriteArray<int>(new[] { 1, 2 }), "WriteArray<T>(span)");
            ExpectDisposed(() => root.ReadArray<int>(), "ReadArray<T>()");
        }

        [Test]
        public void Misc_Apis_Throw_When_Disposed()
        {
            var root = CreateDisposedRoot();
            ExpectDisposed(() => root.GetValueView("x"), "GetValueView(name)");
            ExpectDisposed(() => root.GetField("x"), "GetField(name)");
            ExpectDisposed(() => root.GetMember("x"), "GetMember(path)");
            ExpectDisposed(() => root.HasField("x"), "HasField(name)");
            ExpectDisposed(() => root.Rescheme(ContainerLayout.Empty), "Rescheme(layout)");
            ExpectDisposed(() => root.Delete("x"), "Delete(name)");
            ExpectDisposed(() => root.Subscribe("x", (in StorageFieldWriteEventArgs _) => { }), "Subscribe(path,handler)");
            ExpectDisposed(() => root.Subscribe((in StorageFieldWriteEventArgs _) => { }), "Subscribe(container)");
        }
    }
}
