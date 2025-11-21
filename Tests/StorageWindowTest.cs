#if UNITY_2021_3_OR_NEWER
using Minerva.DataStorage.Serialization;
using UnityEngine;

namespace Minerva.DataStorage.Tests
{
    public class StorageWindowTest : MonoBehaviour
    {
        public struct Blob
        {
            public int v;
            public short s;
        }

        public TextAsset jsonText;

        private Storage storage;
        private Storage stat;

        // Start is called once before the first execution of Update after the MonoBehaviour is created
        void Start()
        {
            storage = new Storage();
            var child = storage.Root.GetObject("game");
            child.Write("blob", new Blob() { v = 1, s = 2 });
            child.Write("start", 0);
            child.Write("dynamic", "A variable string");
            storage.Root.Write("time", Time.time);
            storage.Root.WriteArray<float>("arr", new float[] { 1.0f, 233f, 6f });
            storage.Root.Write("str", "A messsage");

            stat = JsonSerialization.Parse(jsonText.text);
        }

        // Update is called once per frame
        void Update()
        {
            storage.Root.Write("time", storage.Root.Read<float>("time") + Time.deltaTime);
            var child = storage.Root.GetObject("game");
            child.Write("dynamic", $"A variable string at {Time.deltaTime}");
        }
    }
}

#endif