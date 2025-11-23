# Meialian Bibliography

Dynamic serializable/deserializable byte container — used as the backing store for scoreboards, stat systems, and other structured game data.

`Minerva.DataStorage` gives you:

- A **schema-driven, fixed-layout byte container**, backed by pooled buffers.
- A **tree-shaped storage** API (`Storage` / `StorageObject`) for nested objects.
- A **JSON adapter** based on `Unity.Serialization.Json` for save/load and debugging.
- **Schema migration / rescheming** support so layouts can evolve without losing data.

Package name: `com.minervastudio.meialianbibliography`  
Unity: `2021.3`+

---

## Table of Contents

- [Meialian Bibliography](#meialian-bibliography)
  - [Table of Contents](#table-of-contents)
  - [Motivation](#motivation)
  - [Features](#features)
  - [Installation](#installation)
    - [Via Unity Package Manager (Git URL)](#via-unity-package-manager-git-url)
    - [From local source](#from-local-source)
  - [Quick Start](#quick-start)
    - [1. (Optional) Define a schema](#1-optional-define-a-schema)
    - [2. Create storage \& write data](#2-create-storage--write-data)
    - [3. Serialize to / from JSON](#3-serialize-to--from-json)
    - [4. Subscribe to field or container write events](#4-subscribe-to-field-or-container-write-events)
  - [Core Concepts](#core-concepts)
    - [Storage \& StorageObject](#storage--storageobject)
    - [ContainerLayout \& ObjectBuilder](#containerlayout--objectbuilder)
    - [Field types \& arrays](#field-types--arrays)
    - [Schema migration \& rescheme](#schema-migration--rescheme)
  - [Binary \& standalone JSON serialization](#binary--standalone-json-serialization)
    - [BinarySerialization](#binaryserialization)
    - [JsonSerialization](#jsonserialization)
    - [(Unity) JSON adapter](#unity-json-adapter)
  - [Performance \& Benchmarks](#performance--benchmarks)
  - [Limitations](#limitations)
  - [Roadmap / TODO](#roadmap--todo)
  - [License](#license)
  - [Contact](#contact)

---

## Motivation

> _Why does this package exist?_

Typical Unity data workflows either:

- Use plain C# objects/ScriptableObjects — easy, but **GC-heavy** and not great for high-frequency stats.
- Use ad-hoc JSON / `Dictionary<string, object>` — flexible, but **slow**, hard to reason about layout.

`Meialian Bibliography` sits in between:

- Data is stored in a **compact, fixed-layout byte buffer**, described by a schema.
- You access fields through a small API (`StorageObject`) instead of pushing around raw `byte[]`.
- Layouts can be **migrated** when you add/remove/rename fields.

Use it when you need a **small in-memory “database” for game stats / scoreboards / runtime metadata** and care about:

- allocation behavior,
- deterministic layout,
- and safe schema evolution.

---

## Features

- **Schema-driven containers**
  - `ContainerLayout` describes fields (name, type, length, array vs scalar).
  - `ObjectBuilder` builds layouts and packs them into headers that can be reused to create many storages with the **same shape**.

- **Tree-shaped storage**
  - `Storage` is the root owner of a tree of internal containers.
  - You never manage those containers directly; instead you work with `Storage` and `StorageObject`.
  - Supports nested objects and arrays of child objects.

- **Value & reference fields**
  - Numeric primitives (`int`, `float`, etc.), `bool`, and `char` stored inline.
  - Large or variable-length data goes through ref storage when needed.

- **Inline arrays & object arrays**
  - Fixed-length inline value arrays (`float[4]`, `int[16]`, …) for compact data.
  - Object arrays for arrays of child “rows” / entries.

- **Auto-expanding layouts**
  - For simple cases you can skip schemas entirely:
    - Start with a `Storage`,
    - write fields by name,
    - the layout expands lazily as you go.
  - For repeated structures or hot paths, pre-building a layout with `ObjectBuilder` is much faster than expanding one field at a time.

- **Schema migration**
  - Add new fields → zero-initialized.
  - Remove fields → dropped.
  - Rescheming keeps logical identities stable while cleaning up layout.

- **JSON serialization (Unity)**
  - `StorageAdapter` integrates with `Unity.Serialization.Json`.
  - Supports round-tripping `Storage` to/from JSON for saves, debugging, or tools.
  - Basic “self-heal” behavior when JSON shape drifts from the current schema.
- **Write subscriptions**
  - Register callbacks for specific fields or entire containers.
  - Observe changes without polling; useful for hot stats or debugging tools.
  - Dispose the returned handle to unsubscribe cleanly.

- **Tested & benchmarked**
  - NUnit tests cover memory safety, public API semantics, and migration.
  - Separate performance tests using `Unity.PerformanceTesting` (explicit).

---

## Installation

### Via Unity Package Manager (Git URL)

1. Open **Unity** → **Window** → **Package Manager**.
2. Click **+** → **Add package from git URL…**
3. Paste:

```text
https://github.com/minerva-studio/meialian-bibliography.git
````

4. Import.

> Optionally, add it to your project manifest:

```jsonc
{
  "dependencies": {
    "com.minervastudio.meialianbibliography": "https://github.com/minerva-studio/meialian-bibliography.git"
  }
}
```

### From local source

1. Clone this repository into your project, e.g. `Packages/meialian-bibliography/`.
2. Unity will detect the package via `package.json`.

> JSON serialization examples rely on `Unity.Serialization.Json` (the `com.unity.serialization` package). Install it from UPM if your project doesn’t already use it.

---

## Quick Start

### 1. (Optional) Define a schema

You **do not need** to define a schema to start using `Storage`.
You can create a `Storage`, start writing fields, and let the layout auto-expand.

However, when:

* you know the shape in advance, and/or
* you’ll create many identical storages or objects,

using `ObjectBuilder` to define a `ContainerLayout` once and reuse it is faster and more predictable.

Example of defining a root layout:

```csharp
using Minerva.DataStorage;

ObjectBuilder builder = new ObjectBuilder();

// root fields:
//   - score: Int32
//   - player_name: string (stored as a nested array/object)
builder.SetScalar<int>("score");
builder.SetRef("player_name"); // ref field; actual content will be a string/object container

ContainerLayout rootLayout = builder.BuildLayout();
// rootLayout can be serialized/packed and reused as a header for new Storage instances.
```

You can create separate layouts for children if you want more structure:

```csharp
ObjectBuilder playerBuilder = new ObjectBuilder();
playerBuilder.SetScalar<int>("level");
playerBuilder.SetScalar<int>("hp");
ContainerLayout playerLayout = playerBuilder.BuildLayout();
```

---

### 2. Create storage & write data

`Storage` owns the data tree. `Storage.Root` gives you a `StorageObject` view:

```csharp
// With a predefined layout
using var storage = new Storage(rootLayout);
StorageObject root = storage.Root;

// Or: let Storage auto-expand its layout as you write new fields.
// using var storage = new Storage();
// StorageObject root = storage.Root;

// Or: build the storage with ObjectBuilder
// var builder = new ObjectBuilder();
// builder.SetScalar<int>("score", 2);
// builder.SetScalar<int>("hp", 4);
// using var storage = builder.BuildStorage();

// Write simple scalar data
root.Write<int>("score", 12345);

// Create/get a child object (reusing a layout is fastest)
StorageObject player = root.GetObject("player", layout: playerLayout);
player.Write<int>("level", 10);
player.Write<int>("hp", 42);

// Read it back
int score = root.Read<int>("score");
int level = player.Read<int>("level");

// Path-based navigation from the root (dot-separated)
root.WritePath("persistent.entity.mamaRhombear.killed", 1);
int killed = root.ReadPath<int>("persistent.entity.mamaRhombear.killed");

// You can think of this as roughly equivalent to:
// root.GetObject("persistent").GetObject("entity").GetObject("mamaRhombear").Write("killed", 1);

// Strings and arrays via path
root.WritePath("strings.greeting", "Hello");
string greeting = root.ReadStringPath("strings.greeting");

root.WriteArrayPath("stats.speeds", new[] { 1.0f, 2.5f, 3.75f });
float[] speeds = root.ReadArrayPath<float>("stats.speeds");
```

Path segments are separated by `.` by default.  
Advanced usage can override the separator via the span-based overloads, e.g.:

```csharp
// Use '/' as separator instead of '.'
root.WritePath("persistent/entity/mamaRhombear/killed".AsSpan(), 1, separator: '/');
int killed2 = root.ReadPath<int>("persistent/entity/mamaRhombear/killed".AsSpan(), separator: '/');
```

From user code, you only deal with:

* `Storage` (create/own the data),
* `StorageObject` (read/write fields, descend into children),
* array views for inline arrays / object arrays.

The underlying containers and pools are entirely managed internally.

---

### 3. Serialize to / from JSON

Register `StorageAdapter` with Unity’s JSON serialization so `Storage` can be directly serialized.

```csharp
using System.Collections.Generic;
using Minerva.DataStorage;
using Minerva.DataStorage.Serialization;
using Unity.Serialization.Json;

var parameters = new JsonSerializationParameters
{
    UserDefinedAdapters = new List<IJsonAdapter>
    {
        new StorageAdapter()
    }
};

// Serialize
string json = JsonSerialization.ToJson(storage, parameters);

// Deserialize
Storage loaded = JsonSerialization.FromJson<Storage>(json, parameters);
StorageObject loadedRoot = loaded.Root;
```

---

### 4. Subscribe to field or container write events

Hook into individual fields or an entire container when critical stats change without scanning the storage:

```csharp
using Minerva.DataStorage;

using var storage = new Storage();
var root = storage.Root;

// Subscribe to a specific field
using var fieldSubscription = root.Subscribe("score", (in StorageFieldWriteEventArgs args) =>
{
    int score = args.Target.Read<int>(args.Path);
    UnityEngine.Debug.Log($"Score updated to {score}");
});

// Subscribe to the entire container
using var containerSubscription = root.Subscribe(args =>
{
    UnityEngine.Debug.Log($"{args.Path} changed on root.");
});

root.Write("score", 42);
root.Write("hp", 99);
// both handlers fire; dispose the subscriptions when done
```

Need to watch a nested field? Pass a dotted path or navigate manually:

```csharp
using var sub = root.Subscribe("player.stats.hp", args =>
{
    var hp = args.Target.Read<int>("hp");
    OnHpChanged(hp);
});

// Equivalent manual navigation:
var stats = root.GetObject("player").GetObject("stats");
using var sub2 = stats.Subscribe("hp", args => ...);

> `Subscribe()` or `Subscribe("")` targets the current container. Use dotted paths to reach deeper containers or fields; paths must point at data that already exists (create child containers up front with `GetObject` if needed).
```

---

## Core Concepts

### Storage & StorageObject

* **`Storage`**

  * Root owner for an entire data tree.
  * Manages all internal containers and pooling.
  * Disposing the `Storage`:

    * Unregisters its containers from the internal registry.
    * Returns their underlying buffers to the pool.
  * Typical lifetime: one `Storage` per save slot, per session, or per logical database.

* **`StorageObject`**

  * A lightweight view/handle to a node in a `Storage` tree.
  * Provided by APIs such as `Storage.Root` or `GetObject`.
  * Offers read/write access by field name or index:

    * `Write<T>(string fieldName, T value)`
    * `Read<T>(string fieldName)`
    * `ReadOrDefault<T>(string fieldName)`
    * `WritePath<T>(string path, T value)`, `ReadPath<T>(string path)`
    * `WritePath(string path, string value)`, `ReadStringPath(string path)`
    * `WriteArrayPath<T>(string path, T[] value)`, `ReadArrayPath<T>(string path)`
    * `GetObject`, `GetObjectByPath`, `GetField`, etc.
  * Uses an internal generation number to detect stale handles (e.g., after migration).

From a consumer perspective, you generally never touch the underlying container type directly; you only keep `Storage` instances and the `StorageObject`/array views they hand out.

---

### ContainerLayout & ObjectBuilder

* **`ContainerLayout`**

  * Describes the schema/shape of a node:

    * Field names, value kinds (`ValueType`), sizes, array flags.
  * Typically:

    * built once with `ObjectBuilder`,
    * serialized/embedded as a header,
    * reused to create multiple `Storage` instances with identical layout.

* **`ObjectBuilder`**

  * Builds both layouts and data.
  * Key methods:

    * `SetScalar<T>(string name)` — add a scalar field.
    * `SetRef(string name, ulong defaultId)` — add a reference field to another object/child node.
    * `SetArray<T>(string name, FieldType type, int arraySize)` — define fixed-size inline arrays.
    * `BuildLayout()` — construct a `ContainerLayout` that can be stored and reused.
  * The same builder logic is also used internally during rescheming/migrations.

---

### Field types & arrays

* **ValueType**

  * Enum describing primitive kinds: `Bool`, `Int32`, `Float32`, `Blob`, `Ref`, etc.

* **FieldType**

  * Wraps `ValueType` + “is array” bit.
  * Distinguishes:

    * Scalar vs array,
    * inline vs ref storage.

* **Inline value arrays**

  * Exposed via `StorageInlineArray` (a view over an inline array field).
  * Provides:

    * `Length`
    * Indexer `this[int index]` returning a `ValueView`.
    * Helpers to copy to/from managed arrays.

* **Object arrays**

  * Arrays of child nodes (e.g., scoreboard entries, party members).
  * Exposed via array APIs on `StorageObject`, typically by field name.

Again, you work with these through `StorageObject` and its helpers; the container backing them stays internal.

---

### Schema migration & rescheme

Schemas/layouts can change safely via a **rescheme** API:

* New fields → allocated and zeroed.
* Removed fields → their data is dropped.
* Existing fields → copied & reused when compatible.

Under the hood, rescheming:

1. Uses an `ObjectBuilder` to reconstruct a new layout and mapping.
2. Migrates existing data into the new byte layout.
3. Swaps the underlying storage while keeping logical identities stable.
4. Updates registry/generation so stale handles can be detected.

Typical usages:

* Add a new stat to an existing layout without breaking old saves.
* Shrink/expand an inline array (with careful handling).
* Convert certain scalars to arrays, or vice versa, during a version upgrade.

---

## Binary & standalone JSON serialization

Namespace: `Minerva.DataStorage.Serialization`

In addition to working with `Storage` / `StorageObject` in memory, the package ships with two low-level helpers for snapshotting and restoring an entire storage tree:

- **`BinarySerialization`** — compact, depth-first binary format for fast round-trips within the same build.
- **`JsonSerialization`** — direct JSON text mapping that does not depend on `Unity.Serialization.Json`.

These helpers treat `Storage` as a DOM-like tree and stream data in and out without building intermediate object graphs.

```csharp
using Minerva.DataStorage;
using Minerva.DataStorage.Serialization;
````

---

### BinarySerialization

`BinarySerialization` produces and consumes a compact binary representation of a `Storage` tree.

**Writing:**

```csharp
// Raw binary snapshot
ReadOnlySpan<byte> bytes = storage.ToBinary();

// Base64 for text-based channels
string base64 = storage.ToBase64();
```

* `ToBinary(this Storage storage)`
  Walks from `storage.Root` in depth-first, pre-order and writes each internal container as:

  ```text
  [ id      : sizeof(ContainerReference) bytes (8), little-endian ]
  [ payload : container.Memory.Length bytes ]
  ```

  where `payload` is the container’s raw backing memory (header + all field data).

* `ToBase64(this Storage storage)`
  Calls `ToBinary` and encodes the resulting bytes as Base64.

During serialization, reference fields (`IsRef == true`) are treated as arrays of `ContainerReference` IDs: the code follows those IDs and recursively emits the referenced containers.

> **Assumption:** the storage graph is a **tree**. Each child container is reachable from exactly one parent, and there are no cycles or shared subtrees. If the same container is referenced from multiple places, it will be serialized multiple times and the behavior on parse is undefined.

**Reading:**

```csharp
// Read-only span -> always clones container payloads
Storage s1 = BinarySerialization.Parse(bytes);

// Memory<byte> -> optional aliasing
var buffer = bytes.ToArray().AsMemory();
Storage s2 = BinarySerialization.Parse(buffer, allocate: false);

// Base64 entry point
Storage s3 = BinarySerialization.ParseBase64(base64, allocate: true);
```

* `Parse(ReadOnlySpan<byte> bytes)`
  Always allocates a fresh backing buffer for each container (safe, but copies).

* `Parse(Memory<byte> bytes, bool allocate = true)`

  * `allocate == true`: clone each container’s payload into its own block (independent of the source buffer).
  * `allocate == false`: containers alias slices of the provided buffer; **the caller must ensure the buffer outlives the resulting `Storage`**.

* `ParseBase64(string base64, bool allocate = true)`
  Decodes Base64 to a byte array and then delegates to the `Parse(Memory<byte>, bool)` overload.

All reconstructed containers are re-registered in `Container.Registry` and assigned fresh IDs; reference fields are patched to point at the new IDs.

**Use this when:**

* you need a fast in-process snapshot / restore within the same build;
* you don’t care about long-term, cross-version compatibility of the binary format.

---

### JsonSerialization

`JsonSerialization` is a hand-written JSON mapper that streams text directly into and out of a `Storage` tree, without any dependency on `Unity.Serialization.Json`.

**Writing:**

```csharp
ReadOnlySpan<char> span = storage.ToJson();
string json = span.ToString(); // copy only if you really need a string
```

* `ToJson(this Storage storage)`
  Writes a JSON object rooted at `storage.Root`. Field names become JSON property names. The mapping is:

  * Root JSON value: always an **object**.
  * For each property `"name": value`:

    * `value` is **object** → child object via `GetObject("name")`, then recurse.
    * `value` is **array** → child “array container” with a single array field.
    * `value` is **string** → if length == 1, a scalar `char`; otherwise a UTF-16 string/char array.
    * `value` is **true/false** → scalar `bool`.
    * `value` is **number** → stored as `Int64` (if it fits) or `Double` otherwise.
    * `value` is **null** → ignored (no field written).

Inline arrays and object arrays are emitted as JSON arrays; blobs are encoded as Base64 strings.

**Reading:**

```csharp
Storage storage = JsonSerialization.Parse(jsonText, maxDepth: 1000);
StorageObject root = storage.Root;
```

Overloads:

```csharp
Storage Parse(string json, int maxDepth = 1000);
Storage Parse(ReadOnlySpan<char> text, int maxDepth = 1000);
```

Both use an internal `JsonToStorageReader` that parses and writes directly into `StorageObject` without building an intermediate JSON tree.

**Mapping rules:**

* Root JSON must be an object; everything else is rejected.
* Scalars:

  * `true` / `false` → `bool`.
  * Numbers:

    * All integer literals are normalized to **64-bit integers** (`Int64`).
    * Any non-integer literal is normalized to **64-bit float** (`Double`).
    * Calling `Read<int>` or `Read<float>` on such fields is allowed; the value will be converted at read time, but the stored representation is 64-bit.
* Strings:

  * length == 1 → stored as a single `char`;
  * length > 1 → stored as UTF-16 string/char array.
* Arrays:

  * pure `bool` → `bool[]`;
  * pure integers → `Int64[]`;
  * any float present → entire array as `Double[]`;
  * arrays of objects / strings → arrays of child containers.
  * mixed incompatible types (e.g., numbers + strings) → exception.

**Depth limit:**

* `maxDepth` caps the nesting depth (default 1000) to protect against malicious or malformed JSON causing unbounded recursion.

**When to use which JSON path?**

* Use **`JsonSerialization`** when you want a self-contained JSON representation (for tooling, logs, or non-Unity environments).
* Use the **Unity `StorageAdapter`** (see the JSON adapter section above) when you want to plug `Storage` into `Unity.Serialization.Json` and reuse Unity’s serialization pipeline.

---

### (Unity) JSON adapter

Namespace: `Minerva.DataStorage.Serialization`

The adapter:

* Walks your layout and writes fields to JSON.
* Can infer or update the layout from JSON when deserializing into an empty or older storage.
* Tries to “self-heal” certain mismatches (e.g., compatible type upgrades).

* **`StorageAdapter`**

  * Implements Unity’s JSON adapter interfaces to map between:

    * `Storage` / `StorageObject` trees, and
    * Unity’s `SerializedObjectView` / `SerializedArrayView`.

* Key behaviors:

  * **Null handling**
    JSON `null` can map to empty or missing nodes.

  * **Arrays**

    * Inline numeric arrays become JSON arrays.
    * Arrays of child objects become JSON arrays of objects or `null`s.

  * **Blobs**

    * Unknown bytes (`ValueType.Blob`) are emitted as an object with "$blob" property and a base64 string.

---

## Performance & Benchmarks

The `Tests/Performance` folder contains **explicit** performance tests (using `Unity.PerformanceTesting`). They are not meant to run in every CI pass, but give you a sense of:

* write throughput for scalar fields,
* behavior under mixed workloads (scalars, strings, child objects),
* how registry and pool usage scale with many nodes.

You can run them via Unity Test Runner:

* Filter by Category: `Perf`.
* All tests are marked `[Explicit]` to avoid accidental runs.

---

## Limitations

> Things to be aware of before you adopt this in production.

* **Low-level API**

  The library focuses on a small, low-level core. There is no LINQ-style query API or high-level ORM-like layer on top by default.

* **Schema knowledge lives in your code**

  JSON round-trips preserve all fields by name – deserialization does not silently drop keys. However:

  * There is no built-in, versioned “schema registry”.
  * Numeric types are **normalized** on JSON round-trip:
    * All integer-like JSON values are treated as 64-bit integers (`Int64`).
    * All floating-point JSON values are treated as 64-bit floats (`Double`).
  * The runtime performs automatic numeric conversion when you call `Read<T>` / `Write<T>`:
    * Calling `Read<float>` on a value that is actually stored as `double` is allowed and will convert for you.
    * Likewise, `Read<int>` from an internally stored `long` is allowed as long as the value is in range.

  In practice, this means:
  * Field **names** and **values** survive, but the exact original primitive shape (e.g. “this used to be `int` not `long`”) does not.
  * You are responsible for knowing which fields exist and what semantic type/constraints you expect (boolean flag vs. counter vs. enum, valid ranges, etc.) in your own code or wrapper layer.

* **Unity-specific JSON integration**

  JSON support is currently tied to `Unity.Serialization.Json`.  
  If you want to use other serializers, you’ll need to write your own adapter.

* **Handle validity**

  `StorageObject` and the various views are lightweight handles into a `Storage`.  
  They become invalid once the owning `Storage` is disposed or after certain migrations; generation checks are used to catch misuse.

---

## Roadmap / TODO

* [x] Path for locating a field/object from the root/any position of the storage
* [x] Subscribe to write events for specific designated fields within the storage
* [x] Support for any random unmanaged struct type.
* [x] Higher-level typed wrapper API (e.g., generated strongly-typed views).
* [x] Editor inspectors for debugging storages in the Unity Editor.
* [ ] Additional backends (e.g., file-based persistence helpers).

---

## License

This project is licensed under the **MIT License**.
See [`LICENSE`](./LICENSE) for details.

---

## Contact

**Minerva Game Studio**
📧 `library.of.meialia@gmail.com`
GitHub: [https://github.com/minerva-studio/meialian-bibliography](https://github.com/minerva-studio/meialian-bibliography)
