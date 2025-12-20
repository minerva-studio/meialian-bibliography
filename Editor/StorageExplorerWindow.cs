#if UNITY_EDITOR

using Minerva.DataStorage.Serialization;
using System;
using System.Collections.Generic;
using UnityEditor;
using UnityEditor.IMGUI.Controls;
using UnityEngine;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Addressables-style Storage Explorer window.
    /// - Left: tree view (Container → Field → array elements / child containers)
    /// - Right: multi-column view (Name / Type / Value / Length / Offset / ID)
    /// - Alternating row stripes for readability
    /// - Supports inline editing for scalar and string (Char16[]) fields that are non-ref and non-Blob
    /// </summary>
    public sealed class StorageExplorerWindow : EditorWindow
    {
        [SerializeField] private TreeViewState _treeViewState;
        [SerializeField] private MultiColumnHeaderState _headerState;

        private StorageTreeView _treeView;
        private readonly Dictionary<ulong, Container> _snapshot = new();

        [MenuItem("Window/Storage Explorer")]
        private static void Open()
        {
            var window = GetWindow<StorageExplorerWindow>();
            window.titleContent = new GUIContent("Storage Explorer");
            window.Show();
        }

        private void OnEnable()
        {
            EnsureTreeView();
            RefreshSnapshot();
        }

        private void OnGUI()
        {
            EnsureTreeView();

            using (new EditorGUILayout.HorizontalScope(EditorStyles.toolbar))
            {
                if (GUILayout.Button("Refresh", EditorStyles.toolbarButton, GUILayout.Width(70)))
                {
                    RefreshSnapshot();
                }

                GUILayout.FlexibleSpace();
                EditorGUILayout.LabelField($"Containers: {_snapshot.Count} / Pool: {Container.Registry.PoolCount}", EditorStyles.miniLabel);
            }

            Rect rect = GUILayoutUtility.GetRect(0, 100000, 0, 100000);
            _treeView?.OnGUI(rect);
        }

        private void RefreshSnapshot()
        {
            _snapshot.Clear();

            try
            {
                Container.Registry.Shared.DebugCopyLiveContainers(_snapshot);
            }
            catch (Exception ex)
            {
                Debug.LogException(ex);
            }

            if (_treeView != null)
            {
                _treeView.SetSnapshot(_snapshot);
                _treeView.Reload();
                Repaint();
            }
        }

        private void EnsureTreeView()
        {
            if (_treeViewState == null)
                _treeViewState = new TreeViewState();

            if (_headerState == null || _headerState.columns == null || _headerState.columns.Length == 0)
            {
                var columns = new[]
                {
                    new MultiColumnHeaderState.Column
                    {
                        headerContent = new GUIContent("Name"),
                        width = 260,
                        minWidth = 160,
                        autoResize = true,
                        canSort = false
                    },
                    new MultiColumnHeaderState.Column
                    {
                        headerContent = new GUIContent("Type"),
                        width = 100,
                        minWidth = 80,
                        autoResize = false,
                        canSort = false
                    },
                    new MultiColumnHeaderState.Column
                    {
                        headerContent = new GUIContent("Value"),
                        width = 220,
                        minWidth = 140,
                        autoResize = true,
                        canSort = false
                    },
                    new MultiColumnHeaderState.Column
                    {
                        headerContent = new GUIContent("Length (bytes)"),
                        width = 90,
                        minWidth = 70,
                        autoResize = false,
                        canSort = false
                    },
                    new MultiColumnHeaderState.Column
                    {
                        headerContent = new GUIContent("Offset"),
                        width = 70,
                        minWidth = 60,
                        autoResize = false,
                        canSort = false
                    },
                    new MultiColumnHeaderState.Column
                    {
                        headerContent = new GUIContent("ID"),
                        width = 80,
                        minWidth = 60,
                        autoResize = false,
                        canSort = false
                    },
                };

                _headerState = new MultiColumnHeaderState(columns);
            }

            if (_treeView == null)
            {
                var header = new MultiColumnHeader(_headerState)
                {
                    height = 22f
                };

                _treeView = new StorageTreeView(_treeViewState, header);
                _treeView.SetSnapshot(_snapshot);
                _treeView.Reload();
            }
        }
        private static void CopyContainerJson(Container container)
        {
            if (container == null || container.ID == 0UL)
            {
                EditorUtility.DisplayDialog("Copy Failed", "Copy Failed：Container is not registerd or empty", "OK");
                return;
            }

            try
            {
                string json = JsonSerialization.ToJson(new StorageObject(container)).ToString();
                EditorGUIUtility.systemCopyBuffer = json;
            }
            catch (Exception ex)
            {
                Debug.LogException(ex);
                EditorUtility.DisplayDialog("Copy Failed", $"Encounter exception when serializing container：{ex.Message}", "OK");
            }
        }

        private static void CopyContainerBase64(Container container)
        {
            if (container == null || container.ID == 0UL)
            {
                EditorUtility.DisplayDialog("Copy Failed", "Copy Failed：Container is not registerd or empty", "OK");
                return;
            }
            try
            {
                string base64 = BinarySerialization.ToBase64(new StorageObject(container));
                EditorGUIUtility.systemCopyBuffer = base64;
            }
            catch (Exception ex)
            {
                Debug.LogException(ex);
                EditorUtility.DisplayDialog("Copy Failed", $"Encounter exception when serializing container：{ex.Message}", "OK");
            }
        }

        // =========================
        // TreeView Implementation
        // =========================

        private enum ColumnId
        {
            Name = 0,
            Type = 1,
            Value = 2,
            Length = 3,
            Offset = 4,
            Id = 5,
        }

        private sealed class StorageTreeView : TreeView
        {
            private Dictionary<ulong, Container> _snapshot;
            private int _nextId;

            private readonly GUIStyle _rightAlignMini;

            private sealed class Item : TreeViewItem
            {
                public enum NodeKind
                {
                    Root,
                    Field,
                    Info,
                    ArrayElement
                }

                public NodeKind Kind;
                public Container Container;
                public int FieldIndex = -1;  // valid when Kind == Field or ArrayElement
                public int ArrayIndex = -1;  // valid when Kind == ArrayElement

                /// <summary>
                /// Container.Generation at the time this tree item was built.
                /// Used to detect pooled reuse and stale items.
                /// </summary>
                public int GenerationSnapshot;

                public Container ReferencedContainer;
                public int ReferencedGenerationSnapshot;
            }

            public StorageTreeView(TreeViewState state, MultiColumnHeader header)
                : base(state, header)
            {
                showBorder = true;
                showAlternatingRowBackgrounds = true;
                rowHeight = EditorGUIUtility.singleLineHeight + 4f;

                _rightAlignMini = new GUIStyle(EditorStyles.miniLabel)
                {
                    alignment = TextAnchor.MiddleRight
                };
            }

            public void SetSnapshot(Dictionary<ulong, Container> snapshot)
            {
                _snapshot = snapshot;
            }

            protected override TreeViewItem BuildRoot()
            {
                _nextId = 1;
                var root = new Item
                {
                    id = 0,
                    depth = -1,
                    displayName = "Root"
                };

                try
                {
                    if (_snapshot == null || _snapshot.Count == 0)
                    {
                        root.children = new List<TreeViewItem>();
                        return root;
                    }

                    // Step 1: collect referenced containers
                    var referenced = new HashSet<ulong>();
                    foreach (var kv in _snapshot)
                    {
                        var c = kv.Value;
                        if (c == null) continue;
                        if (c.ID == 0UL) continue;

                        int fieldCount;
                        try
                        {
                            fieldCount = c.FieldCount;
                        }
                        catch
                        {
                            // Skip containers that are already invalid
                            continue;
                        }

                        for (int i = 0; i < fieldCount; i++)
                        {
                            ref var header = ref c.GetFieldHeader(i);
                            if (!header.IsRef) continue;

                            var refs = c.GetFieldData<ContainerReference>(in header);
                            for (int j = 0; j < refs.Length; j++)
                            {
                                ulong id = refs[j];
                                if (id == 0UL ||
                                    id == Container.Registry.ID.Empty ||
                                    id == Container.Registry.ID.Wild)
                                    continue;
                                referenced.Add(id);
                            }
                        }
                    }

                    // Step 2: find root containers (not referenced by any other)
                    var roots = new List<Container>();
                    foreach (var kv in _snapshot)
                    {
                        var c = kv.Value;
                        if (c == null) continue;
                        if (c.ID == 0UL) continue;

                        if (!referenced.Contains(kv.Key))
                            roots.Add(c);
                    }

                    if (roots.Count == 0)
                    {
                        // If no roots found, treat all containers as top-level
                        foreach (var kv in _snapshot)
                        {
                            var c = kv.Value;
                            if (c != null && c.ID != 0UL)
                                roots.Add(c);
                        }
                    }

                    var pathVisited = new HashSet<ulong>();
                    foreach (var c in roots)
                    {
                        var rootItem = CreateRootItem(c);
                        root.AddChild(rootItem);
                        BuildContainerChildrenRecursive(c, rootItem, pathVisited);
                    }

                    root.children ??= new List<TreeViewItem>();

                    SetupDepthsFromParentsAndChildren(root);
                }
                catch (Exception ex)
                {
                    Debug.LogException(ex);
                    root.children = new List<TreeViewItem>();
                }

                return root;
            }

            private Item CreateRootItem(Container container)
            {
                return new Item
                {
                    id = _nextId++,
                    depth = 0,
                    Kind = Item.NodeKind.Root,
                    Container = container,
                    GenerationSnapshot = container.Generation,
                    displayName = (string.IsNullOrEmpty(container.Name) ? $"Storage (ID={container.ID})" : $"Storage '{container.Name}' (ID={container.ID})")
                };
            }

            private Item CreateContainerItem(Container container)
            {
                return new Item
                {
                    id = _nextId++,
                    depth = 0,
                    Kind = Item.NodeKind.Root,
                    Container = container,
                    GenerationSnapshot = container.Generation,
                    displayName = $"Container (ID={container.ID})"
                };
            }

            private void BuildContainerChildrenRecursive(Container container, Item parent, HashSet<ulong> pathVisited)
            {
                if (container == null)
                    return;

                if (!pathVisited.Add(container.ID))
                {
                    var info = new Item
                    {
                        id = _nextId++,
                        Kind = Item.NodeKind.Info,
                        Container = container,
                        GenerationSnapshot = container.Generation,
                        displayName = "<cycle detected>"
                    };
                    parent.AddChild(info);
                    return;
                }

                int fieldCount;
                try
                {
                    fieldCount = container.FieldCount;
                }
                catch
                {
                    pathVisited.Remove(container.ID);
                    return;
                }

                for (int i = 0; i < fieldCount; i++)
                {
                    ref var header = ref container.GetFieldHeader(i);
                    string fieldName = container.GetFieldName(in header).ToString();

                    var fieldItem = new Item
                    {
                        id = _nextId++,
                        Kind = Item.NodeKind.Field,
                        Container = container,
                        FieldIndex = i,
                        GenerationSnapshot = container.Generation,
                        displayName = fieldName
                    };
                    parent.AddChild(fieldItem);

                    // Array field: either ref array or value-type array
                    if (TryGetInlineOrRefArray(container, i, out var arr))
                    {
                        Container arrayContainer = arr.Handle.Container;
                        fieldItem.displayName = fieldName + $" (ID={arrayContainer.ID})";
                        // string: dir draw
                        if (arr.IsString)
                        {
                            fieldItem.Container = arrayContainer;
                            fieldItem.FieldIndex = arr.IsExternalArray ? 0 : fieldItem.FieldIndex;
                            fieldItem.GenerationSnapshot = arrayContainer.Generation;
                        }
                        // Ref array: spawn child containers
                        else if (arr.Type == ValueType.Ref)
                        {
                            var refs = arr.References;
                            var count = arr.Length;

                            for (int j = 0; j < count; j++)
                            {
                                ulong id = refs[j];
                                if (id == 0UL ||
                                    id == Container.Registry.ID.Empty ||
                                    id == Container.Registry.ID.Wild)
                                    continue;

                                if (!_snapshot.TryGetValue(id, out var child) || child == null)
                                    continue;

                                var childItem = CreateContainerItem(child);
                                childItem.displayName = $"{fieldName}[{j}] (ID={child.ID})";
                                fieldItem.AddChild(childItem);
                                BuildContainerChildrenRecursive(child, childItem, pathVisited);
                            }
                        }
                        else
                        {
                            // Non-ref array: create element nodes
                            for (int j = 0; j < arr.Length; j++)
                            {
                                var elementItem = new Item
                                {
                                    id = _nextId++,
                                    Kind = Item.NodeKind.ArrayElement,
                                    Container = arrayContainer,
                                    FieldIndex = arr.IsExternalArray ? 0 : fieldItem.FieldIndex,
                                    ArrayIndex = j,
                                    GenerationSnapshot = arrayContainer.Generation,
                                    displayName = $"{fieldName}[{j}]"
                                };
                                fieldItem.AddChild(elementItem);
                            }
                        }
                    }
                    // Single ref field
                    else if (header.IsRef)
                    {
                        var refs = container.GetFieldData<ContainerReference>(in header);
                        if (refs.Length == 0)
                            continue;

                        ulong id = refs[0];
                        if (id == Container.Registry.ID.Empty || id == Container.Registry.ID.Wild)
                            continue;

                        if (!_snapshot.TryGetValue(id, out var child) || child == null)
                            continue;

                        fieldItem.displayName = $"{fieldName} (ID={child.ID})";
                        fieldItem.ReferencedContainer = child;
                        fieldItem.ReferencedGenerationSnapshot = child.Generation;

                        BuildContainerChildrenRecursive(child, fieldItem, pathVisited);
                    }
                }

                pathVisited.Remove(container.ID);
            }

            protected override void RowGUI(RowGUIArgs args)
            {
                var item = (Item)args.item;

                for (int i = 0; i < args.GetNumVisibleColumns(); i++)
                {
                    var column = (ColumnId)args.GetColumn(i);
                    Rect cellRect = args.GetCellRect(i);
                    CenterRectUsingSingleLineHeight(ref cellRect);
                    DrawCell(cellRect, item, column, ref args);
                }

                // Right-click context menu
                if (Event.current.type == EventType.ContextClick)
                {
                    var rowRect = args.rowRect;
                    if (rowRect.Contains(Event.current.mousePosition))
                    {
                        ShowContextMenu(item);
                        Event.current.Use();
                    }
                }
            }

            private void ShowContextMenu(Item item)
            {
                var menu = new GenericMenu();
                bool live = TryGetLiveContainer(item, out var container);

                if (item.Kind == Item.NodeKind.Root && live && container != null)
                {
                    menu.AddItem(new GUIContent("Copy JSON..."), false, () =>
                    {
                        CopyContainerJson(container);
                    });
                    menu.AddItem(new GUIContent("Copy Base64..."), false, () =>
                    {
                        CopyContainerBase64(container);
                    });
                }
                else if (item.Kind == Item.NodeKind.Field && live && container != null)
                {
                    menu.AddItem(new GUIContent("Copy Container JSON..."), false, () =>
                    {
                        CopyContainerJson(container);
                    });
                    menu.AddItem(new GUIContent("Copy Container Base64..."), false, () =>
                    {
                        CopyContainerBase64(container);
                    });
                }
                else
                {
                    menu.AddDisabledItem(new GUIContent("Copy JSON..."));
                }

                menu.ShowAsContext();
            }

            private void DrawCell(Rect cellRect, Item item, ColumnId column, ref RowGUIArgs args)
            {
                switch (column)
                {
                    case ColumnId.Name:
                        {
                            float indent = GetContentIndent(item);
                            cellRect.xMin += indent;
                            EditorGUI.LabelField(cellRect, item.displayName);
                            break;
                        }

                    case ColumnId.Type:
                        {
                            if (item.Kind == Item.NodeKind.ArrayElement && item.FieldIndex >= 0)
                            {
                                if (TryGetLiveContainer(item, out var c))
                                {
                                    if (TryGetInlineOrRefArray(c, item.FieldIndex, out var arr))
                                        EditorGUI.LabelField(cellRect, TypeUtil.ToString(arr.Type));
                                    else
                                        EditorGUI.LabelField(cellRect, "(N/A)");
                                }
                                else
                                {
                                    EditorGUI.LabelField(cellRect, "<stale>");
                                }
                            }
                            else if (item.Kind == Item.NodeKind.Field && item.FieldIndex >= 0)
                            {
                                if (TryGetLiveContainer(item, out var c))
                                {
                                    if (TryGetInlineOrRefArray(c, item.FieldIndex, out var arr))
                                    {
                                        EditorGUI.LabelField(cellRect, arr.FieldType.ToString());
                                    }
                                    else
                                    {
                                        ref var header = ref c.GetFieldHeader(item.FieldIndex);
                                        EditorGUI.LabelField(cellRect, header.FieldType.ToString());
                                    }
                                }
                                else
                                {
                                    EditorGUI.LabelField(cellRect, "<stale>");
                                }
                            }
                            else if (item.Kind == Item.NodeKind.Root && item.Container != null)
                            {
                                if (IsContainerLive(item.Container, item.GenerationSnapshot))
                                    EditorGUI.LabelField(cellRect, "Container");
                                else
                                    EditorGUI.LabelField(cellRect, "Container (stale)");
                            }
                            else if (item.Kind == Item.NodeKind.Info)
                            {
                                EditorGUI.LabelField(cellRect, "<info>");
                            }
                            break;
                        }

                    case ColumnId.Value:
                        {
                            DrawValueCell(cellRect, item);
                            break;
                        }

                    case ColumnId.Length:
                        {
                            if (item.Kind == Item.NodeKind.ArrayElement && item.FieldIndex >= 0 && item.ArrayIndex >= 0)
                            {
                                if (TryGetLiveContainer(item, out var c))
                                {
                                    ref var header = ref c.GetFieldHeader(item.FieldIndex);
                                    EditorGUI.LabelField(cellRect, header.ElemSize.ToString(), _rightAlignMini);
                                }
                                else
                                {
                                    EditorGUI.LabelField(cellRect, "-", _rightAlignMini);
                                }
                            }
                            else if (item.Kind == Item.NodeKind.Field && item.FieldIndex >= 0)
                            {
                                if (TryGetLiveContainer(item, out var c))
                                {
                                    ref var header = ref c.GetFieldHeader(item.FieldIndex);
                                    string length;
                                    if (header.IsRef)
                                    {
                                        var r = c.GetFieldData<ContainerReference>(in header)[0];
                                        _snapshot.TryGetValue(r, out var value);
                                        length = $"({value?.Length ?? ContainerReference.Size})";
                                    }
                                    else length = header.Length.ToString();
                                    EditorGUI.LabelField(cellRect, length, _rightAlignMini);
                                }
                                else
                                {
                                    EditorGUI.LabelField(cellRect, "-", _rightAlignMini);
                                }
                            }
                            else if (item.Kind == Item.NodeKind.Root)
                            {
                                if (TryGetLiveContainer(item, out var c))
                                {
                                    EditorGUI.LabelField(cellRect, $"\"{c.Length}\"", _rightAlignMini);
                                }
                                else
                                {
                                    EditorGUI.LabelField(cellRect, "-", _rightAlignMini);
                                }
                            }
                            break;
                        }

                    case ColumnId.Offset:
                        {
                            if (item.Kind == Item.NodeKind.ArrayElement && item.FieldIndex >= 0 && item.ArrayIndex >= 0)
                            {
                                if (TryGetLiveContainer(item, out var c))
                                {
                                    ref var header = ref c.GetFieldHeader(item.FieldIndex);
                                    int offset = header.DataOffset + item.ArrayIndex * header.ElemSize;
                                    EditorGUI.LabelField(cellRect, offset.ToString(), _rightAlignMini);
                                }
                                else
                                {
                                    EditorGUI.LabelField(cellRect, "-", _rightAlignMini);
                                }
                            }
                            else if (item.Kind == Item.NodeKind.Field && item.FieldIndex >= 0)
                            {
                                if (TryGetLiveContainer(item, out var c))
                                {
                                    ref var header = ref c.GetFieldHeader(item.FieldIndex);
                                    EditorGUI.LabelField(cellRect, header.DataOffset.ToString(), _rightAlignMini);
                                }
                                else
                                {
                                    EditorGUI.LabelField(cellRect, "-", _rightAlignMini);
                                }
                            }
                            break;
                        }

                    case ColumnId.Id:
                        {
                            if (item.Container != null)
                            {
                                var idText = item.Container.ID == 0UL
                                    ? "0 (unregistered)"
                                    : item.Container.ID.ToString();
                                EditorGUI.LabelField(cellRect, idText, _rightAlignMini);
                            }
                            break;
                        }
                }
            }

            private void DrawValueCell(Rect cellRect, Item item)
            {
                // Only Field and ArrayElement have values to display
                if (item.Kind != Item.NodeKind.Field && item.Kind != Item.NodeKind.ArrayElement)
                    return;

                if (!TryGetLiveContainer(item, out var container))
                {
                    EditorGUI.LabelField(cellRect, "<stale>");
                    return;
                }

                ValueView view;
                string valueText;
                bool isInlineArray;
                // Array element: show the concrete element value
                if (item.Kind == Item.NodeKind.ArrayElement)
                {
                    if (item.FieldIndex < 0 || item.ArrayIndex < 0)
                        return;

                    if (!TryGetInlineOrRefArray(container, item.FieldIndex, out var arr))
                    {
                        EditorGUI.LabelField(cellRect, "<invalid array>");
                        return;
                    }

                    if ((uint)item.ArrayIndex >= (uint)arr.Length)
                    {
                        EditorGUI.LabelField(cellRect, "<out of range>");
                        return;
                    }

                    view = arr.Raw[item.ArrayIndex];
                    valueText = view.ToString();
                    isInlineArray = false;
                }
                else
                {
                    // Field node (scalar or array summary)
                    if (item.FieldIndex < 0)
                        return;

                    ref var fieldHeader = ref container.GetFieldHeader(item.FieldIndex);
                    view = container.GetValueView(in fieldHeader);
                    valueText = view.ToString();
                    isInlineArray = fieldHeader.IsInlineArray;

                    bool canEdit = !isInlineArray || (isInlineArray && fieldHeader.Type == ValueType.Char16);

                    if (!canEdit)
                    {
                        EditorGUI.LabelField(cellRect, valueText);
                        return;
                    }
                }

                // Non-editable cases: ref or Blob
                if (view.Type == ValueType.Ref)
                {
                    EditorGUI.LabelField(cellRect, "-");
                    return;
                }
                if (view.Type == ValueType.Blob)
                {
                    EditorGUI.LabelField(cellRect, view.ToHex());
                    return;
                }

                // toggle for bool
                if (view.Type == ValueType.Bool)
                {
                    EditorGUI.BeginChangeCheck();
                    bool value = view.Read<bool>();
                    var newValue = EditorGUI.Toggle(cellRect, value);
                    if (value != newValue)
                        view.Write(newValue);
                }
                // default
                else
                {
                    EditorGUI.BeginChangeCheck();
                    string newText = EditorGUI.DelayedTextField(cellRect, valueText);
                    if (EditorGUI.EndChangeCheck())
                    {
                        var obj = new StorageObject(container);
                        if (isInlineArray && view.Type == ValueType.Char16) obj.WriteString(newText);
                        else TryWriteValue(view, newText);
                    }
                }
            }

            private void TryWriteValue(ValueView view, string text)
            {
                var vt = view.Type;
                try
                {
                    switch (vt)
                    {
                        case ValueType.Bool:
                            if (bool.TryParse(text, out var b))
                            {
                                view.Write(b);
                            }
                            else if (int.TryParse(text, out var bi))
                            {
                                view.Write(bi != 0);
                            }
                            break;

                        case ValueType.Int8:
                            if (sbyte.TryParse(text, out var i8))
                                view.Write(i8);
                            break;

                        case ValueType.UInt8:
                            if (byte.TryParse(text, out var u8))
                                view.Write(u8);
                            break;

                        case ValueType.Char16:
                            if (!string.IsNullOrEmpty(text))
                                view.Write(text[0]);
                            break;

                        case ValueType.Int16:
                            if (short.TryParse(text, out var i16))
                                view.Write(i16);
                            break;

                        case ValueType.UInt16:
                            if (ushort.TryParse(text, out var u16))
                                view.Write(u16);
                            break;

                        case ValueType.Int32:
                            if (int.TryParse(text, out var i32))
                                view.Write(i32);
                            break;

                        case ValueType.UInt32:
                            if (uint.TryParse(text, out var u32))
                                view.Write(u32);
                            break;

                        case ValueType.Int64:
                            if (long.TryParse(text, out var i64))
                                view.Write(i64);
                            break;

                        case ValueType.UInt64:
                            if (ulong.TryParse(text, out var u64))
                                view.Write(u64);
                            break;

                        case ValueType.Float32:
                            if (float.TryParse(text, out var f32))
                                view.Write(f32);
                            break;

                        case ValueType.Float64:
                            if (double.TryParse(text, out var f64))
                                view.Write(f64);
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Debug.LogWarning($"[StorageExplorer] Failed to write value '{text}' as {vt}: {ex.Message}");
                }
            }

            /// <summary>
            /// Returns true if the container still represents the same live instance
            /// captured when the tree item was created:
            /// - ID != 0 (still registered), and
            /// - Generation matches the snapshot.
            /// </summary>
            private static bool IsContainerLive(Container container, int generationSnapshot)
            {
                if (container == null) return false;
                if (container.ID == 0UL)
                    return false;
                if (container.Generation != generationSnapshot)
                    return false;

                return true;
            }

            private bool TryGetInlineOrRefArray(Container container, int index, out StorageArray storageArray)
            {
                storageArray = default;
                ref var header = ref container.GetFieldHeader(index);
                if (header.IsInlineArray)
                {
                    storageArray = new StorageArray(container, container.GetFieldName(in header).ToString());
                    return true;
                }

                if (!header.IsRef) return false;
                var r = container.GetFieldData<ContainerReference>(in header)[0];
                if (!_snapshot.TryGetValue(r, out var child)) return false;
                if (child.IsDisposed(child.Generation) || !child.IsArray) return false;
                storageArray = new StorageArray(child);
                return true;
            }

            /// <summary>
            /// Tries to get a live container for a given item. Returns false
            /// if unregistered, disposed, or reused (generation mismatch).
            /// </summary>
            private static bool TryGetLiveContainer(Item item, out Container container)
            {
                if (item.Kind == Item.NodeKind.Field && item.ReferencedContainer != null)
                {
                    container = item.Container;
                    return IsContainerLive(container, item.GenerationSnapshot)
                        && IsContainerLive(item.ReferencedContainer, item.ReferencedGenerationSnapshot);
                }

                container = item.Container;
                if (container == null)
                    return false;

                return IsContainerLive(container, item.GenerationSnapshot);
            }
        }
    }
}

#endif