package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"unsafe"
)

const (
	NODE_INTERNAL = iota
	NODE_LEAF
)

const (
	NODE_TYPE_SIZE           = uint32(unsafe.Sizeof(uint8(0)))
	NODE_TYPE_OFFSET         = uint32(0)
	IS_ROOT_SIZE             = uint32(unsafe.Sizeof(uint8(0)))
	IS_ROOT_OFFSET           = NODE_TYPE_SIZE
	PARENT_POINTER_SIZE      = uint32(unsafe.Sizeof(uint32(0)))
	PARENT_POINTER_OFFSET    = IS_ROOT_OFFSET + IS_ROOT_SIZE
	COMMON_NODE_HEADER_SIZE  = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE

	LEAF_NODE_NUM_CELLS_SIZE   = uint32(unsafe.Sizeof(uint32(0)))
	LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
	LEAF_NODE_HEADER_SIZE      = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE
)

const (
	LEAF_NODE_KEY_SIZE        = uint32(unsafe.Sizeof(uint32(0)))
	LEAF_NODE_KEY_OFFSET      = uint32(0)
	LEAF_NODE_VALUE_SIZE      = uint32(ROW_SIZE)
	LEAF_NODE_VALUE_OFFSET    = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
	LEAF_NODE_CELL_SIZE       = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
	LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	LEAF_NODE_MAX_CELLS       = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
	LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
	LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;
)

// ブログではこの構造を可視化する
// right child, left childのポインタが格納されている部分のビットマップとBtreeの木を対応付けする

const (
	INTERNAL_NODE_NUM_KEYS_SIZE = 4
	INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE
	INTERNAL_NODE_RIGHT_CHILD_SIZE = 4
	INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
	INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

	INTERNAL_NODE_KEY_SIZE = 4
	INTERNAL_NODE_CHILD_SIZE = 4
	INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE
)

func internal_node_num_keys(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[INTERNAL_NODE_NUM_KEYS_OFFSET:])
}

func internal_node_right_child(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:])
}

func internal_node_cell(node []byte, cellNum uint32) []byte {
	return node[INTERNAL_NODE_HEADER_SIZE + cellNum*INTERNAL_NODE_CELL_SIZE:]
}

func internal_node_child(node []byte, childNum uint32) []byte {
	numKeys := internal_node_num_keys(node)
	if childNum > numKeys {
		log.Fatalf("Tried to access child_num %d > num_keys %d\n", childNum, numKeys)
		return nil
	} else if childNum == numKeys {
		return node[internal_node_right_child(node):]
	} else {
		return internal_node_cell(node, childNum)
	}
}

func internal_node_key(node []byte, keyNum uint32) []byte {
	return internal_node_cell(node, keyNum)[INTERNAL_NODE_CHILD_SIZE:]
}

func get_node_max_key(node []byte) uint32 {
	switch get_node_type(node) {
	case NODE_INTERNAL:
		return binary.LittleEndian.Uint32(internal_node_key(node, internal_node_num_keys(node)-1))
	case NODE_LEAF:
		return leaf_node_key(node, leaf_node_num_cells(node)-1)
	default:
		return 0
	}
}

func is_node_root(node []byte) bool {
	value := node[IS_ROOT_OFFSET]
	return value == 1
}

func set_node_root(node []byte, isRoot bool) {
	var value byte
	if isRoot {
		value = 1
	} else {
		value = 0
	}
	node[IS_ROOT_OFFSET] = value
}

func initialize_internal_node(node []byte) {
	set_node_type(node, NODE_INTERNAL)
	set_node_root(node, false)
	binary.LittleEndian.PutUint32(node[INTERNAL_NODE_NUM_KEYS_OFFSET:], 0)
}


func create_new_root(table *Table, rightChildPageNum uint32) {
	root := get_page(table.pager, table.root_page_num)
	leftChildPageNum := get_unused_page_num(table.pager)
	leftChild := get_page(table.pager, leftChildPageNum)

	copy(*leftChild, *root)
	set_node_root(*leftChild, false)

	initialize_internal_node(*root)
	set_node_root(*root, true)
	binary.LittleEndian.PutUint32((*root)[INTERNAL_NODE_NUM_KEYS_OFFSET:], 1)
	binary.LittleEndian.PutUint32(internal_node_child(*root, 0), leftChildPageNum)
	leftChildMaxKey := get_node_max_key(*leftChild)
	binary.LittleEndian.PutUint32(internal_node_key(*root, 0), leftChildMaxKey)
	binary.LittleEndian.PutUint32((*root)[INTERNAL_NODE_RIGHT_CHILD_OFFSET:], rightChildPageNum)
}


func leaf_node_num_cells(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[LEAF_NODE_NUM_CELLS_OFFSET:])
}

func leaf_node_cell(node []byte, cell_num uint32) []byte {
	return node[LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE:]
}

func leaf_node_key(node []byte, cellNum uint32) uint32 {
	return binary.LittleEndian.Uint32(leaf_node_cell(node, cellNum))
}

func leaf_node_value(node []byte, cell_num uint32) []byte {
	return leaf_node_cell(node, cell_num)[LEAF_NODE_KEY_SIZE:]
}

func leaf_node_find(table *Table, page_num uint32, key uint32) *Cursor {
	node := get_page(table.pager, page_num)
	numCells := leaf_node_num_cells(*node)

	cursor := &Cursor{
		table:   table,
		page_num: page_num,
	}

	minIndex := uint32(0)
	onePastMaxIndex := numCells

	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := leaf_node_key(*node, index)
		if key == keyAtIndex {
			cursor.cell_num = index
			return cursor
		}
		if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}

	cursor.cell_num = minIndex
	return cursor
}

func leaf_node_split_and_insert(cursor *Cursor, key uint32, value *Row) {
	oldNode := get_page(cursor.table.pager, cursor.page_num)
	newPageNum := get_unused_page_num(cursor.table.pager)
	newNode := get_page(cursor.table.pager, newPageNum)
	initialize_leaf_node(*newNode)

	for i := int(LEAF_NODE_MAX_CELLS); i >= 0; i-- {
		var destinationNode []byte
		if i >= int(LEAF_NODE_LEFT_SPLIT_COUNT) {
			destinationNode = *newNode
		} else {
			destinationNode = *oldNode
		}
		indexWithinNode := i % int(LEAF_NODE_LEFT_SPLIT_COUNT)
		destination := leaf_node_cell(destinationNode, uint32(indexWithinNode))

		if i == int(cursor.cell_num) {
			serialize_row(value, destination)
		} else if i > int(cursor.cell_num) {
			copy(destination, leaf_node_cell(*oldNode, uint32(i-1)))
		} else {
			copy(destination, leaf_node_cell(*oldNode, uint32(i)))
		}
	}

	binary.LittleEndian.PutUint32((*oldNode)[LEAF_NODE_NUM_CELLS_OFFSET:], LEAF_NODE_LEFT_SPLIT_COUNT)
	binary.LittleEndian.PutUint32((*newNode)[LEAF_NODE_NUM_CELLS_OFFSET:], LEAF_NODE_RIGHT_SPLIT_COUNT)

	if is_node_root(*oldNode) {
		create_new_root(cursor.table, newPageNum)
	} else {
		log.Fatal("Need to implement updating parent after split")
	}
}

func get_node_type(node []byte) int {
	value := int(node[NODE_TYPE_OFFSET])
	return value
}

func set_node_type(node []byte, nodeType int) {
	value := uint8(nodeType)
	node[NODE_TYPE_OFFSET] = value
}

func printConstants() {
	fmt.Printf("ROW_SIZE: %d\n", ROW_SIZE)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS)
}

func printLeafNode(node []byte) {
	numCells := leaf_node_num_cells(node)
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32(0); i < numCells; i++ {
		key := leaf_node_key(node, i)
		fmt.Printf("  - %d : %d\n", i, key)
	}
}

func initialize_leaf_node(node []byte) {
	set_node_type(node, NODE_LEAF)
	set_node_root(node, false)
	binary.LittleEndian.PutUint32(node[LEAF_NODE_NUM_CELLS_OFFSET:], 0)
}

func leaf_node_insert(cursor *Cursor, key uint32, value *Row) {
	node := get_page(cursor.table.pager, cursor.page_num)

	numCells := leaf_node_num_cells(*node)
	if numCells >= LEAF_NODE_MAX_CELLS {
		leaf_node_split_and_insert(cursor, key, value);
		return
	}

	if cursor.cell_num < numCells {
		for i := numCells; i > cursor.cell_num; i-- {
			copy(leaf_node_cell(*node, i), leaf_node_cell(*node, i-1))
		}
	}

	binary.LittleEndian.PutUint32((*node)[LEAF_NODE_NUM_CELLS_OFFSET:], numCells+1)
	binary.LittleEndian.PutUint32(leaf_node_cell(*node, cursor.cell_num), key)
	serialize_row(value, leaf_node_value(*node, cursor.cell_num))
}


func print_constants() {
    fmt.Printf("ROW_SIZE: %d\n", ROW_SIZE)
    fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE)
    fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE)
    fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE)
    fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS)
    fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS)
}

func print_leaf_node(node []byte) {
    numCells := leaf_node_num_cells(node)
    fmt.Printf("leaf (size %d)\n", numCells)
    for i := uint32(0); i < numCells; i++ {
        key := leaf_node_key(node, i)
        fmt.Printf("  - %d : %d\n", i, key)
    }
}

func indent(level uint32) {
    for i := uint32(0); i < level; i++ {
        fmt.Print("  ")
    }
}

func print_tree(pager *Pager, pageNum uint32, indentationLevel uint32) {
    node := get_page(pager, pageNum)
    var numKeys, child uint32

    switch get_node_type(*node) {
    case NODE_LEAF:
        numKeys = leaf_node_num_cells(*node)
        indent(indentationLevel)
        fmt.Printf("- leaf (size %d)\n", numKeys)
        for i := uint32(0); i < numKeys; i++ {
            indent(indentationLevel + 1)
            fmt.Printf("- %d\n", leaf_node_key(*node, i))
        }
    case NODE_INTERNAL:
        numKeys = internal_node_num_keys(*node)
        indent(indentationLevel)
        fmt.Printf("- internal (size %d)\n", numKeys)
        for i := uint32(0); i < numKeys; i++ {
            child = binary.LittleEndian.Uint32(internal_node_child(*node, i))
            print_tree(pager, child, indentationLevel+1)

            indent(indentationLevel + 1)
            fmt.Printf("- key %d\n", binary.LittleEndian.Uint32(internal_node_key(*node, i)))
        }
        child = internal_node_right_child(*node)
        print_tree(pager, child, indentationLevel+1)
    }
}
