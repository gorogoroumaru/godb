package main

import (
	"encoding/binary"
	"fmt"
	"os"
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
)

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
	binary.LittleEndian.PutUint32(node[LEAF_NODE_NUM_CELLS_OFFSET:], 0)
}

func leaf_node_insert(cursor *Cursor, key uint32, value *Row) {
	node := get_page(cursor.table.pager, cursor.page_num)

	numCells := leaf_node_num_cells(*node)
	if numCells >= LEAF_NODE_MAX_CELLS {
		fmt.Println("Need to implement splitting a leaf node.")
		os.Exit(1)
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