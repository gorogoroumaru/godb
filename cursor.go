package main

import (
	"fmt"
	"os"
)

type Cursor struct {
	table       *Table
	page_num    uint32
	cell_num    uint32
	end_of_table  bool // Indicates a position one past the last element
}

func table_start(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.table = table
	cursor.page_num = table.root_page_num
	cursor.cell_num = 0

	root_node := get_page(table.pager, table.root_page_num)
	num_cells := leaf_node_num_cells(*root_node)
	cursor.end_of_table = (num_cells == 0)

	return cursor
}

func table_find(table *Table, key uint32) *Cursor {
	rootPageNum := table.root_page_num
	rootNode := get_page(table.pager, rootPageNum)

	fmt.Println(get_node_type(*rootNode))
	if get_node_type(*rootNode) == NODE_LEAF {
		return leaf_node_find(table, rootPageNum, key)
	} else {
		fmt.Println("Need to implement searching an internal node")
		os.Exit(1)
	}
	return nil
}

func cursor_value(cursor *Cursor) []byte {
	row_num := cursor.page_num
	page_num := row_num / uint32(ROWS_PER_PAGE)
	page := get_page(cursor.table.pager, page_num)

	return leaf_node_value(*page, cursor.cell_num)
}

func cursor_advance(cursor *Cursor) {
	page_num := cursor.page_num
	node := get_page(cursor.table.pager, page_num)
	cursor.cell_num += 1
	if cursor.cell_num >= leaf_node_num_cells(*node) {
		cursor.end_of_table = true
	}
}
