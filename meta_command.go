package main

import (
	"fmt"
	"os"
)


const (
	META_COMMAND_SUCCESS = 0
	META_COMMAND_UNRECOGNIZED_COMMAND = 1
)

func do_meta_command(input_buffer *InputBuffer, table *Table) int {
	if input_buffer.buffer[:5] == ".exit" {
		close_input_buffer(input_buffer)
		db_close(table)
		os.Exit(0)
	} else if input_buffer.buffer[:6] == ".btree" {
		fmt.Println("Tree: ")
		print_leaf_node(*get_page(table.pager, 0))
		return META_COMMAND_SUCCESS
	} else if input_buffer.buffer[:10] == ".constants" {
		fmt.Println("constants: ")
		print_constants()
		return META_COMMAND_SUCCESS
	} else {
		fmt.Printf("Unrecognized command '%s'.\n", input_buffer.buffer)
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
	return META_COMMAND_SUCCESS
}
