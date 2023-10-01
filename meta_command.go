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
		free_table(table)
		os.Exit(0)
	} else {
		fmt.Printf("Unrecognized command '%s'.\n", input_buffer.buffer)
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
	return META_COMMAND_SUCCESS
}
