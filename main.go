package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Must supply a database filename.")
		os.Exit(1)
	}

	filename := os.Args[1]
	table := db_open(filename)

	input_buffer := new_input_buffer()

	for {
		print_prompt()
		read_input(input_buffer)

		if len(input_buffer.buffer) == 0 {
			continue
		}

		if string(input_buffer.buffer[0]) == "." {
			switch do_meta_command(input_buffer, table) {
			case META_COMMAND_SUCCESS:
				fmt.Println("success")
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("unrecognized command")
				continue
			}
		}

		statement := NewStatement()
		switch prepare_statement(input_buffer, statement) {
		case PREPARE_STRING_TOO_LONG:
			fmt.Println("String is too long")
			continue
		case PREPARE_NEGATIVE_ID:
			fmt.Println("ID must be positive")
			continue
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("syntax error. could not parse statement")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			s := fmt.Sprintf("unrecognized at start of %#v", input_buffer.buffer)
			fmt.Println(s)
			continue
		}

		switch execute_statement(statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed")
		case EXECUTE_DUPLICATE_KEY:
			fmt.Println("Error: Duplicate Key")
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table Full")
		}
		fmt.Println("execution finished")
	}
}