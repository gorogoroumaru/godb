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


		if string(input_buffer.buffer[0]) == "." {
			switch do_meta_command(input_buffer, table) {
			case META_COMMAND_SUCCESS:
				fmt.Println("success")
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("unrecognized command")
			}
		}

		statement := NewStatement()
		switch prepare_statement(input_buffer, statement) {
		case PREPARE_STRING_TOO_LONG:
			fmt.Println("String is too long")
		case PREPARE_NEGATIVE_ID:
			fmt.Println("ID must be positive")
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("syntax error. could not parse statement")
		case PREPARE_UNRECOGNIZED_STATEMENT:
			s := fmt.Sprintf("unrecognized at start of %#v", input_buffer.buffer)
			fmt.Println(s)
		}

		switch execute_statement(statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed")
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table Full")
		}
		fmt.Println("execution finished")
	}
}