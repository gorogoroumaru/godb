package main

import (
	"bufio"
	"fmt"
	"os"
)

type InputBuffer struct {
	buffer        string
	buffer_length int
	input_length  int
}

func NewInputBuffer() *InputBuffer {
	return &InputBuffer{
		buffer:        "",
		buffer_length: 0,
		input_length:  0,
	}
}

func print_prompt() { fmt.Print("db > ") }

func read_input(input_buffer *InputBuffer) {
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input_buffer.buffer = input[:len(input)-1]
	input_buffer.input_length = len(input) - 1
}

const (
	META_COMMAND_SUCCESS = 0
	META_COMMAND_UNRECOGNIZED_COMMAND = 1
)


func do_meta_command(input_buffer *InputBuffer) int {
	if input_buffer.buffer == ".exit" {
		os.Exit(0)
	} else {
		fmt.Printf("Unrecognized command '%s'.\n", input_buffer.buffer)
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
	return META_COMMAND_SUCCESS
}

const (
	PREPARE_SUCCESS = 0
	PREPARE_UNRECOGNIZED_STATEMENT = 1
)

const (
	STATEMENT_INSERT = 0
	STATEMENT_SELECT = 1
)

type Statement struct {
	statement_type int
}

func NewStatement() *Statement {
	return &Statement{}
}

func prepare_statement(input_buffer *InputBuffer, statement *Statement) int {
	if input_buffer.buffer[:6] == "insert" {
		statement.statement_type = STATEMENT_INSERT
		return PREPARE_SUCCESS
	}
	if input_buffer.buffer[:6] == "select" {
		statement.statement_type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}
	return PREPARE_UNRECOGNIZED_STATEMENT
}

func execute_statement(statement *Statement) {
	switch statement.statement_type {
	case STATEMENT_INSERT:
		fmt.Println("this is insert")
	case STATEMENT_SELECT:
		fmt.Println("this is select")
	}
}

func main() {
	input_buffer := NewInputBuffer()
	for {
		print_prompt()
		read_input(input_buffer)


		if string(input_buffer.buffer[0]) == "." {
			switch do_meta_command(input_buffer) {
			case META_COMMAND_SUCCESS:
				fmt.Println("success")
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("unrecognized command")
			}
		}

		statement := NewStatement()
		switch prepare_statement(input_buffer, statement) {
		case PREPARE_UNRECOGNIZED_STATEMENT:
			s := fmt.Sprintf("unrecognized at start of %#v", input_buffer.buffer)
			fmt.Println(s)
		}

		execute_statement(statement)
		fmt.Println("execution finished")
	}
}