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

func main() {
	input_buffer := NewInputBuffer()
	for {
		print_prompt()
		read_input(input_buffer)

		if input_buffer.buffer == ".exit" {
			os.Exit(0)
		} else {
			fmt.Printf("Unrecognized command '%s'.\n", input_buffer.buffer)
		}
	}
}
