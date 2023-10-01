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

func new_input_buffer() *InputBuffer {
	return &InputBuffer{
		buffer:        "",
		buffer_length: 0,
		input_length:  0,
	}
}

func close_input_buffer(input_buffer *InputBuffer) {
	input_buffer = nil
}

func print_prompt() { fmt.Print("db > ") }

func read_input(input_buffer *InputBuffer) {
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input_buffer.buffer = input[:len(input)-1]
	input_buffer.input_length = len(input) - 1
}