package main

import (
	"strconv"
	"strings"
)

const (
	PREPARE_SUCCESS = iota
	PREPARE_NEGATIVE_ID
	PREPARE_STRING_TOO_LONG
	PREPARE_SYNTAX_ERROR
	PREPARE_UNRECOGNIZED_STATEMENT
)

const (
	STATEMENT_INSERT = 0
	STATEMENT_SELECT = 1
)

const (
	EXECUTE_SUCCESS = iota
	EXECUTE_TABLE_FULL
)

type Statement struct {
	statement_type int
	row_to_insert Row
}

func NewStatement() *Statement {
	return &Statement{}
}

func prepare_insert(input_buffer *InputBuffer, statement *Statement) int {
	statement.statement_type = STATEMENT_INSERT
	args := strings.Split(input_buffer.buffer, " ")

	if len(args) <= 3 {
		return PREPARE_SYNTAX_ERROR
	}

	id, _ := strconv.ParseUint(args[1], 10, 32)

	if id < 0 {
		return PREPARE_NEGATIVE_ID
	}
	if len(args[2]) > COLUMN_USERNAME_SIZE {
		return PREPARE_STRING_TOO_LONG
	}
	if len(args[3]) > COLUMN_EMAIL_SIZE {
		return PREPARE_STRING_TOO_LONG
	}

	statement.row_to_insert.id = uint32(id)

	copy(statement.row_to_insert.username[:], args[2])
	copy(statement.row_to_insert.email[:], args[3])

	return PREPARE_SUCCESS
}

func prepare_statement(input_buffer *InputBuffer, statement *Statement) int {
	if strings.HasPrefix(input_buffer.buffer, "insert") {
		return prepare_insert(input_buffer, statement)
	}
	if strings.HasPrefix(input_buffer.buffer, "select") {
		statement.statement_type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func execute_insert(statement *Statement, table *Table) int {
	if table.num_rows >= uint32(TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL
	}

	row_to_insert := &statement.row_to_insert
	cursor := table_end(table)

	serialize_row(row_to_insert, cursor_value(cursor))
	table.num_rows++

	return EXECUTE_SUCCESS
}

func execute_select(statement *Statement, table *Table) int {
	cursor := table_start(table)
	var row Row

	for !cursor.end_of_table {
		deserialize_row(cursor_value(cursor), &row)
		print_row(&row)
		cursor_advance(cursor)
	}

	return EXECUTE_SUCCESS
}

func execute_statement(statement *Statement, table *Table) int {
	switch statement.statement_type {
	case STATEMENT_INSERT:
		return execute_insert(statement, table)
	case STATEMENT_SELECT:
		return execute_select(statement, table)
	}
	return EXECUTE_SUCCESS
}