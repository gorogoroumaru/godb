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
	EXECUTE_DUPLICATE_KEY
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
	node := get_page(table.pager, table.root_page_num)
	num_cells := leaf_node_num_cells(*node)

	row_to_insert := &statement.row_to_insert
	key_to_insert := row_to_insert.id
	cursor := table_find(table, key_to_insert)

	if (cursor.cell_num < num_cells) {
		key_at_index := leaf_node_key(*node, cursor.cell_num)
		if (key_at_index == key_to_insert) {
			return EXECUTE_DUPLICATE_KEY;
		}
	}
	leaf_node_insert(cursor, row_to_insert.id, row_to_insert)

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