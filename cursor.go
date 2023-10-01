package main

type Cursor struct {
	table       *Table
	row_num      uint32
	end_of_table  bool // Indicates a position one past the last element
}

func table_start(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.table = table
	cursor.row_num = 0
	cursor.end_of_table = (table.num_rows == 0)

	return cursor
}

func table_end(table *Table) *Cursor {
	cursor := &Cursor{}
	cursor.table = table
	cursor.row_num = table.num_rows
	cursor.end_of_table = true

	return cursor
}

func cursor_value(cursor *Cursor) []byte {
	row_num := cursor.row_num
	page_num := row_num / uint32(ROWS_PER_PAGE)
	page := get_page(cursor.table.pager, page_num)
	row_offset := row_num % uint32(ROWS_PER_PAGE)
	byte_offset := row_offset * uint32(ROW_SIZE)

	return (*page)[byte_offset:]
}

func cursor_advance(cursor *Cursor) {
	cursor.row_num += 1
	if cursor.row_num >= cursor.table.num_rows {
		cursor.end_of_table = true
	}
}
