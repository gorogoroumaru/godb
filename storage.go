package main

import (
	"fmt"
	"unsafe"
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE = 255
)

const (
	ID_SIZE         = unsafe.Sizeof(Row{}.id)
	USERNAME_SIZE   = unsafe.Sizeof(Row{}.username)
	EMAIL_SIZE      = unsafe.Sizeof(Row{}.email)
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

const (
	PAGE_SIZE       = 4096
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Row struct {
	id uint32
	username [COLUMN_USERNAME_SIZE + 1]byte
	email [COLUMN_EMAIL_SIZE + 1]byte
}

func print_row(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.id, row.username, row.email)
}

type Table struct {
	num_rows uint32
	pages    [TABLE_MAX_PAGES]*[ROWS_PER_PAGE]Row
}

func serialize_row(source *Row, destination unsafe.Pointer) {
	dest := uintptr(destination)
	copy((*[ID_SIZE]byte)(unsafe.Pointer(dest + ID_OFFSET))[:], (*[ID_SIZE]byte)(unsafe.Pointer(&source.id))[:])
	copy((*[USERNAME_SIZE]byte)(unsafe.Pointer(dest + USERNAME_OFFSET))[:], (*[USERNAME_SIZE]byte)(unsafe.Pointer(&source.username))[:])
	copy((*[EMAIL_SIZE]byte)(unsafe.Pointer(dest + EMAIL_OFFSET))[:], (*[EMAIL_SIZE]byte)(unsafe.Pointer(&source.email))[:])
}

func deserialize_row(source unsafe.Pointer, destination *Row) {
	src := uintptr(source)
	copy((*[ID_SIZE]byte)(unsafe.Pointer(&destination.id))[:], (*[ID_SIZE]byte)(unsafe.Pointer(src + ID_OFFSET))[:])
	copy((*[USERNAME_SIZE]byte)(unsafe.Pointer(&destination.username))[:], (*[USERNAME_SIZE]byte)(unsafe.Pointer(src + USERNAME_OFFSET))[:])
	copy((*[EMAIL_SIZE]byte)(unsafe.Pointer(&destination.email))[:], (*[EMAIL_SIZE]byte)(unsafe.Pointer(src + EMAIL_OFFSET))[:])
}

func row_slot(table *Table, row_num uint32) unsafe.Pointer {
	page_num := row_num / uint32(ROWS_PER_PAGE)
	page := table.pages[page_num]
	if page == nil {
		page := new([ROWS_PER_PAGE]Row)
		table.pages[page_num] = page
	}
	row_offset := row_num % uint32(ROWS_PER_PAGE)
	byte_offset := row_offset * uint32(ROW_SIZE)
	return unsafe.Pointer(uintptr(unsafe.Pointer(table.pages[page_num])) + uintptr(byte_offset))
}

func new_table() *Table {
	table := &Table{}
	table.num_rows = 0
	return table
}

func free_table(table *Table) {
	for _, page := range table.pages {
		if page != nil {
			page = nil
		}
	}
}
