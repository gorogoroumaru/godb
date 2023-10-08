package main

import (
	"fmt"
	"log"
	"syscall"
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

type Pager struct {
	fileDescriptor int
	fileLength     uint32
	num_pages      uint32
	pages          [TABLE_MAX_PAGES]*[]byte
}

type Table struct {
	pager *Pager
	root_page_num uint32
}

func new_table() *Table {
	table := &Table{}
	return table
}

func get_page(pager *Pager, page_num uint32) *[]byte {
	if page_num > TABLE_MAX_PAGES {
		log.Fatalf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES)
	}

	if pager.pages[page_num] == nil {
		page := make([]byte, PAGE_SIZE)
		num_pages := pager.fileLength / PAGE_SIZE

		if pager.fileLength%PAGE_SIZE != 0 {
			num_pages += 1
		}

		if page_num <= num_pages {
			_, err := syscall.Seek(pager.fileDescriptor, int64(page_num*PAGE_SIZE), 0)
			if err != nil {
				log.Fatalf("Error seeking file: %v\n", err)
			}

			_, err = syscall.Read(pager.fileDescriptor, page)
			if err != nil {
				log.Fatalf("Error reading file: %v\n", err)
			}
		}

		pager.pages[page_num] = &page

		if page_num >= pager.num_pages {
			pager.num_pages = page_num + 1
		}
	}

	return pager.pages[page_num]
}

func serialize_row(source *Row, destination []byte) {
	copy(destination[ID_OFFSET:ID_OFFSET+ID_SIZE], (*[ID_SIZE]byte)(unsafe.Pointer(&source.id))[:])
	copy(destination[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE], (*[USERNAME_SIZE]byte)(unsafe.Pointer(&source.username))[:])
	copy(destination[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE], (*[EMAIL_SIZE]byte)(unsafe.Pointer(&source.email))[:])
}

func deserialize_row(source []byte, destination *Row) {
	copy((*[ID_SIZE]byte)(unsafe.Pointer(&destination.id))[:], source[ID_OFFSET:ID_OFFSET+ID_SIZE])
	copy((*[USERNAME_SIZE]byte)(unsafe.Pointer(&destination.username))[:], source[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE])
	copy((*[EMAIL_SIZE]byte)(unsafe.Pointer(&destination.email))[:], source[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE])
}


func row_slot(table *Table, row_num uint32) []byte {
	page_num := row_num / uint32(ROWS_PER_PAGE)
	page := get_page(table.pager, page_num)
	row_offset := row_num % uint32(ROWS_PER_PAGE)
	byte_offset := uintptr(row_offset) * ROW_SIZE
	return (*page)[byte_offset : byte_offset+ROW_SIZE]
}

func pager_open(filename string) *Pager {
	fd, err := syscall.Open(filename, syscall.O_RDWR|syscall.O_CREAT, 0666)
	if err != nil {
		log.Fatalf("Unable to open file: %v\n", err)
	}

	file_length, _ := syscall.Seek(fd, 0, 2)

	pager := &Pager{
		fileDescriptor: fd,
		fileLength:     uint32(file_length),
		num_pages:      uint32(file_length / int64(PAGE_SIZE)),
		pages:          [TABLE_MAX_PAGES]*[]byte{},
	}

	if (file_length % PAGE_SIZE) != 0 {
		fmt.Println("Db file is not a whole number of pages. Corrupt file.")
		syscall.Exit(1)
	}

	return pager
}

func db_open(filename string) *Table {
	pager := pager_open(filename)

	table := &Table{
		pager:   pager,
		root_page_num: 0,
	}

	if pager.num_pages == 0 {
		root_node := get_page(pager, 0)
		initialize_leaf_node(*root_node)
	}

	return table
}

func pager_flush(pager *Pager, page_num uint32, size uint32) {
	if pager.pages[page_num] == nil {
		log.Fatalf("Tried to flush null page\n")
	}

	offset, err := syscall.Seek(pager.fileDescriptor, int64(page_num*PAGE_SIZE), 0)
	if err != nil || offset == -1 {
		log.Fatalf("Error seeking: %v\n", err)
	}

	bytes_written, err := syscall.Write(pager.fileDescriptor, (*pager.pages[page_num])[:PAGE_SIZE])
	if err != nil || bytes_written == -1 {
		log.Fatalf("Error writing: %v\n", err)
	}
}

func db_close(table *Table) {
	pager := table.pager

	for i := uint32(0); i < pager.num_pages; i++ {
		if pager.pages[i] == nil {
			continue
		}

        pager_flush(pager, i, i)
        pager.pages[i] = nil
    }

    result := syscall.Close(pager.fileDescriptor)
    if result != nil {
        log.Fatalf("Error closing db file.\n")
    }
}
