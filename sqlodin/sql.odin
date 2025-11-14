package sqlodin

import "core:fmt"
import "core:mem"
import "core:os"

SQLITE_MAGIC: [16]u8 = {
	0x53,
	0x51,
	0x4C,
	0x69,
	0x74,
	0x65,
	0x20,
	0x66,
	0x6F,
	0x72,
	0x6D,
	0x61,
	0x74,
	0x20,
	0x33,
	0x00,
}

sqlite_header :: struct {
	header_string:                 [16]u8,
	page_size:                     u16,
	file_format_write_version:     u8,
	file_format_read_version:      u8,
	reserved_space:                u8,
	max_embedded_payload_fraction: u8,
	min_embedded_payload_fraction: u8,
	leaf_payload_fraction:         u8,
	file_change_counter:           u32,
	database_size_in_pages:        u32,
	first_freelist_trunk_page:     u32,
	total_freelist_pages:          u32,
	schema_cookie:                 u32,
	schema_format_number:          u32,
	default_page_cache_size:       u32,
	largest_root_btree_page:       u32,
	database_text_encoding:        u32,
	user_version:                  u32,
	incremental_vacuum_mode:       u32,
	application_id:                u32,
	reserved_for_expansion:        [20]u8,
	version_valid_for_number:      u32,
	sqlite_version_number:         u32,
}

sqlite_db :: struct {
	file_path: string,
	page_size: u32,
}

DATABASE_READ_ERROR :: enum {
	NONE                = 0,
	FILE_NOT_FOUND      = 1,
	INVALID_FILE_FORMAT = 2,
	IO_ERROR            = 3,
}

HEADER_PARSE_ERROR :: enum {
	NONE           = 0,
	INVALID_HEADER = 1,
	UNKNOWN_ERROR  = 2,
	INVALID_DATA   = 3,
	ALLOC_ERROR    = 4,
}

be_read_u16 :: #force_inline proc(bytes: []u8, offset: int) -> u16 {
	return (u16(bytes[offset]) << 8) | u16(bytes[offset + 1])
}

be_read_u32 :: #force_inline proc(bytes: []u8, offset: int) -> u32 {
	return(
		(u32(bytes[offset]) << 24) |
		(u32(bytes[offset + 1]) << 16) |
		(u32(bytes[offset + 2]) << 8) |
		u32(bytes[offset + 3]) \
	)
}

derive_page_size :: #force_inline proc(raw: u16) -> (u32, bool) {
	if raw == 0 {
		return 0, false
	}
	if raw == 1 {
		return 65536, true
	}
	if raw < 512 || raw > 32768 {
		return 0, false
	}
	if (raw & (raw - 1)) != 0 {
		return 0, false
	}
	return u32(raw), true
}

header_parse :: proc(data: ^[100]u8) -> (^sqlite_header, HEADER_PARSE_ERROR) {
	if data == nil {
		return nil, HEADER_PARSE_ERROR.INVALID_HEADER
	}
	bytes := data^
	for idx := 0; idx < len(SQLITE_MAGIC); idx += 1 {
		if bytes[idx] != SQLITE_MAGIC[idx] {
			return nil, HEADER_PARSE_ERROR.INVALID_HEADER
		}
	}
	header_ptr := new(sqlite_header, context.allocator)
	if header_ptr == nil {
		return nil, HEADER_PARSE_ERROR.ALLOC_ERROR
	}
	for idx := 0; idx < len(header_ptr.header_string); idx += 1 {
		header_ptr.header_string[idx] = bytes[idx]
	}

	byte_slice := bytes[:]
	offset := len(SQLITE_MAGIC)

	page_size_raw := be_read_u16(byte_slice, offset)
	_, valid_page_size := derive_page_size(page_size_raw)
	if !valid_page_size {
		mem.free(header_ptr)
		return nil, HEADER_PARSE_ERROR.INVALID_DATA
	}
	header_ptr.page_size = page_size_raw
	offset += 2

	header_ptr.file_format_write_version = byte_slice[offset]
	offset += 1
	header_ptr.file_format_read_version = byte_slice[offset]
	offset += 1
	header_ptr.reserved_space = byte_slice[offset]
	offset += 1
	header_ptr.max_embedded_payload_fraction = byte_slice[offset]
	offset += 1
	header_ptr.min_embedded_payload_fraction = byte_slice[offset]
	offset += 1
	header_ptr.leaf_payload_fraction = byte_slice[offset]
	offset += 1

	header_ptr.file_change_counter = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.database_size_in_pages = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.first_freelist_trunk_page = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.total_freelist_pages = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.schema_cookie = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.schema_format_number = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.default_page_cache_size = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.largest_root_btree_page = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.database_text_encoding = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.user_version = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.incremental_vacuum_mode = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.application_id = be_read_u32(byte_slice, offset)
	offset += 4

	for idx := 0; idx < len(header_ptr.reserved_for_expansion); idx += 1 {
		header_ptr.reserved_for_expansion[idx] = byte_slice[offset + idx]
	}
	offset += len(header_ptr.reserved_for_expansion)

	header_ptr.version_valid_for_number = be_read_u32(byte_slice, offset)
	offset += 4
	header_ptr.sqlite_version_number = be_read_u32(byte_slice, offset)

	return header_ptr, HEADER_PARSE_ERROR.NONE
}

header_cleanup :: #force_inline proc(header_ptr: ^sqlite_header) {
	if header_ptr != nil {
		mem.free(header_ptr)
	}
}

open_db_file :: #force_inline proc(db_ptr: ^sqlite_db) -> (os.Handle, DATABASE_READ_ERROR) {
	file, open_file_err := os.open(db_ptr^.file_path)
	if open_file_err != nil {
		return file, DATABASE_READ_ERROR.FILE_NOT_FOUND
	}
	return file, DATABASE_READ_ERROR.NONE
}

PAGE_READ_ERROR :: enum {
	NONE,
	FILE_NOT_FOUND,
	READ_ERROR,
	INVALID_PAGE_SIZE,
}

read_page :: proc(db_ptr: ^sqlite_db, page_number: u32) -> ([]byte, PAGE_READ_ERROR) {
	page_size := db_ptr^.page_size
	page_offset := page_number * page_size + 100 // Offset because of header
	page_data := make([]byte, page_size)

	file_handle, open_file_err := open_db_file(db_ptr)
	defer os.close(file_handle)
	if open_file_err != nil {
		return nil, PAGE_READ_ERROR.FILE_NOT_FOUND
	}

	total_read, read_err := os.read_at(file_handle, page_data, i64(page_offset))
	if read_err != nil {
		return nil, PAGE_READ_ERROR.READ_ERROR
	}

	if total_read < len(page_data) {
		return nil, PAGE_READ_ERROR.INVALID_PAGE_SIZE
	}

	return page_data, PAGE_READ_ERROR.NONE
}

read :: proc {
	read_file,
}

read_file :: proc(file_path: string) -> (^sqlite_db, DATABASE_READ_ERROR) {
	db_ptr := new(sqlite_db, context.allocator)
	db_ptr.file_path = file_path

	file_handle, open_file_err := os.open(file_path)
	defer os.close(file_handle)
	if open_file_err != nil {
		return nil, DATABASE_READ_ERROR.FILE_NOT_FOUND
	}

	header: [100]u8
	total_read, read_err := os.read(file_handle, header[:])
	if read_err != nil {
		fmt.println("Error reading file:", read_err)
		return nil, DATABASE_READ_ERROR.IO_ERROR
	}

	if total_read < len(header) {
		fmt.println("Error: Incomplete header read")
		return nil, DATABASE_READ_ERROR.IO_ERROR
	}

	header_ptr, header_parse_err := header_parse(&header)
	defer header_cleanup(header_ptr)
	if header_parse_err != HEADER_PARSE_ERROR.NONE {
		return nil, DATABASE_READ_ERROR.INVALID_FILE_FORMAT
	}

	resolved_page_size, valid_page_size := derive_page_size(header_ptr^.page_size)
	if !valid_page_size {
		return nil, DATABASE_READ_ERROR.INVALID_FILE_FORMAT
	}

	db_ptr.page_size = resolved_page_size
	return db_ptr, DATABASE_READ_ERROR.NONE
}
