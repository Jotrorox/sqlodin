package sqlodin

import "core:fmt"
import "core:mem"
import "core:os"

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

header_parse :: proc(data: ^[100]u8) -> ^sqlite_header {
	header_ptr := new(sqlite_header, context.allocator)
	mem.copy(header_ptr, data, size_of(sqlite_header))
	return header_ptr
}

header_cleanup :: #force_inline proc(header_ptr: ^sqlite_header) {
	if header_ptr != nil {
		mem.free(header_ptr)
	}
}

main :: proc() {
	if len(os.args) < 2 {
		fmt.println("Usage: sqlodin <database_file>")
		return
	}
	if len(os.args) > 2 {
		fmt.println("Error: Too many arguments")
		return
	}

	file_name := os.args[1]

	file_handle, file_open_err := os.open(file_name)
	if file_open_err != nil {
		fmt.println("Error opening file:", file_open_err)
		return
	}
	defer os.close(file_handle)

	header: [100]u8
	total_read, read_err := os.read(file_handle, header[:])
	if read_err != nil {
		fmt.println("Error reading file:", read_err)
		return
	}

	if total_read < len(header) {
		fmt.println("Error: Incomplete header read")
		return
	}

	header_ptr := header_parse(&header)
	defer header_cleanup(header_ptr)

	fmt.println("Total bytes read:", total_read)
	fmt.println("File header data:", header_ptr^.header_string)
	page_size_be := (u16(header[16]) << 8) | u16(header[17])
	fmt.println("Page size:", page_size_be)
}
