package sqlodin

import "core:fmt"
import "core:mem"
import "core:os"
import "core:strings"
import "core:testing"

@(private = "file")
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

@(private = "file")
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

@(private = "file")
HEADER_PARSE_ERROR :: enum {
	NONE           = 0,
	INVALID_HEADER = 1,
	UNKNOWN_ERROR  = 2,
	INVALID_DATA   = 3,
	ALLOC_ERROR    = 4,
}

@(private)
PAGE_READ_ERROR :: enum {
	NONE,
	FILE_NOT_FOUND,
	READ_ERROR,
	INVALID_PAGE_SIZE,
}

@(private = "file")
be_read_u16 :: #force_inline proc(bytes: []u8, offset: int) -> u16 {
	return (u16(bytes[offset]) << 8) | u16(bytes[offset + 1])
}

@(private = "file")
be_read_u32 :: #force_inline proc(bytes: []u8, offset: int) -> u32 {
	return(
		(u32(bytes[offset]) << 24) |
		(u32(bytes[offset + 1]) << 16) |
		(u32(bytes[offset + 2]) << 8) |
		u32(bytes[offset + 3]) \
	)
}

@(private = "file")
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

@(private = "file")
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

@(private = "file")
header_cleanup :: #force_inline proc(header_ptr: ^sqlite_header) {
	if header_ptr != nil {
		mem.free(header_ptr)
	}
}

@(private = "file")
open_db_file :: #force_inline proc(db_ptr: ^sqlite_db) -> (^os.File, DATABASE_READ_ERROR) {
	file, open_file_err := os.open(db_ptr^.file_path)
	if open_file_err != nil {
		return file, DATABASE_READ_ERROR.FILE_NOT_FOUND
	}
	return file, DATABASE_READ_ERROR.NONE
}

@(private)
read_page :: proc(db_ptr: ^sqlite_db, page_number: u32) -> ([]byte, PAGE_READ_ERROR) {
	page_size := db_ptr^.page_size
	if page_number < 1 {
		return nil, PAGE_READ_ERROR.READ_ERROR
	}
	page_offset := i64(page_number - 1) * i64(page_size)
	page_data := make([]byte, page_size)

	file_handle, open_file_err := open_db_file(db_ptr)
	defer os.close(file_handle)
	if open_file_err != nil {
		return nil, PAGE_READ_ERROR.FILE_NOT_FOUND
	}

	total_read, read_err := os.read_at(file_handle, page_data, page_offset)
	if read_err != nil {
		return nil, PAGE_READ_ERROR.READ_ERROR
	}

	if total_read < len(page_data) {
		return nil, PAGE_READ_ERROR.INVALID_PAGE_SIZE
	}

	return page_data, PAGE_READ_ERROR.NONE
}

@(private = "file")
page_header_offset :: #force_inline proc(page_number: u32) -> int {
	if page_number == 1 {
		return 100
	}
	return 0
}

read :: proc {
	read_file,
}

@(private)
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

@(private = "file")
read_varint :: proc(data: []byte, offset: int) -> (u64, int) {
	val: u64 = 0
	for i := 0; i < 8; i += 1 {
		if offset + i >= len(data) {
			return val, i
		}
		b := data[offset + i]
		val = (val << 7) | u64(b & 0x7F)
		if b < 0x80 {
			return val, i + 1
		}
	}
	if offset + 8 < len(data) {
		val = (val << 8) | u64(data[offset + 8])
		return val, 9
	}
	return val, 9
}

@(private = "file")
serial_type_len :: proc(serial_type: u64) -> u64 {
	switch serial_type {
	case 0, 8, 9:
		return 0
	case 1:
		return 1
	case 2:
		return 2
	case 3:
		return 3
	case 4:
		return 4
	case 5:
		return 6
	case 6, 7:
		return 8
	case:
		if serial_type >= 12 {
			if serial_type % 2 == 0 {
				return (serial_type - 12) / 2
			} else {
				return (serial_type - 13) / 2
			}
		}
	}
	return 0
}

get_table_names :: proc(db: ^sqlite_db) -> ([]string, DATABASE_READ_ERROR) {
	page_data, err := read_page(db, 1)
	if err != PAGE_READ_ERROR.NONE {
		return nil, DATABASE_READ_ERROR.IO_ERROR
	}
	defer delete(page_data)

	header_offset := 100
	page_type := page_data[header_offset]

	// Only handling Leaf Table B-Tree (0x0D) for schema root
	if page_type != 0x0D {
		return nil, DATABASE_READ_ERROR.INVALID_FILE_FORMAT
	}

	num_cells := be_read_u16(page_data, header_offset + 3)
	names := make([dynamic]string)
	cell_ptr_offset := header_offset + 8

	for i := 0; i < int(num_cells); i += 1 {
		ptr := be_read_u16(page_data, cell_ptr_offset + i * 2)
		cell_offset := int(ptr)

		_, ps_len := read_varint(page_data, cell_offset)
		_, rid_len := read_varint(page_data, cell_offset + ps_len)
		content_offset := cell_offset + ps_len + rid_len

		header_len, hl_len := read_varint(page_data, content_offset)
		header_end := content_offset + int(header_len)

		cursor := content_offset + hl_len
		data_cursor := content_offset + int(header_len)

		col_idx := 0
		is_table := false

		for cursor < header_end {
			serial_type, st_len := read_varint(page_data, cursor)
			cursor += st_len
			len_in_bytes := serial_type_len(serial_type)

			if col_idx == 0 { 	// type column
				if len_in_bytes > 0 {
					val := string(page_data[data_cursor:data_cursor + int(len_in_bytes)])
					if val == "table" {
						is_table = true
					}
				}
			} else if col_idx == 1 { 	// name column
				if is_table && len_in_bytes > 0 {
					val := string(page_data[data_cursor:data_cursor + int(len_in_bytes)])
					append(&names, strings.clone(val))
				}
			}

			data_cursor += int(len_in_bytes)
			col_idx += 1
		}
	}

	return names[:], DATABASE_READ_ERROR.NONE
}

SqlNull :: struct {}

SqlValue :: union {
	SqlNull,
	i64,
	f64,
	string,
	[]byte,
}

Row :: map[string]SqlValue

QueryResult :: struct {
	rows:    [dynamic]Row,
	columns: []string,
	err:     DATABASE_READ_ERROR,
}

destroy_query_result :: proc(result: QueryResult) {
	for row in result.rows {
		for _, v in row {
			#partial switch val in v {
			case string:
				delete(val)
			case []byte:
				delete(val)
			}
		}
		delete(row)
	}
	delete(result.rows)
	for col in result.columns {
		delete(col)
	}
	delete(result.columns)
}

row_get_int :: proc(row: Row, col: string) -> (i64, bool) {
	val, ok := row[col]
	if !ok {
		return 0, false
	}
	res, is_int := val.(i64)
	return res, is_int
}

row_get_str :: proc(row: Row, col: string) -> (string, bool) {
	val, ok := row[col]
	if !ok {
		return "", false
	}
	res, is_str := val.(string)
	return res, is_str
}

@(private = "file")
TableSchema :: struct {
	type:      string,
	name:      string,
	tbl_name:  string,
	root_page: int,
	sql:       string,
}

@(private = "file")
get_schemas :: proc(db: ^sqlite_db) -> ([]TableSchema, DATABASE_READ_ERROR) {
	page_data, err := read_page(db, 1)
	if err != PAGE_READ_ERROR.NONE {
		return nil, DATABASE_READ_ERROR.IO_ERROR
	}
	defer delete(page_data)

	header_offset := 100
	page_type := page_data[header_offset]
	if page_type != 0x0D {
		return nil, DATABASE_READ_ERROR.INVALID_FILE_FORMAT
	}

	num_cells := be_read_u16(page_data, header_offset + 3)
	schemas := make([dynamic]TableSchema)
	cell_ptr_offset := header_offset + 8

	for i := 0; i < int(num_cells); i += 1 {
		ptr := be_read_u16(page_data, cell_ptr_offset + i * 2)
		cell_offset := int(ptr)

		// Parse Cell
		_, ps_len := read_varint(page_data, cell_offset)
		_, rid_len := read_varint(page_data, cell_offset + ps_len)
		content_offset := cell_offset + ps_len + rid_len

		header_len, hl_len := read_varint(page_data, content_offset)
		header_end := content_offset + int(header_len)

		cursor := content_offset + hl_len
		data_cursor := content_offset + int(header_len)

		col_idx := 0
		schema := TableSchema{}
		// valid := true

		for cursor < header_end {
			serial_type, st_len := read_varint(page_data, cursor)
			cursor += st_len
			len_in_bytes := serial_type_len(serial_type)

			val: string
			if len_in_bytes > 0 {
				// We only care about string columns for schema parsing (type, name, tbl_name, sql)
				// Rootpage is int (col 3)
				if serial_type >= 12 && serial_type % 2 != 0 {
					val = string(page_data[data_cursor:data_cursor + int(len_in_bytes)])
				}
			}

			switch col_idx {
			case 0:
				// type
				schema.type = strings.clone(val)
			case 1:
				// name
				schema.name = strings.clone(val)
			case 2:
				// tbl_name
				schema.tbl_name = strings.clone(val)
			case 3:
				// rootpage
				// Rootpage is usually integer
				int_val, _ := read_column_int(page_data, data_cursor, serial_type)
				schema.root_page = int(int_val)
			case 4:
				// sql
				schema.sql = strings.clone(val)
			}

			data_cursor += int(len_in_bytes)
			col_idx += 1
		}

		if schema.type == "table" {
			append(&schemas, schema)
		}
	}

	return schemas[:], DATABASE_READ_ERROR.NONE
}

@(private = "file")
read_column_int :: proc(data: []byte, offset: int, serial_type: u64) -> (i64, bool) {
	switch serial_type {
	case 1:
		return i64(i8(data[offset])), true
	case 2:
		return i64(i16(be_read_u16(data, offset))), true
	case 3:
		// 24-bit
		val := (u32(data[offset]) << 16) | (u32(data[offset + 1]) << 8) | u32(data[offset + 2])
		if (val & 0x800000) != 0 {
			val |= 0xFF000000
			return i64(i32(val)), true
		}
		return i64(val), true
	case 4:
		return i64(i32(be_read_u32(data, offset))), true
	case 5:
		return 0, false // 48-bit not impl
	case 6:
		return 0, false // 64-bit need be_read_u64
	case 8:
		return 0, true
	case 9:
		return 1, true
	}
	return 0, false
}

@(private = "file")
ascii_is_space :: #force_inline proc(ch: byte) -> bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

@(private = "file")
ascii_is_alpha :: #force_inline proc(ch: byte) -> bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

@(private = "file")
ascii_is_digit :: #force_inline proc(ch: byte) -> bool {
	return ch >= '0' && ch <= '9'
}

@(private = "file")
ascii_is_ident_char :: #force_inline proc(ch: byte) -> bool {
	return ascii_is_alpha(ch) || ascii_is_digit(ch) || ch == '_'
}

@(private = "file")
ascii_lower :: #force_inline proc(ch: byte) -> byte {
	if ch >= 'A' && ch <= 'Z' {
		return ch + ('a' - 'A')
	}
	return ch
}

@(private = "file")
consume_sql_string :: proc(sql: string, start: int, quote: byte) -> int {
	i := start + 1
	for i < len(sql) {
		if sql[i] == quote {
			if i + 1 < len(sql) && sql[i + 1] == quote {
				i += 2
				continue
			}
			return i
		}
		i += 1
	}
	return len(sql) - 1
}

@(private = "file")
consume_sql_bracket_identifier :: proc(sql: string, start: int) -> int {
	i := start + 1
	for i < len(sql) {
		if sql[i] == ']' {
			return i
		}
		i += 1
	}
	return len(sql) - 1
}

@(private = "file")
extract_definition_list :: proc(sql: string) -> string {
	start := strings.index(sql, "(")
	if start == -1 {
		return ""
	}

	depth := 0
	for i := start; i < len(sql); i += 1 {
		switch sql[i] {
		case '\'', '"', '`':
			i = consume_sql_string(sql, i, sql[i])
		case '[':
			i = consume_sql_bracket_identifier(sql, i)
		case '(':
			depth += 1
		case ')':
			depth -= 1
			if depth == 0 {
				return sql[start + 1:i]
			}
		}
	}

	return ""
}

@(private = "file")
split_top_level_sql_items :: proc(content: string) -> []string {
	items := make([dynamic]string)
	item_start := 0
	depth := 0

	for i := 0; i < len(content); i += 1 {
		switch content[i] {
		case '\'', '"', '`':
			i = consume_sql_string(content, i, content[i])
		case '[':
			i = consume_sql_bracket_identifier(content, i)
		case '(':
			depth += 1
		case ')':
			if depth > 0 {
				depth -= 1
			}
		case ',':
			if depth == 0 {
				item := strings.trim_space(content[item_start:i])
				if len(item) > 0 {
					append(&items, item)
				}
				item_start = i + 1
			}
		}
	}

	if item_start < len(content) {
		item := strings.trim_space(content[item_start:])
		if len(item) > 0 {
			append(&items, item)
		}
	}

	return items[:]
}

@(private = "file")
read_unquoted_sql_token :: proc(definition: string, start: int) -> (string, int) {
	i := start
	for i < len(definition) && ascii_is_ident_char(definition[i]) {
		i += 1
	}
	return definition[start:i], i
}

@(private = "file")
read_quoted_sql_identifier :: proc(definition: string, start: int) -> (string, int, bool) {
	if start >= len(definition) {
		return "", start, false
	}

	switch definition[start] {
	case '"', '`':
		end := consume_sql_string(definition, start, definition[start])
		if end <= start {
			return "", start, false
		}
		return definition[start + 1:end], end + 1, true
	case '[':
		end := consume_sql_bracket_identifier(definition, start)
		if end <= start {
			return "", start, false
		}
		return definition[start + 1:end], end + 1, true
	}

	return "", start, false
}

@(private = "file")
first_sql_keyword_is :: proc(definition: string, keyword: string) -> bool {
	i := 0
	for i < len(definition) && ascii_is_space(definition[i]) {
		i += 1
	}
	if i >= len(definition) {
		return false
	}

	token, token_end := read_unquoted_sql_token(definition, i)
	if len(token) == 0 || len(token) != len(keyword) {
		return false
	}
	for idx := 0; idx < len(keyword); idx += 1 {
		if ascii_lower(token[idx]) != keyword[idx] {
			return false
		}
	}
	return token_end == len(definition) || ascii_is_space(definition[token_end]) || definition[token_end] == '('
}

@(private = "file")
parse_column_name :: proc(definition: string) -> (string, bool) {
	if first_sql_keyword_is(definition, "constraint") ||
		first_sql_keyword_is(definition, "primary") ||
		first_sql_keyword_is(definition, "foreign") ||
		first_sql_keyword_is(definition, "unique") ||
		first_sql_keyword_is(definition, "check") {
		return "", false
	}

	i := 0
	for i < len(definition) && ascii_is_space(definition[i]) {
		i += 1
	}
	if i >= len(definition) {
		return "", false
	}

	if name, next, ok := read_quoted_sql_identifier(definition, i); ok {
		_ = next
		return name, len(name) > 0
	}

	name, _ := read_unquoted_sql_token(definition, i)
	if len(name) == 0 {
		return "", false
	}

	return name, true
}

@(private = "file")
parse_columns :: proc(sql: string) -> []string {
	content := extract_definition_list(sql)
	if len(content) == 0 {
		return nil
	}

	items := split_top_level_sql_items(content)
	defer delete(items)

	cols := make([dynamic]string)
	for item in items {
		if col_name, ok := parse_column_name(item); ok {
			append(&cols, strings.clone(col_name))
		}
	}

	return cols[:]
}

@(private = "file")
collect_table_leaf_pages :: proc(
	db: ^sqlite_db,
	page_number: u32,
	leaf_pages: ^[dynamic]u32,
) -> DATABASE_READ_ERROR {
	page_data, pg_err := read_page(db, page_number)
	if pg_err != PAGE_READ_ERROR.NONE {
		return DATABASE_READ_ERROR.IO_ERROR
	}
	defer delete(page_data)

	header_offset := page_header_offset(page_number)
	page_type := page_data[header_offset]

	switch page_type {
	case 0x0D:
		append(leaf_pages, page_number)
		return DATABASE_READ_ERROR.NONE
	case 0x05:
		num_cells := be_read_u16(page_data, header_offset + 3)
		right_most_page := be_read_u32(page_data, header_offset + 8)
		cell_ptr_offset := header_offset + 12

		for i := 0; i < int(num_cells); i += 1 {
			ptr := be_read_u16(page_data, cell_ptr_offset + i * 2)
			cell_offset := int(ptr)
			child_page := be_read_u32(page_data, cell_offset)

			if err := collect_table_leaf_pages(db, child_page, leaf_pages); err != DATABASE_READ_ERROR.NONE {
				return err
			}
		}

		if err := collect_table_leaf_pages(db, right_most_page, leaf_pages); err != DATABASE_READ_ERROR.NONE {
			return err
		}
		return DATABASE_READ_ERROR.NONE
	}

	return DATABASE_READ_ERROR.INVALID_FILE_FORMAT
}

@(private = "file")
parse_leaf_table_page_rows :: proc(
	page_data: []byte,
	page_number: u32,
	columns: []string,
) -> ([dynamic]Row, DATABASE_READ_ERROR) {
	header_offset := page_header_offset(page_number)
	page_type := page_data[header_offset]
	if page_type != 0x0D {
		return nil, DATABASE_READ_ERROR.INVALID_FILE_FORMAT
	}

	num_cells := be_read_u16(page_data, header_offset + 3)
	rows := make([dynamic]Row)
	cell_ptr_offset := header_offset + 8

	for i := 0; i < int(num_cells); i += 1 {
		ptr := be_read_u16(page_data, cell_ptr_offset + i * 2)
		cell_offset := int(ptr)

		_, ps_len := read_varint(page_data, cell_offset)
		row_id, rid_len := read_varint(page_data, cell_offset + ps_len)
		content_offset := cell_offset + ps_len + rid_len

		header_len, hl_len := read_varint(page_data, content_offset)
		header_end := content_offset + int(header_len)

		cursor := content_offset + hl_len
		data_cursor := content_offset + int(header_len)

		col_idx := 0
		row := make(Row)
		row["_rowid_"] = i64(row_id)

		for cursor < header_end {
			serial_type, st_len := read_varint(page_data, cursor)
			cursor += st_len
			len_in_bytes := serial_type_len(serial_type)

			col_name := ""
			if col_idx < len(columns) {
				col_name = columns[col_idx]
			} else {
				col_name = fmt.tprintf("col_%d", col_idx)
			}

			if serial_type == 0 {
				row[col_name] = SqlNull{}
			} else if serial_type >= 1 && serial_type <= 6 {
				int_val, ok := read_column_int(page_data, data_cursor, serial_type)
				if ok {
					row[col_name] = int_val
				}
			} else if serial_type == 7 {
				// TODO
			} else if serial_type == 8 {
				row[col_name] = i64(0)
			} else if serial_type == 9 {
				row[col_name] = i64(1)
			} else if serial_type >= 12 {
				is_text := (serial_type % 2) != 0
				if is_text {
					val := string(page_data[data_cursor:data_cursor + int(len_in_bytes)])
					row[col_name] = strings.clone(val)
				} else {
					// Blob
				}
			}

			data_cursor += int(len_in_bytes)
			col_idx += 1
		}

		append(&rows, row)
	}

	return rows, DATABASE_READ_ERROR.NONE
}

query_table :: proc(db: ^sqlite_db, table_name: string) -> QueryResult {
	schemas, err := get_schemas(db)
	if err != DATABASE_READ_ERROR.NONE {
		return QueryResult{err = err}
	}
	defer {
		for s in schemas {
			delete(s.type)
			delete(s.name)
			delete(s.tbl_name)
			delete(s.sql)
		}
		delete(schemas)
	}

	target_schema: TableSchema
	found := false
	for s in schemas {
		if s.name == table_name {
			target_schema = s
			found = true
			break
		}
	}

	if !found {
		return QueryResult{err = DATABASE_READ_ERROR.NONE} // Not found
	}

	// Parse columns
	columns := parse_columns(target_schema.sql)

	leaf_pages := make([dynamic]u32)
	defer delete(leaf_pages)

	if err := collect_table_leaf_pages(db, u32(target_schema.root_page), &leaf_pages); err != DATABASE_READ_ERROR.NONE {
		return QueryResult{columns = columns, err = err}
	}

	rows := make([dynamic]Row)
	for page_number in leaf_pages {
		page_data, pg_err := read_page(db, page_number)
		if pg_err != PAGE_READ_ERROR.NONE {
			for row in rows {
				delete(row)
			}
			delete(rows)
			return QueryResult{columns = columns, err = DATABASE_READ_ERROR.IO_ERROR}
		}
		page_rows, err := parse_leaf_table_page_rows(page_data, page_number, columns)
		delete(page_data)
		if err != DATABASE_READ_ERROR.NONE {
			for row in rows {
				delete(row)
			}
			delete(rows)
			return QueryResult{columns = columns, err = err}
		}
		for row in page_rows {
			append(&rows, row)
		}
		delete(page_rows)
	}

	return QueryResult{rows = rows, columns = columns, err = DATABASE_READ_ERROR.NONE}
}

@(private = "file")
put_be_u16 :: #force_inline proc(data: []u8, offset: int, value: u16) {
	data[offset] = u8(value >> 8)
	data[offset + 1] = u8(value)
}

@(private = "file")
put_be_u32 :: #force_inline proc(data: []u8, offset: int, value: u32) {
	data[offset] = u8(value >> 24)
	data[offset + 1] = u8(value >> 16)
	data[offset + 2] = u8(value >> 8)
	data[offset + 3] = u8(value)
}

@(private = "file")
append_small_varint :: #force_inline proc(buf: ^[dynamic]u8, value: u64) {
	append(buf, u8(value))
}

@(private = "file")
append_string_bytes :: proc(buf: ^[dynamic]u8, value: string) {
	for b in transmute([]u8)value {
		append(buf, b)
	}
}

@(private = "file")
write_record_leaf_cell :: proc(
	page: []u8,
	page_number: u32,
	row_id: u64,
	record: []u8,
	cell_index: int,
) {
	header_offset := page_header_offset(page_number)
	cell_len := 2 + len(record)
	cell_offset := len(page) - cell_len

	append_offset := cell_offset
	page[append_offset] = u8(len(record))
	append_offset += 1
	page[append_offset] = u8(row_id)
	append_offset += 1
	mem.copy(raw_data(page[append_offset:]), raw_data(record), len(record))

	put_be_u16(page, header_offset + 3, 1)
	put_be_u16(page, header_offset + 5, u16(cell_offset))
	put_be_u16(page, header_offset + 8 + cell_index * 2, u16(cell_offset))
}

@(private = "file")
build_table_leaf_record :: proc(id: i64, name: string) -> [dynamic]u8 {
	record := make([dynamic]u8)

	if id == 1 {
		append_small_varint(&record, 3)
		append_small_varint(&record, 9)
	} else {
		append_small_varint(&record, 3)
		append_small_varint(&record, 1)
	}

	append_small_varint(&record, 13 + 2 * u64(len(name)))

	if id != 1 {
		append(&record, u8(id))
	}
	append_string_bytes(&record, name)

	return record
}

@(private = "file")
build_schema_record :: proc(root_page: u32, sql: string) -> [dynamic]u8 {
	record := make([dynamic]u8)

	append_small_varint(&record, 6)
	append_small_varint(&record, 23)
	append_small_varint(&record, 23)
	append_small_varint(&record, 23)
	append_small_varint(&record, 1)
	append_small_varint(&record, 13 + 2 * u64(len(sql)))

	append_string_bytes(&record, "table")
	append_string_bytes(&record, "users")
	append_string_bytes(&record, "users")
	append(&record, u8(root_page))
	append_string_bytes(&record, sql)

	return record
}

@(test)
test_be_read_u16 :: proc(t: ^testing.T) {
	data := []u8{0x01, 0x02}
	val := be_read_u16(data, 0)
	testing.expect(t, val == 0x0102, "Expected 0x0102")
}

@(test)
test_be_read_u32 :: proc(t: ^testing.T) {
	data := []u8{0x01, 0x02, 0x03, 0x04}
	val := be_read_u32(data, 0)
	testing.expect(t, val == 0x01020304, "Expected 0x01020304")
}

@(test)
test_derive_page_size :: proc(t: ^testing.T) {
	size, valid := derive_page_size(1)
	testing.expect(t, valid == true, "Expected valid page size")
	testing.expect(t, size == 65536, "Expected 65536")

	size, valid = derive_page_size(512)
	testing.expect(t, valid == true, "Expected valid page size")
	testing.expect(t, size == 512, "Expected 512")

	size, valid = derive_page_size(1024)
	testing.expect(t, valid == true, "Expected valid page size")
	testing.expect(t, size == 1024, "Expected 1024")

	size, valid = derive_page_size(300)
	testing.expect(t, valid == false, "Expected invalid page size")

	size, valid = derive_page_size(513)
	testing.expect(t, valid == false, "Expected invalid page size")
}

@(test)
test_read_varint :: proc(t: ^testing.T) {
	data1 := []u8{0x7F}
	val, n := read_varint(data1, 0)
	testing.expect(t, val == 127, "Expected 127")
	testing.expect(t, n == 1, "Expected 1 byte read")

	data2 := []u8{0x81, 0x00}
	val, n = read_varint(data2, 0)
	testing.expect(t, val == 128, "Expected 128")
	testing.expect(t, n == 2, "Expected 2 bytes read")
}

@(test)
test_parse_columns_handles_nested_expressions_and_constraints :: proc(t: ^testing.T) {
	sql := `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		email TEXT NOT NULL,
		name TEXT DEFAULT (trim('last, first')),
		slug TEXT GENERATED ALWAYS AS (lower(name || ',' || email)) STORED,
		score INTEGER CHECK (score IN (1, 2, 3)),
		CONSTRAINT users_email_unique UNIQUE (email),
		PRIMARY KEY (id)
	)`

	columns := parse_columns(sql)
	defer {
		for column in columns {
			delete(column)
		}
		delete(columns)
	}

	testing.expect(t, len(columns) == 5, "Expected only table columns to be returned")
	testing.expect(t, columns[0] == "id", "Expected first column to be id")
	testing.expect(t, columns[1] == "email", "Expected second column to be email")
	testing.expect(t, columns[2] == "name", "Expected third column to be name")
	testing.expect(t, columns[3] == "slug", "Expected fourth column to be slug")
	testing.expect(t, columns[4] == "score", "Expected fifth column to be score")
}

@(test)
test_parse_columns_handles_quoted_identifiers :: proc(t: ^testing.T) {
	sql := "CREATE TABLE weird ([select] TEXT, \"two words\" INTEGER DEFAULT (1), `tick-name` BLOB)"

	columns := parse_columns(sql)
	defer {
		for column in columns {
			delete(column)
		}
		delete(columns)
	}

	testing.expect(t, len(columns) == 3, "Expected three quoted column names")
	testing.expect(t, columns[0] == "select", "Expected bracket-quoted identifier")
	testing.expect(t, columns[1] == "two words", "Expected double-quoted identifier")
	testing.expect(t, columns[2] == "tick-name", "Expected backtick-quoted identifier")
}

@(test)
test_serial_type_len :: proc(t: ^testing.T) {
	testing.expect(t, serial_type_len(0) == 0, "Type 0 length should be 0")
	testing.expect(t, serial_type_len(1) == 1, "Type 1 length should be 1")
	testing.expect(t, serial_type_len(2) == 2, "Type 2 length should be 2")
	testing.expect(t, serial_type_len(3) == 3, "Type 3 length should be 3")
	testing.expect(t, serial_type_len(4) == 4, "Type 4 length should be 4")
	testing.expect(t, serial_type_len(5) == 6, "Type 5 length should be 6")
	testing.expect(t, serial_type_len(6) == 8, "Type 6 length should be 8")
	testing.expect(t, serial_type_len(7) == 8, "Type 7 length should be 8")
	testing.expect(t, serial_type_len(8) == 0, "Type 8 length should be 0")
	testing.expect(t, serial_type_len(9) == 0, "Type 9 length should be 0")
	testing.expect(t, serial_type_len(12) == 0, "Type 12 length should be 0")
	testing.expect(t, serial_type_len(13) == 0, "Type 13 length should be 0")
	testing.expect(t, serial_type_len(14) == 1, "Type 14 length should be 1")
}

@(test)
test_header_parse :: proc(t: ^testing.T) {
	header_data: [100]u8
	for i := 0; i < 16; i += 1 {
		header_data[i] = SQLITE_MAGIC[i]
	}
	// Set page size to 4096 (0x1000) at offset 16
	header_data[16] = 0x10
	header_data[17] = 0x00

	header_ptr, err := header_parse(&header_data)
	defer header_cleanup(header_ptr)

	testing.expect(t, err == HEADER_PARSE_ERROR.NONE, "Expected no error parsing header")
	testing.expect(t, header_ptr != nil, "Expected header pointer not to be nil")
	if header_ptr != nil {
		testing.expect(t, header_ptr.page_size == 4096, "Expected page size 4096")
	}

	// Test invalid magic
	invalid_header: [100]u8
	header_ptr_invalid, err_invalid := header_parse(&invalid_header)
	testing.expect(
		t,
		err_invalid == HEADER_PARSE_ERROR.INVALID_HEADER,
		"Expected invalid header error",
	)
	testing.expect(t, header_ptr_invalid == nil, "Expected nil header pointer")
}

@(test)
test_query_table_handles_interior_root_pages :: proc(t: ^testing.T) {
	db_path := fmt.tprintf("/tmp/sqlodin_interior_root_%d.sqlite", os.get_pid())
	defer {
		os.remove(db_path)
	}

	file, create_err := os.create(db_path)
	testing.expect(t, create_err == nil, "Expected test database file to be created")
	if create_err != nil || file == nil {
		return
	}
	defer os.close(file)

	page_size := 512
	page1 := make([]u8, page_size)
	page2 := make([]u8, page_size)
	page3 := make([]u8, page_size)
	page4 := make([]u8, page_size)
	defer delete(page1)
	defer delete(page2)
	defer delete(page3)
	defer delete(page4)

	mem.copy(raw_data(page1[:len(SQLITE_MAGIC)]), raw_data(SQLITE_MAGIC[:]), len(SQLITE_MAGIC))
	put_be_u16(page1, 16, u16(page_size))
	put_be_u32(page1, 28, 4)

	page1[100] = 0x0D
	schema_sql := "CREATE TABLE users (id INT, name TEXT)"
	schema_record := build_schema_record(2, schema_sql)
	write_record_leaf_cell(page1, 1, 1, schema_record[:], 0)
	delete(schema_record)

	page2[0] = 0x05
	put_be_u16(page2, 3, 1)
	put_be_u16(page2, 5, 507)
	put_be_u32(page2, 8, 4)
	put_be_u16(page2, 12, 507)
	put_be_u32(page2, 507, 3)
	page2[511] = 1

	page3[0] = 0x0D
	row1_record := build_table_leaf_record(1, "Alice")
	write_record_leaf_cell(page3, 3, 1, row1_record[:], 0)
	delete(row1_record)

	page4[0] = 0x0D
	row2_record := build_table_leaf_record(2, "Bob")
	write_record_leaf_cell(page4, 4, 2, row2_record[:], 0)
	delete(row2_record)

	total_written, write_err := os.write(file, page1)
	testing.expect(t, write_err == nil && total_written == len(page1), "Expected page 1 to be written")
	total_written, write_err = os.write(file, page2)
	testing.expect(t, write_err == nil && total_written == len(page2), "Expected page 2 to be written")
	total_written, write_err = os.write(file, page3)
	testing.expect(t, write_err == nil && total_written == len(page3), "Expected page 3 to be written")
	total_written, write_err = os.write(file, page4)
	testing.expect(t, write_err == nil && total_written == len(page4), "Expected page 4 to be written")

	db_ptr, db_err := read_file(db_path)
	testing.expect(t, db_err == DATABASE_READ_ERROR.NONE, "Expected database header to be readable")
	if db_ptr == nil {
		return
	}
	defer mem.free(db_ptr)

	result := query_table(db_ptr, "users")
	defer destroy_query_result(result)

	testing.expect(t, result.err == DATABASE_READ_ERROR.NONE, "Expected query to succeed")
	testing.expect(t, len(result.rows) == 2, "Expected two rows from interior-root table")

	row1_id, ok1 := row_get_int(result.rows[0], "id")
	testing.expect(t, ok1 && row1_id == 1, "Expected first row id to be 1")
	row1_name, ok2 := row_get_str(result.rows[0], "name")
	testing.expect(t, ok2 && row1_name == "Alice", "Expected first row name to be Alice")

	row2_id, ok3 := row_get_int(result.rows[1], "id")
	testing.expect(t, ok3 && row2_id == 2, "Expected second row id to be 2")
	row2_name, ok4 := row_get_str(result.rows[1], "name")
	testing.expect(t, ok4 && row2_name == "Bob", "Expected second row name to be Bob")
}
