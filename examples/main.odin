package main

import sqlodin "../sqlodin"
import "core:fmt"
import "core:os"

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

	db_ptr, db_read_err := sqlodin.read(file_name)
	if db_read_err != sqlodin.DATABASE_READ_ERROR.NONE {
		fmt.println("Error reading database:", db_read_err)
		return
	}

	fmt.println("Page size:", db_ptr^.page_size)

	page_number: u32 = 0
	page_data, page_read_err := sqlodin.read_page(db_ptr, page_number)
	if page_read_err != sqlodin.PAGE_READ_ERROR.NONE {
		fmt.println("Error reading page:", page_read_err)
		return
	}

	fmt.println("Page data:", page_data)
}
