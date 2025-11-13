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
}
