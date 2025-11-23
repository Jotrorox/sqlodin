package main

import sqlodin "../../sqlodin"
import "core:fmt"
import "core:os"

main :: proc() {
	if len(os.args) != 2 {
		fmt.println("Usage: sqlodin <database_file>")
		return
	}

	file_name := os.args[1]

	db_ptr, db_read_err := sqlodin.read(file_name)
	if db_read_err != nil {
		fmt.println("Error reading database:", db_read_err)
		return
	}

	tables, tables_err := sqlodin.get_table_names(db_ptr)
	if tables_err != nil {
		fmt.println("Error reading tables:", tables_err)
		return
	}
	defer delete(tables)

	fmt.println("Tables:", tables)
}
