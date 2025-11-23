package main

import sqlodin "../../sqlodin"
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

	tables, tables_err := sqlodin.get_table_names(db_ptr)
	if tables_err != sqlodin.DATABASE_READ_ERROR.NONE {
		fmt.println("Error reading tables:", tables_err)
		return
	}
	defer delete(tables)

	fmt.println("Tables:", tables)
	for table in tables {
		fmt.println("Data for table:", table)
		result := sqlodin.query_table(db_ptr, table)
		defer sqlodin.destroy_query_result(result)

		if result.err != sqlodin.DATABASE_READ_ERROR.NONE {
			fmt.println("Error querying table:", result.err)
			delete(table)
			continue
		}

		fmt.println("    Columns:", result.columns)
		for row in result.rows {
			fmt.println("   ", row)
		}

		delete(table)
	}
}
