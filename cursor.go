package main

import "unsafe"

type Cursor struct {
	Table      *Table
	RowNum     uint32
	EndOfTable bool
}

func tableStart(table *Table) *Cursor {
	cursor := new(Cursor)
	cursor.Table = table
	cursor.RowNum = 0
	cursor.EndOfTable = table.NumRows == 0
	return cursor
}

func tableEnd(table *Table) *Cursor {
	cursor := new(Cursor)
	cursor.Table = table
	cursor.RowNum = table.NumRows
	cursor.EndOfTable = true
	return cursor
}

func cursorValue(cursor *Cursor) unsafe.Pointer {
	rowNum := cursor.RowNum
	pageNum := rowNum / RowsPerPage
	page := getPage(cursor.Table.Pager, pageNum)

	rowOffset := rowNum % RowsPerPage
	byteOffset := rowOffset * RowSize

	return unsafe.Add(page, byteOffset)
}

func cursorAdvance(cursor *Cursor) {
	cursor.RowNum += 1
	if cursor.RowNum >= cursor.Table.NumRows {
		cursor.EndOfTable = true
	}
}
