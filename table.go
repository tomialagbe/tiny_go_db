package main

import (
	"log"
	"os"
)

type Row struct {
	Id       uint32
	Username [ColumnUsernameSize]byte
	Email    [ColumnEmailSize]byte
}

type Statement struct {
	Type        StatementType
	RowToInsert Row
}

type Table struct {
	NumRows uint32
	Pager   *Pager
}

func openDb(fileName string) *Table {
	pager := pagerOpen(fileName)
	numRows := pager.fileLength / RowSize

	table := new(Table)
	table.Pager = pager
	table.NumRows = numRows
	return table
}

func dbClose(table *Table) {
	pager := table.Pager
	numFullPages := table.NumRows / RowsPerPage

	for i := uint32(0); i < numFullPages; i++ {
		if pager.Pages[i] == nil {
			continue
		}
		pagerFlush(pager, i, PageSize)
		pager.Pages[i] = nil
	}

	// There may be a partial page to write to the end of the file
	// This should not be needed after we switch to a B-tree
	numAdditionalRows := table.NumRows % RowsPerPage
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.Pages[pageNum] != nil {
			pagerFlush(pager, pageNum, numAdditionalRows*RowSize)
			pager.Pages[pageNum] = nil
		}
	}

	err := pager.fileDescriptor.Close()
	if err != nil {
		log.Println("Error closing db file.")
		os.Exit(1)
	}

	for i := uint32(0); i < TableMaxPages; i++ {
		page := pager.Pages[i]
		if page != nil {
			page = nil
			pager.Pages[i] = nil
		}
	}

	pager = nil
	table = nil
}

//func rowSlot(table *Table, rowNum uint32) unsafe.Pointer {
//	pageNum := rowNum / RowsPerPage
//	page := getPage(table.Pager, pageNum)
//
//	rowOffset := rowNum % RowsPerPage
//	byteOffset := rowOffset * RowSize
//
//	return unsafe.Add(page, byteOffset)
//}
