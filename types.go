package main

import "unsafe"

type MetaCommandResult int
type PrepareResult int
type StatementType int
type ExecuteResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandUnrecognizedCommand
)

const (
	PrepareSuccess PrepareResult = iota
	PrepareStringTooLong
	PrepareNegativeId
	PrepareSyntaxError
	PrepareUnrecognizedStatement
)

const (
	StatementInsert StatementType = iota
	StatementSelect
)

const (
	ExecuteSuccess ExecuteResult = iota
	ExecuteTableFull
	ExecuteError
)

const ColumnUsernameSize = 32
const ColumnEmailSize = 255

var tmpRow = Row{}

const IdSize = unsafe.Sizeof(tmpRow.Id)
const UsernameSize = unsafe.Sizeof(tmpRow.Username)
const EmailSize = unsafe.Sizeof(tmpRow.Email)

const IdOffset uint32 = 0
const UsernameOffset = IdOffset + uint32(IdSize)
const EmailOffset = UsernameOffset + uint32(UsernameSize)
const RowSize = uint32(IdSize + UsernameSize + EmailSize)

const PageSize uint32 = 4096
const TableMaxPages uint32 = 100
const RowsPerPage = PageSize / RowSize
const TableMaxRows = RowsPerPage * TableMaxPages
