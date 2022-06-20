package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

func main() {
	//log.Printf("Type Sizes: ID: %v, Username: %v, Email: %v\n", IdSize, UsernameSize, EmailSize)
	//
	//log.Println("TEST SERDE")
	//
	//emailSlice := make([]byte, ColumnEmailSize)
	//copy(emailSlice, "tomialagbe@yahoo.com")
	//email := *(*[ColumnEmailSize]byte)(emailSlice)
	//
	//usernameSlice := make([]byte, ColumnUsernameSize)
	//copy(usernameSlice, "12345678901234567890123456789012")
	//username := *(*[ColumnUsernameSize]byte)(usernameSlice)
	//
	//row := Row{Id: 1, Email: email, Username: username}
	//serialized := [RowSize]byte{}
	//serializeRow(&row, serialized[:])
	//var deserialized Row
	//deserializeRow(serialized[:], &deserialized)
	//log.Printf("Deserialized: ID: %d, email: %s, username: %s", deserialized.Id, deserialized.Email, deserialized.Username)

	args := os.Args
	if len(args) < 2 {
		log.Println("Must supply a database filename.")
		os.Exit(1)
	}

	fileName := os.Args[1]
	table := openDb(fileName)

	for {
		printPrompt()
		input := readInput()

		if input[0] == '.' {
			switch doMetaCommand(input, table) {
			case MetaCommandSuccess:
				continue
			case MetaCommandUnrecognizedCommand:
				log.Printf("Unrecognized command '%s'. \n", input)
				continue
			}
		}

		statement := new(Statement)
		switch prepareStatement(input, statement) {
		case PrepareSuccess:
			break
		case PrepareSyntaxError:
			log.Println("Syntax error. Could not parse statement.")
			continue
		case PrepareStringTooLong:
			log.Println("String is too long.")
			continue
		case PrepareNegativeId:
			log.Println("ID must be positive.")
			continue
		case PrepareUnrecognizedStatement:
			log.Printf("Unrecognized keyword at start of '%s'.\n", input)
			continue
		}

		switch executeStatement(statement, table) {
		case ExecuteSuccess:
			log.Println("Executed.")
		case ExecuteTableFull:
			log.Println("Error: Table full.")
		case ExecuteError:
			log.Println("Error: An unknown error occurred")
		}
	}
}

func printPrompt() {
	fmt.Print("tgdb > ")
}

func readInput() []byte {
	reader := bufio.NewReader(os.Stdin)
	line, _, err := reader.ReadLine()
	if err != nil {
		log.Fatalf("Error reading input. %v\n", err)
	}
	return line
}

func doMetaCommand(command []byte, table *Table) MetaCommandResult {
	if string(command) == ".exit" {
		dbClose(table)
		os.Exit(0)
	}

	return MetaCommandUnrecognizedCommand
}

func prepareStatement(input []byte, statement *Statement) PrepareResult {
	command := strings.TrimSpace(string(input))
	if strings.Index(strings.ToLower(command), "insert") == 0 {
		return prepareInsert(command, statement)
	}

	if strings.Index(strings.ToLower(command), "select") == 0 {
		statement.Type = StatementSelect
		return PrepareSuccess
	}

	return PrepareUnrecognizedStatement
}

func prepareInsert(input string, statement *Statement) PrepareResult {
	tokens := strings.Split(input, " ")
	if len(tokens) < 4 {
		return PrepareSyntaxError
	}

	statement.Type = StatementInsert
	//var id uint32
	var username, email string

	idStr := tokens[1]
	username = tokens[2]
	email = tokens[3]

	id, err := strconv.Atoi(idStr)
	if err != nil {
		return PrepareSyntaxError
	}
	if id < 0 {
		return PrepareNegativeId
	}

	if len(username) > int(UsernameSize) {
		return PrepareStringTooLong
	}

	if len(email) > int(EmailSize) {
		return PrepareStringTooLong
	}

	statement.RowToInsert.Id = uint32(id)
	copy(statement.RowToInsert.Username[:], username)
	copy(statement.RowToInsert.Email[:], email)

	return PrepareSuccess
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.NumRows >= TableMaxRows {
		return ExecuteTableFull
	}

	rowToInsert := statement.RowToInsert
	cursor := tableEnd(table)

	//offset := rowSlot(table, table.NumRows)
	offset := cursorValue(cursor)
	offsetArr := (*[PageSize]byte)(offset)

	serializeRow(&rowToInsert, offsetArr[:])
	table.NumRows += 1

	cursor = nil
	return ExecuteSuccess
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	cursor := tableStart(table)
	var row Row

	for !cursor.EndOfTable {
		offset := cursorValue(cursor)
		offsetArr := (*[PageSize]byte)(offset)
		deserializeRow(offsetArr[:], &row)
		printRow(&row)
		cursorAdvance(cursor)
	}

	cursor = nil
	return ExecuteSuccess
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
	switch statement.Type {
	case StatementInsert:
		return executeInsert(statement, table)
	case StatementSelect:
		return executeSelect(statement, table)
	}
	return ExecuteError
}

func serializeRow(source *Row, dest []byte) {
	idPtr := unsafe.Pointer(&source.Id)
	idArr := *((*[IdSize]byte)(idPtr))

	usernamePtr := unsafe.Pointer(&source.Username)
	usernameArr := *((*[UsernameSize]byte)(usernamePtr))

	emailPtr := unsafe.Pointer(&source.Email)
	emailArr := *((*[EmailSize]byte)(emailPtr))

	copy(dest[IdOffset:], idArr[:])
	copy(dest[UsernameOffset:], usernameArr[:])
	copy(dest[EmailOffset:], emailArr[:])
}

func deserializeRow(source []byte, dest *Row) {
	idPtr := unsafe.Pointer(&dest.Id)
	idArr := (*[IdSize]byte)(idPtr)

	usernamePtr := unsafe.Pointer(&dest.Username)
	usernameArr := (*[UsernameSize]byte)(usernamePtr)

	emailPtr := unsafe.Pointer(&dest.Email)
	emailArr := (*[EmailSize]byte)(emailPtr)

	end := IdOffset + uint32(IdSize)
	copy(idArr[:], source[IdOffset:end])
	end = UsernameOffset + uint32(UsernameSize)
	copy(usernameArr[:], source[UsernameOffset:end])
	end = EmailOffset + uint32(EmailSize)
	copy(emailArr[:], source[EmailOffset:end])
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.Id, row.Username, row.Email)
}
