package main

import (
	"io"
	"log"
	"os"
	"unsafe"
)

type Pager struct {
	fileDescriptor *os.File
	fileLength     uint32
	Pages          [TableMaxPages]unsafe.Pointer //each page is a [PageSize]byte
}

func pagerOpen(fileName string) *Pager {
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		log.Printf("Unable to open file %s.\n", fileName)
		os.Exit(1)
	}

	//fileLength, err := syscall.Seek(int(file.Fd()), 0, io.SeekEnd)
	fileLength, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Println("Unable to open get file length")
		os.Exit(1)
	}

	pager := new(Pager)
	pager.fileDescriptor = file
	pager.fileLength = uint32(fileLength)

	for i := uint32(0); i < TableMaxPages; i++ {
		pager.Pages[i] = nil
	}
	return pager
}

func pagerFlush(pager *Pager, pageNum uint32, size uint32) {
	if pager.Pages[pageNum] == nil {
		log.Println("Tried to flush a null page")
		os.Exit(1)
	}

	_, err := pager.fileDescriptor.Seek(int64(pageNum*PageSize), io.SeekStart)
	if err != nil {
		log.Printf("Error seeking: %v\n", err)
		os.Exit(1)
	}

	buff := pager.Pages[pageNum]
	buffArr := (*[PageSize]byte)(buff)

	bytesWritten, err := pager.fileDescriptor.WriteAt(buffArr[:size], int64(pageNum*PageSize))
	if err != nil {
		log.Printf("Error writing: %v.\n", err)
		os.Exit(1)
	}
	log.Printf("Wrote %d bytes to file %s\n", bytesWritten, pager.fileDescriptor.Name())
}

func getPage(pager *Pager, pageNum uint32) unsafe.Pointer {
	if pageNum > TableMaxPages {
		log.Printf("Tried to fetch page number out of bounds. %d > %d.\n", pageNum, TableMaxPages)
		os.Exit(1)
	}

	if pager.Pages[pageNum] == nil {
		// cache miss. allocate memory and load from file
		page := [PageSize]byte{}
		numPages := pager.fileLength / PageSize

		// We might save a partial page at the end of the file
		if pager.fileLength%PageSize > 0 {
			numPages += 1
		}

		if numPages > 0 && pageNum <= numPages {
			pager.fileDescriptor.Seek(int64(pageNum*PageSize), io.SeekStart)
			bytesRead, err := pager.fileDescriptor.ReadAt(page[:], int64(pageNum*PageSize))
			if err != nil && err != io.EOF {
				log.Printf("Error reading file %v.\n", err)
				os.Exit(1)
			}
			log.Printf("Successfuly read %d bytes from %s", bytesRead, pager.fileDescriptor.Name())
		}

		pager.Pages[pageNum] = unsafe.Pointer(&page)
	}

	return pager.Pages[pageNum]
}
