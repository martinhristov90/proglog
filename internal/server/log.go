package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(record Record) (uint64,error) {
	c.mu.Lock() // Lock with mutex
	defer c.mu.Unlock() // Unlock with mutex at the end of execute of this method

	record.Offset = uint64(len(c.records)) // Calculating the starting point for the new commit entry
	c.records = append(c.records, record) // Appeding the commit

	return record.Offset, nil // Returning the starting point from where the newly inserted data begin
}

func (c *Log) Read(offset uint64) (Record,error) {

	c.mu.Lock() // Locking to do the read
	defer c.mu.Unlock() // Unlocking at the end of method execution

	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound // If the given offset is larger than the commit log, error is returned
	}

	return c.records[offset],nil
}

type Record struct {
	Value []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")