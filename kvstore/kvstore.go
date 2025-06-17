package kvstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"kurocifer/LeichtKV/btree"
	"kurocifer/LeichtKV/utils"
	"os"
	"syscall"
)

// create the initial mmap that covers the while file.
func mmapInt(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}

	if fi.Size()%btree.BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("File size is not a multiple of page size")
	}

	mmapSize := 64 << 20 // 64MB
	utils.Assert(mmapSize < int(fi.Size()))

	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}

	// mmapSize can be larger than the file
	chunk, err := syscall.Mmap(
		int(fp.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}

	return int(fi.Size()), chunk, nil
}

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree btree.BTree

	mmap struct {
		file   int
		total  int
		chunks [][]byte
	}

	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}
}

// extend the mmap by adding new mappings
func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*btree.BTREE_PAGE_SIZE {
		return nil
	}

	chunk, err := syscall.Mmap(
		int(db.fp.Fd()), int64(db.mmap.total), db.mmap.total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)

	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// callback for BTree, dereference a pointer. Accessing a page from the mapped address
func (db *KV) pageGet(ptr uint64) btree.BNode {
	start := uint64(0)

	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/btree.BTREE_PAGE_SIZE
		if ptr < end {
			offset := btree.BTREE_PAGE_SIZE * (ptr - start)
			return btree.BNode{Data: chunk[offset : offset+btree.BTREE_PAGE_SIZE]}
		}
		start = end
	}

	panic("bad ptr")
}

const DB_SIG = "BANKAI"

func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty file, the master page will be created on the first write
		db.page.flushed = 1 // reserced for the master page
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])

	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("Bad signature")
	}

	bad := !(1 <= used && used <= uint64(db.mmap.file/btree.BTREE_PAGE_SIZE))
	bad = bad || !(0 <= root && root < used)

	if bad {
		return errors.New("Bad master page.")
	}

	db.tree.Root = root
	db.page.flushed = used
	return nil
}

// update the master page. Must be atomic
func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))

	binary.LittleEndian.PutUint64(data[16:], db.tree.Root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)

	// NOTE: Updating the page via mmap is not atomic.
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

// callback for BTree, allocate a new page
func (db *KV) pageNew(node btree.BNode) uint64 {
	utils.Assert(len(node.Data) <= btree.BTREE_PAGE_SIZE)
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node.Data)
	return ptr
}

// callback for BTree, deallocate a page
func (db *KV) pageDel(uint64) {

}

// extend the file to at least npages
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / btree.BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}

	for filePages < npages {
		// the file size is increased exponentially, so that we don't have to extend the file for every update
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}

	fileSize := filePages * btree.BTREE_PAGE_SIZE
	err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}

	db.mmap.file = fileSize
	return nil
}

func (db *KV) Open() error {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp

	// create the initial mmap
	sz, chunk, err := mmapInt(db.fp)
	if err != nil {
		goto fail
	}

	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// btree callbacks
	db.tree.Get = db.pageGet
	db.tree.New = db.pageNew
	db.tree.Del = db.pageDel

	// read the master page
	err = masterLoad(db)
	if err != nil {
		goto fail
	}

	return nil

fail:
	db.fp.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		utils.Assert(err == nil)
	}
	_ = db.fp.Close()
}

// Update operatins must persist data before returning

// read the db
func (db *KV) Get(key uint64) btree.BNode {
	return db.tree.Get(key)
}

// update the db
func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

// persist the newly allocated pages after updates
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}

	return syncPages(db)
}

func writePages(db *KV) error {
	npages := int(db.page.flushed) + len(db.page.temp)
	if err := extendFile(db, npages); err != nil {
		return err
	}

	// copy data to the file
	for i, page := range db.page.temp {
		ptr := db.page.flushed + uint64(i)
		copy(db.pageGet(ptr).Data, page)
	}

	return nil
}

func syncPages(db *KV) error {
	// Flush data to the disk. Must be done before updating master
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]

	// update and flush the master
	if err := masterStore(db); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	return nil
}
