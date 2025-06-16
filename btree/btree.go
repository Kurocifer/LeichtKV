package btree

import (
	"encoding/binary"
	"kurocifer/LeichtKV/utils"
)

// Represnts the content a disk page
type BNode struct {
	data []byte
}

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

type BTree struct {
	// Pointer (a nonzero page)
	root uint64

	// Callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer (takes a pointer, an returns the Node at that location (page))
	new func(BNode) uint64 // allocates a new page
	del func(uint64)       // deallocate a page
}

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VALUE_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VALUE_SIZE
	utils.Assert(node1max <= BTREE_PAGE_SIZE, "node1max exceeds page size")
}

// Header

// this two functions accesses the first 4 bytes of the BNode, which is the Header, holding the node type and it's number of keys

// returns the first 2 bytes of the header which holds information on the node type
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

// returns the next 2 bytes (2 and 3) of the header which holds the number of keys
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

// Sets the header data (node type and the number of keys)
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	utils.Assert(idx < node.nkeys())

	pos := HEADER + (8 * idx) // skip the header, and the previous 64 bit pointers (idx * 8)
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	utils.Assert(idx < node.nkeys())

	pos := HEADER + (8 * idx)
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	utils.Assert(1 <= idx && idx <= node.nkeys())
	return HEADER + (8 * node.nkeys()) + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	utils.Assert(idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	utils.Assert(idx < node.nkeys())

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+1:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	utils.Assert(idx < node.nkeys())

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])

	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}
