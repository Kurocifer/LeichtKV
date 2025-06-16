package btree

import (
	"bytes"
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

// returns the first kid node whose range intersects the key. (kid[i] <= key)
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	// The first key is a copy from the parent node, thus it's always less than or equal to teh key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)

		if cmp <= 0 {
			found = i
		}

		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a new key to the leaf node
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// Copies keys from an old node to a new node
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	utils.Assert(srcOld+n <= old.nkeys())
	utils.Assert(dstNew+n <= new.nkeys())

	if n == 0 {
		return
	}

	// copy pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+1, old.getPtr(srcOld+1))
	}

	// copy offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	// copy kvs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// copy a kv pair into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)

	// kvs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)

	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// Insert a KV into a node, the result might be split into 2 nodes.
// the caller is responsible for deallocating the input node and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The result node. Can be bigger than 1 page and if so will be splitted
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// where to insert the key?
	idx := nodeLookupLE(node, key)

	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key update it
			leafInsert(new, node, idx, key, val)
		} else {
			// insert if after the position
			leafInsert(new, node, idx+1, key, val)
		}

	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, new, node, idx, key, val)

	default:
		panic("bad node!")
	}

	return new
}

func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte) {
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)

	knode = treeInsert(tree, knode, key, val)
	nsplit, splitted := nodeSplit3(knode)

	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splitted[:nsplit]...)
}

func nodeSplit2(left BNode, right BNode, old BNode) {

}

// split a node if it's too big. the results are 1-3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)}
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// the left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	utils.Assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right}
}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}
