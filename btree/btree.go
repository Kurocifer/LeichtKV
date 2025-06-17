package btree

import (
	"bytes"
	"encoding/binary"
	"kurocifer/LeichtKV/utils"
)

// Represnts the content a disk page
type BNode struct {
	Data []byte
}

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

type BTree struct {
	// Pointer (a nonzero page)
	Root uint64

	// Callbacks for managing on-disk pages
	Get func(uint64) BNode // dereference a pointer (takes a pointer, an returns the Node at that location (page))
	New func(BNode) uint64 // allocates a New page
	Del func(uint64)       // deallocate a page
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
	return binary.LittleEndian.Uint16(node.Data)
}

// returns the next 2 bytes (2 and 3) of the header which holds the number of keys
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.Data[2:4])
}

// Sets the header Data (node type and the number of keys)
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.Data[0:2], btype)
	binary.LittleEndian.PutUint16(node.Data[2:4], nkeys)
}

// pointers
func (node BNode) GetPtr(idx uint16) uint64 {
	utils.Assert(idx < node.nkeys())

	pos := HEADER + (8 * idx) // skip the header, and the previous 64 bit pointers (idx * 8)
	return binary.LittleEndian.Uint64(node.Data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	utils.Assert(idx < node.nkeys())

	pos := HEADER + (8 * idx)
	binary.LittleEndian.PutUint64(node.Data[pos:], val)
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	utils.Assert(1 <= idx && idx <= node.nkeys())
	return HEADER + (8 * node.nkeys()) + 2*(idx-1)
}

func (node BNode) GetOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.Data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.Data[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	utils.Assert(idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.GetOffset(idx)
}

func (node BNode) GetKey(idx uint16) []byte {
	utils.Assert(idx < node.nkeys())

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos:])
	return node.Data[pos+1:][:klen]
}

func (node BNode) GetVal(idx uint16) []byte {
	utils.Assert(idx < node.nkeys())

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.Data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.Data[pos+2:])

	return node.Data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// returns the first kid node whose range intersects the key. (kid[i] <= key)
func noDelookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	// The first key is a copy from the parent node, thus it's always less than or equal to teh key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.GetKey(i), key)

		if cmp <= 0 {
			found = i
		}

		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a New key to the leaf node
func leafInsert(New BNode, old BNode, idx uint16, key []byte, val []byte) {
	New.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(New, old, 0, 0, idx)
	nodeAppendKV(New, idx, 0, key, val)
	nodeAppendRange(New, old, idx+1, idx, old.nkeys()-idx)
}

// Copies keys from an old node to a New node
func nodeAppendRange(New BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	utils.Assert(srcOld+n <= old.nkeys())
	utils.Assert(dstNew+n <= New.nkeys())

	if n == 0 {
		return
	}

	// copy pointers
	for i := uint16(0); i < n; i++ {
		New.setPtr(dstNew+1, old.GetPtr(srcOld+1))
	}

	// copy offsets
	dstBegin := New.GetOffset(dstNew)
	srcBegin := old.GetOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.GetOffset(srcOld+i) - srcBegin
		New.setOffset(dstNew+i, offset)
	}

	// copy kvs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(New.Data[New.kvPos(dstNew):], old.Data[begin:end])
}

// copy a kv pair into the position
func nodeAppendKV(New BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	New.setPtr(idx, ptr)

	// kvs
	pos := New.kvPos(idx)
	binary.LittleEndian.PutUint16(New.Data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(New.Data[pos+2:], uint16(len(val)))
	copy(New.Data[pos+4:], key)
	copy(New.Data[pos+4+uint16(len(key)):], val)

	// the offset of the next key
	New.setOffset(idx+1, New.GetOffset(idx)+4+uint16((len(key)+len(val))))
}

// Insert a KV into a node, the result might be split into 2 nodes.
// the caller is responsible for deallocating the input node and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The result node. Can be bigger than 1 page and if so will be splitted
	New := BNode{Data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// where to insert the key?
	idx := noDelookupLE(node, key)

	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.GetKey(idx)) {
			// found the key update it
			leafInsert(New, node, idx, key, val)
		} else {
			// insert if after the position
			leafInsert(New, node, idx+1, key, val)
		}

	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, New, node, idx, key, val)

	default:
		panic("bad node!")
	}

	return New
}

func nodeInsert(tree *BTree, New BNode, node BNode, idx uint16, key []byte, val []byte) {
	// Get and deallocate the kid node
	kptr := node.GetPtr(idx)
	knode := tree.Get(kptr)
	tree.Del(kptr)

	knode = treeInsert(tree, knode, key, val)
	nsplit, splitted := nodeSplit3(knode)

	// update the kid links
	nodeReplaceKidN(tree, New, node, idx, splitted[:nsplit]...)
}

func nodeSplit2(left BNode, right BNode, old BNode) {

}

// split a node if it's too big. the results are 1-3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.Data = old.Data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)}
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.Data = left.Data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// the left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	utils.Assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right}
}

func nodeReplaceKidN(tree *BTree, New BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	New.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(New, old, 0, 0, idx)

	for i, node := range kids {
		nodeAppendKV(New, idx+uint16(i), tree.New(node), node.GetKey(0), nil)
	}
	nodeAppendRange(New, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// Deletion

// remove a key from a leaf node
func leafDelete(New BNode, old BNode, idx uint16) {
	New.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(New, old, 0, 0, idx)
	nodeAppendRange(New, old, idx, idx+1, old.nkeys()-(idx+1))
}

// Delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// find the location of the key
	idx := noDelookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.GetKey(idx)) {
			return BNode{} // node not found
		}

		// Delete the key in the leaf
		New := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(New, node, idx)
		return New

	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)

	default:
		panic("bad node!")
	}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.GetPtr(idx)
	updated := treeDelete(tree, tree.Get(kptr), key)
	if len(updated.Data) == 0 {
		return BNode{} // not found
	}
	tree.Del(kptr)

	New := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}

	// Check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)

	switch {
	case mergeDir < 0:
		merged := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.Del(node.GetPtr(idx - 1))
		nodeReplace2Kid(New, node, idx-1, tree.New(merged), merged.GetKey(0))

	case mergeDir > 0:
		merged := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.Del(node.GetPtr(idx + 1))
		nodeReplace2Kid(New, node, idx, tree.New(merged), merged.GetKey(0))

	case mergeDir == 0:
		utils.Assert(updated.nkeys() > 0)
		nodeReplaceKidN(tree, New, node, idx, updated)
	}

	return New
}

func nodeReplace2Kid(New, node BNode, u1 uint16, u2 uint64, b []byte) {
}

// merge 2 nodes into 1
func nodeMerge(New BNode, left BNode, right BNode) {
	New.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(New, left, 0, 0, left.nkeys())
	nodeAppendRange(New, right, left.nkeys(), 0, right.nkeys())
}

// determine if the updated kid should be merged with the sibling
func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := tree.Get(node.GetPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER

		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}

	if idx+1 < node.nkeys() {
		sibling := tree.Get(node.GetPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER

		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}

	return 0, BNode{}
}

// managing the Root node as tree grows and shrinks

func (tree *BTree) Delete(key []byte) bool {
	utils.Assert(len(key) != 0)
	utils.Assert(len(key) <= BTREE_MAX_KEY_SIZE)
	if tree.Root == 0 {
		return false
	}

	updated := treeDelete(tree, tree.Get(tree.Root), key)
	if len(updated.Data) == 0 {
		return false // not found
	}

	tree.Del(tree.Root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// trim a level
		tree.Root = updated.GetPtr(0)
	} else {
		tree.Root = tree.New(updated)
	}

	return true
}

func (tree *BTree) Insert(key []byte, val []byte) {
	utils.Assert(len(key) != 0)
	utils.Assert(len(key) <= BTREE_MAX_KEY_SIZE)
	utils.Assert(len(val) <= BTREE_MAX_VALUE_SIZE)

	if tree.Root == 0 {
		Root := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		Root.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(Root, 0, 0, nil, nil)
		nodeAppendKV(Root, 1, 0, key, val)

		tree.Root = tree.New(Root)
		return
	}

	node := tree.Get(tree.Root)
	tree.Del(tree.Root)

	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)

	if nsplit > 1 {
		Root := BNode{Data: make([]byte, BTREE_PAGE_SIZE)}
		Root.setHeader(BNODE_NODE, nsplit)

		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.New(knode), knode.GetKey(0)
			nodeAppendKV(Root, uint16(i), ptr, key, nil)
		}
		tree.Root = tree.New(Root)
	} else {
		tree.Root = tree.New((splitted[0]))
	}
}
