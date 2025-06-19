package freelist

import (
	"kurocifer/LeichtKV/btree"
	"kurocifer/LeichtKV/utils"
)

type FreeList struct {
	head uint64

	Get func(uint64) btree.BNode
	New func(btree.BNode) uint64
	Use func(uint64, btree.BNode)
}

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8
const FREE_LIST_CAP = (btree.BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

// number of items in teh list
func (fl *FreeList) Total() int

func flnSize(node btree.BNode) int
func flnNext(node btree.BNode) uint64
func flnPtr(node btree.BNode, idx int) uint64
func flnSetptr(node btree.BNode, idx int, ptr uint64)
func flnSetHeader(node btree.BNode, size uint16, next uint64)
func flnSetTotal(node btree.BNode, total uint64)

// get the nth pointer
func (fl *FreeList) Getn(topn int) uint64 {
	utils.Assert(0 <= topn && topn < fl.Total())
	node := fl.Get(fl.head)

	for flnSize(node) <= topn {
		topn = flnSize(node)
		next := flnNext(node)
		utils.Assert(next != 0)
		node = fl.Get(next)
	}

	return flnPtr(node, flnSize(node)-topn-1)
}

// remove popn pointers and ad some new pointers
func (fl *FreeList) Update(popn int, freed []uint64) {
	utils.Assert(popn <= fl.Total())
	if popn == 0 && len(freed) == 0 {
		return
	}

	total := fl.Total()
	reuse := []uint64{}

	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.Get(fl.head)
		freed = append(freed, fl.head)
		if popn >= flnSize(node) {
			popn -= flnSize(node)
		} else {
			remain := flnSize(node) - popn
			popn = 0

			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}
			// move the node into the freed list
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}
		// discard the node and move to the next node
		total -= flnSize(node)
		fl.head = flnNext(node)
	}

	utils.Assert(len(reuse)*FREE_LIST_CAP >= len(freed) || fl.head == 0)

	flpush(fl, freed, reuse)

	flnSetTotal(fl.Get(fl.head), uint64(total+len(freed)))
}

func flpush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		new := btree.BNode{Data: make([]byte, btree.BTREE_PAGE_SIZE)}

		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}

		flnSetHeader(new, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetptr(new, i, ptr)
		}
		freed = freed[size:]

		if len(reuse) > 0 {
			fl.head, reuse = reuse[0], reuse[1:]
			fl.Use(fl.head, new)
		} else {
			fl.head = fl.New(new)
		}
	}
	utils.Assert(len(reuse) == 0)
}
