package main

import (
	"fmt"
	"math/rand"

	"github.com/mylux/bsistent/btree"
	"github.com/mylux/bsistent/interfaces"
)

var fixedItems = []int64{298, 46, 1, 286, 191, 364, 278, 353, 132, 156, 8, 179, 420, 60, 322, 55, 367, 302, 319, 36, 106, 81, 218, 210, 239, 328, 78, 11, 83, 244, 34, 270, 456, 265, 9, 211, 281, 300, 464, 334, 249, 229, 6, 94, 490, 362, 157, 426, 135, 30}

func generateUniqueInts(size int) []int64 {
	if size <= 0 {
		return nil
	}

	uniqueInts := make([]int64, 0, size)
	seen := make(map[int64]bool)

	for len(uniqueInts) < size {
		num := rand.Int63n(int64(size*10)) + 1
		if !seen[num] {
			seen[num] = true
			uniqueInts = append(uniqueInts, num)
		}
	}

	return uniqueInts
}

func countPage(p interfaces.Page[int64]) int {
	var res int = 0

	if !p.IsLeaf() {
		for i := range p.Size() + 1 {
			res += countPage(p.Child(i))
		}
	}
	res += p.Size()
	return res
}

func count(b *btree.Btree[int64]) int {
	return countPage(b.Root())
}

func main() {
	var strT string
	var b *btree.Btree[int64] = btree.Configuration[int64]().Grade(5).ItemSize(8).Make()
	if b.IsEmpty() {
		elements := generateUniqueInts(50)
		// elements := fixedItems[:50]
		fmt.Printf("Elements: %v\n", elements)
		fmt.Printf("Created a Btreee object with size=%d and storing data in %s file\n", b.Size(), b.StoragePath())
		fmt.Printf("Add %d random item(s) into the tree\n", len(elements))
		for _, e := range elements {
			//fmt.Printf("Add an element into the tree: %d\n", e)
			b.Add(e)
		}
		fmt.Println("Finished adding items")
		fmt.Printf("Item count %d vs %d\n", count(b), len(elements))
		strT = fmt.Sprintf("%v\n", b)
	} else {
		fmt.Println("Btree is not empty")
		strT = fmt.Sprintf("%v\n", b)
		fmt.Printf("Item count %d\n", count(b))
	}

	fmt.Printf("%v\n", strT)
}
