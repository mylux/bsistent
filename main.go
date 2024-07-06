package main

import (
	"fmt"
	"math/rand"

	"github.com/mylux/bsistent/btree"
	"github.com/mylux/bsistent/interfaces"
)

// var fixedItems []int64 = []int64{59, 73, 162, 97, 174, 212, 239, 209, 206, 136, 141, 166, 204, 9, 36, 48, 149, 133, 121, 119, 224, 169, 40, 157, 50}
// var fixedItems []int64 = []int64{119, 195, 67, 32, 116, 121, 75, 213, 177, 104, 8, 25, 244, 62, 223, 70, 37, 159, 29, 53, 55, 134, 131, 225, 9}
// var fixedItems = []int64{171, 136, 274, 158, 187, 111, 59, 163, 144, 115, 179, 167, 265, 60, 133, 72, 149, 39, 290, 201, 10, 258, 131, 227, 208, 245, 172, 79, 132, 76}

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
	var b *btree.Btree[int64] = btree.Configuration[int64]().Grade(5).ItemSize(8).Make()
	if b.IsEmpty() {
		elements := generateUniqueInts(5)
		// elements := fixedItems
		fmt.Printf("Elements: %v\n", elements)
		fmt.Printf("Created a Btreee object with size=%d and storing data in %s file\n", b.Size(), b.StoragePath())
		fmt.Printf("Add %d random item(s) into the tree\n", len(elements))
		for _, e := range elements {
			//fmt.Printf("Add an element into the tree: %d\n", e)
			b.Add(e)
		}
		fmt.Println("Finished adding items")
		fmt.Printf("Item count %d vs %d\n", count(b), len(elements))
	} else {
		fmt.Println("Btree is not empty")
	}

	fmt.Printf("%v\n", b)
}
