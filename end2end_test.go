package main

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/mylux/bsistent/btree"
	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/utils"
	"github.com/stretchr/testify/assert"
)

type treeitem struct {
	Id            string `bsistent:"key"`
	SomethingMore int64
}

const listSize int64 = 500

func generateUniqueInts(size int64) []int64 {
	if size <= 0 {
		return nil
	}

	uniqueInts := make([]int64, 0, size)
	seen := make(map[int64]bool)

	for len(uniqueInts) < int(size) {
		num := rand.Int63n(int64(size*10)) + 1
		if !seen[num] {
			seen[num] = true
			uniqueInts = append(uniqueInts, num)
		}
	}

	return uniqueInts
}

func isSorted[T any](arr []interfaces.Item[T]) bool {
	for i := 1; i < len(arr); i++ {
		c, _ := arr[i-1].Compare(arr[i])
		if c == 1 {
			return false
		}
	}
	return true
}

func greaterThan[T any](i interfaces.Item[T], j interfaces.Item[T]) bool {
	return utils.OnError[int](func() (int, error) { return i.Compare(j) }, -1) == 1
}

func validatePage[T any](page interfaces.Page[T], bt *btree.Btree[T]) bool {
	if isSorted(page.Items().ToSlice()) {
		children := bt.LoadPageChildren(page)
		for i := 0; i < children.Size()-1; i++ {
			if greaterThan(children.Nth(i).Items().Last(), page.Item(i)) {
				return false
			}
		}
		return children.Size() == 0 || greaterThan(children.Last().Items().First(), page.Items().Last())
	}
	return false
}

func validatePages[T any](pages interfaces.PageChildren[T], bt *btree.Btree[T], t *testing.T) bool {
	children := bt.LoadOffsets(pages.Offsets())
	for _, p := range children {
		if !(assert.True(t, validatePage(p, bt) && validatePages(p.Children(), bt, t))) {
			return false
		}
	}
	return true
}

func validateTree[T any](bt *btree.Btree[T], t *testing.T) bool {
	return validatePage(bt.Root(), bt) && validatePages(bt.LoadPageChildren(bt.Root()), bt, t)
}

func setUpTreeOfInt(n int64, reset ...bool) *btree.Btree[int64] {
	var numbers []int64
	c := btree.Configuration[int64]().Grade(5).ItemSize(8).CacheSize(40).StoragePath("/tmp/unit-test-btree")
	if len(reset) > 0 && reset[0] {
		c = c.Reset()
		numbers = generateUniqueInts(n)
	}
	bt := c.Make()

	for _, i := range numbers {
		bt.Add(i)
	}
	return bt
}

func setUpTreeOfStruct(n int64, reset ...bool) *btree.Btree[treeitem] {
	var numberIds []int64
	var numberValues []int64
	nId := 10
	letters := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	itemSize := utils.ReturnOrPanic(func() (int, error) { return utils.SizeOf(treeitem{Id: "0123456789", SomethingMore: 0}) })
	c := btree.Configuration[treeitem]().Grade(5).ItemSize(int64(itemSize)).CacheSize(40).StoragePath("/tmp/unit-test-btree")
	if len(reset) > 0 && reset[0] {
		c = c.Reset()
		numberValues = generateUniqueInts(n)
	}
	bt := c.Make()

	for _, i := range numberValues {
		var id string
		numberIds = generateUniqueInts(int64(nId * 3))
		for _, j := range numberIds[:nId] {
			id = fmt.Sprintf("%s%s", id, string(letters[(int(j)%len(letters))]))
		}
		bt.Add(treeitem{
			Id:            id,
			SomethingMore: i,
		})
	}
	return bt
}

func TestNewTree(t *testing.T) {
	n := listSize
	bt := setUpTreeOfInt(n, true)
	assert.Equal(t, n, bt.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(bt, t))
}

func TestNewTreeStruct(t *testing.T) {
	n := listSize
	bt := setUpTreeOfStruct(n, true)
	assert.Equal(t, n, bt.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(bt, t))
}

func TestLoadFromDisk(t *testing.T) {
	n := listSize
	btn := setUpTreeOfInt(n, true)
	assert.Equal(t, n, btn.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(btn, t))
	bto := setUpTreeOfInt(n)
	assert.Equal(t, n, bto.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(bto, t))
	assert.Equal(t, btn.Size(), bto.Size(), "The size of both the new list and the one loaded from disk should be the same")
	assert.Equal(t, btn.Root(), bto.Root(), "Both lists should have their roots equivalent")
}

func TestLoadFromDistStruct(t *testing.T) {
	n := listSize
	btn := setUpTreeOfStruct(n, true)
	assert.Equal(t, n, btn.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(btn, t))
	bto := setUpTreeOfStruct(n)
	assert.Equal(t, n, bto.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(bto, t))
	assert.Equal(t, btn.Size(), bto.Size(), "The size of both the new list and the one loaded from disk should be the same")
	assert.Equal(t, btn.Root(), bto.Root(), "Both lists should have their roots equivalent")
}

func TestFindStructExisting(t *testing.T) {
	n := listSize
	btn := setUpTreeOfStruct(n, true)
	assert.Equal(t, n, btn.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(btn, t))
	x := treeitem{
		Id:            "MyId567890",
		SomethingMore: 23,
	}
	btn.Add(x)
	found, xf := btn.Find(treeitem{
		Id: "MyId567890",
	})
	assert.True(t, found)
	assert.Equal(t, x, xf)
}

func TestFindStructNonExisting(t *testing.T) {
	n := listSize
	btn := setUpTreeOfStruct(n, true)
	assert.Equal(t, n, btn.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(btn, t))
	found, _ := btn.Find(treeitem{
		Id: "..Id567890",
	})
	assert.False(t, found)
}
