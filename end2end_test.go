package main_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mylux/bsistent/btree"
	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/utils"
	"github.com/stretchr/testify/assert"
)

type treeitem struct {
	Id            string `bsistent:"key;maxSize:255"`
	SomethingMore int64
}

const treeSize int64 = 500

var cacheSize int = 40

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
	c := btree.Configuration[int64]().Grade(5).ItemSize(8).CacheSize(uint32(cacheSize)).StoragePath("/tmp/unit-test-btree")
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
	c := btree.Configuration[treeitem]().Grade(5).ItemShape(treeitem{Id: "0123456789", SomethingMore: 0}).CacheSize(uint32(cacheSize)).StoragePath("/tmp/unit-test-btree")
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
	n := treeSize
	bt := setUpTreeOfInt(n, true)
	assert.Equal(t, n, bt.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(bt, t))
}

func TestNewTreeStruct(t *testing.T) {
	n := treeSize
	bt := setUpTreeOfStruct(n, true)
	assert.Equal(t, n, bt.Size(), "The list size and the number of elements should be the same")
	assert.True(t, validateTree(bt, t))
}

func TestNewTreeStructVariableSize(t *testing.T) {
	n := treeSize
	bt := setUpTreeOfStruct(n, true)
	assert.Equal(t, n, bt.Size(), "The list size and the number of elements should be the same")
	lessThan10 := treeitem{
		Id:            "LessThan",
		SomethingMore: 10,
	}
	tiny := treeitem{
		Id:            "tiny",
		SomethingMore: 1,
	}
	big := treeitem{
		Id:            "Spoke to the people on the beaches we used to sit alone",
		SomethingMore: 19,
	}
	bt.Add(lessThan10)
	bt.Add(tiny)
	bt.Add(big)
	assert.True(t, validateTree(bt, t))
	found, item := bt.Find(treeitem{
		Id: "LessThan",
	})
	assert.True(t, found)
	assert.Equal(t, lessThan10, item)
	found, item = bt.Find(treeitem{
		Id: "tiny",
	})
	assert.True(t, found)
	assert.Equal(t, tiny, item)
	found, item = bt.Find(treeitem{
		Id: "Spoke to the people on the beaches we used to sit alone",
	})
	assert.True(t, found)
	assert.Equal(t, big, item)
}

func TestLoadFromDisk(t *testing.T) {
	n := treeSize
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
	n := treeSize
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
	n := treeSize
	btn := setUpTreeOfStruct(n, true)
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
	n := treeSize
	btn := setUpTreeOfStruct(n, true)
	found, _ := btn.Find(treeitem{
		Id: "..Id567890",
	})
	assert.False(t, found)
}

func TestCompareCache(t *testing.T) {
	sthMore := int64(1)
	cacheSize = 0
	partial := treeitem{
		Id: "mything",
	}
	n := treeSize
	bt := setUpTreeOfStruct(n, true)
	bt.Add(treeitem{
		Id:            "mything",
		SomethingMore: sthMore,
	})
	found, item := bt.Find(partial)
	assert.True(t, found)
	assert.Equal(t, sthMore, item.SomethingMore)

	start := time.Now()
	found, item = bt.Find(partial)
	elapsedNoCache := time.Since(start)

	assert.True(t, found)
	assert.Equal(t, sthMore, item.SomethingMore)

	cacheSize = 40
	bt = setUpTreeOfStruct(n)
	assert.Equal(t, int64(501), bt.Size())

	found, item = bt.Find(partial)
	assert.True(t, found)
	assert.Equal(t, sthMore, item.SomethingMore)

	start = time.Now()
	found, item = bt.Find(partial)
	elapsedWithCache := time.Since(start)
	assert.True(t, found)
	assert.Equal(t, sthMore, item.SomethingMore)
	enc := elapsedNoCache.Nanoseconds()
	ewc := elapsedWithCache.Nanoseconds()
	assert.Greater(t, enc, ewc)
	var perfImp float64 = ((float64(enc) - float64(ewc)) / float64(enc)) * 100
	var speedRatio float64 = float64(enc) / float64(ewc) * 100
	fmt.Printf("Find performance Without Cache: %d ns, versus %d ns with cache. ", enc, ewc)
	fmt.Printf("Resulting in a performance improvement of %.2f%% or ", perfImp)
	fmt.Printf("Search with cache being %.2f%% percent faster than the same search with no cache\n", speedRatio)
}
