package main_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mylux/bsistent/btree"
	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/utils"
	"github.com/samber/lo"
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

func validatePage[T any](page interfaces.Page[T], bt *btree.Btree[T]) error {
	if !bt.PageIsValid(page) {
		return fmt.Errorf("page %s is not valid", page.String())
	}

	if !isSorted(page.Items().ToSlice()) {
		return fmt.Errorf("page %s is not sorted", page.String())
	}

	children := bt.LoadPageChildren(page)
	for i := 0; i < children.Size()-1; i++ {
		cc := children.Nth(i)
		if last := cc.Items().Last(); greaterThan(last, page.Item(i)) {
			return fmt.Errorf("page %s: item %s in position %d is less than its last child: %s", page.String(), page.Item(i).String(), i, last.String())
		}
		ep, ei := bt.FindEdgeItem(cc, true)
		if greaterThan(ep.Item(ei), page.Item(i)) {
			return fmt.Errorf("edge page %s: item %s in position %d is greater than current page item: %s", ep.String(), page.Item(i).String(), ei, page.Item(i).String())
		}
	}

	if last := page.Items().Last(); children.Size() > 0 {
		if firstItemLastChild := children.Last().Items().First(); !greaterThan(firstItemLastChild, last) {
			return fmt.Errorf("page %s: first item of the last child %s is not greater than its parent item %s", page.String(), firstItemLastChild.String(), last.String())
		}
	}
	return nil
}

func validatePages[T any](pages interfaces.PageChildren[T], bt *btree.Btree[T], t *testing.T) error {
	children := bt.LoadOffsets(pages.Offsets())
	for _, p := range children {
		if err := validatePage(p, bt); err != nil {
			return err
		}
		if err := validatePages(p.Children(), bt, t); err != nil {
			return err
		}
	}
	return nil
}

func validateTree[T any](bt *btree.Btree[T], t *testing.T) error {
	if err := validatePage(bt.Root(), bt); err != nil {
		return err
	}
	if err := validatePages(bt.LoadPageChildren(bt.Root()), bt, t); err != nil {
		return err
	}
	return nil
}

func setUpTreeOfInt(n int64, reset ...bool) *btree.Btree[int64] {
	var numbers []int64
	c := btree.Configuration[int64]().Grade(5).ItemSize(8).CacheSize(uint32(cacheSize)).StoragePath("/tmp/unit-test-btree")
	if len(reset) > 0 && reset[0] {
		numbers = generateUniqueInts(n)
		return setUpTreeOfPredefinedInt(numbers, c)
	}
	bt := c.Make()
	return bt
}

func setUpTreeOfPredefinedInt(numbers []int64, config *btree.BTConfig[int64]) *btree.Btree[int64] {
	bt := config.Reset().Make()
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
	assert.NoError(t, validateTree(bt, t))
}

func TestNewTreeStruct(t *testing.T) {
	n := treeSize
	bt := setUpTreeOfStruct(n, true)
	assert.Equal(t, n, bt.Size(), "The list size and the number of elements should be the same")
	assert.NoError(t, validateTree(bt, t))
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
	assert.NoError(t, validateTree(bt, t))
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
	assert.NoError(t, validateTree(btn, t))
	bto := setUpTreeOfInt(n)
	assert.Equal(t, n, bto.Size(), "The list size and the number of elements should be the same")
	assert.NoError(t, validateTree(bto, t))
	assert.Equal(t, btn.Size(), bto.Size(), "The size of both the new list and the one loaded from disk should be the same")
	assert.Equal(t, btn.Root(), bto.Root(), "Both lists should have their roots equivalent")
}

func TestLoadFromDistStruct(t *testing.T) {
	n := treeSize
	btn := setUpTreeOfStruct(n, true)
	assert.Equal(t, n, btn.Size(), "The list size and the number of elements should be the same")
	assert.NoError(t, validateTree(btn, t))
	bto := setUpTreeOfStruct(n)
	assert.Equal(t, n, bto.Size(), "The list size and the number of elements should be the same")
	assert.NoError(t, validateTree(bto, t))
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

func TestDeleteOne(t *testing.T) {
	n := treeSize
	bt := setUpTreeOfInt(n, true)
	bt.Add(6666)
	assert.NotEmpty(t, bt)
	assert.Equal(t, bt.Size(), n+1)
	found, _ := bt.Find(6666)
	assert.True(t, found)
	err := bt.Delete(6666)
	assert.NoError(t, err)
	assert.Equal(t, bt.Size(), n)
	found, _ = bt.Find(6666)
	assert.False(t, found)
	assert.NoError(t, validateTree(bt, t))
	bt2 := setUpTreeOfInt(n)
	assert.Equal(t, bt2.Size(), n)
	found, _ = bt2.Find(6666)
	assert.False(t, found)
}

func TestDeleteMany(t *testing.T) {
	n := treeSize
	config := btree.Configuration[int64]().Grade(5).ItemSize(8).CacheSize(uint32(cacheSize)).StoragePath("/tmp/unit-test-btree")
	numbers := generateUniqueInts(n)
	bt := setUpTreeOfPredefinedInt(numbers, config)
	assert.NotEmpty(t, bt)
	assert.Equal(t, n, bt.Size())
	for i, number := range lo.Shuffle(numbers) {
		assert.True(t, utils.GetFirstReturned(func() (bool, int64) { return bt.Find(number) }))
		err := bt.Delete(number)
		assert.NoError(t, err)
		assert.Equal(t, n-int64(i)-1, bt.Size())
		found, _ := bt.Find(number)
		assert.False(t, found)
		assert.NoError(t, validateTree(bt, t))
	}
}

func TestDeletePreset(t *testing.T) {
	n := treeSize
	config := btree.Configuration[int64]().Grade(5).ItemSize(8).CacheSize(uint32(cacheSize)).StoragePath("/tmp/unit-test-btree")
	numbers := []int64{724, 801, 3247, 2864, 3672, 1489, 627, 2776, 3609, 172, 2428, 4037, 3397, 806, 788, 1655, 3983, 3414, 1912, 3640, 1748, 2565, 711, 4118, 2770, 637, 3272, 786, 2386, 4187, 1404, 3265, 3568, 4467, 4107, 4636, 772, 3367, 194, 2932, 3315, 2906, 2275, 3974, 1312, 1062, 4949, 395, 4277, 4598, 1064, 4829, 3358, 2715, 2835, 4326, 4821, 2131, 46, 246, 1389, 3121, 1286, 941, 2582, 4801, 1257, 332, 2749, 2314, 3917, 959, 701, 2144, 498, 3091, 1810, 125, 2742, 2141, 4443, 3030, 1821, 1582, 4737, 4092, 3784, 4770, 3750, 2683, 3436, 9, 4889, 2442, 779, 2552, 4446, 2836, 2925, 3132, 3481, 1212, 1120, 1635, 2160, 1733, 1459, 3779, 1528, 1512, 2319, 4260, 4864, 1496, 4286, 3780, 4306, 4154, 137, 4197, 2579, 2861, 448, 927, 3415, 3492, 162, 1866, 2834, 752, 3234, 1150, 4050, 253, 20, 2461, 866, 3326, 183, 4633, 4387, 3195, 4592, 787, 3075, 391, 1998, 4965, 3057, 830, 3115, 1654, 869, 3682, 3533, 2526, 245, 601, 1309, 1075, 1368, 3826, 1289, 1135, 1161, 32, 1918, 209, 2588, 2667, 185, 1285, 2791, 2739, 4420, 3190, 312, 2069, 3607, 3874, 2847, 2518, 4479, 1224, 3406, 3246, 1986, 4308, 1537, 4926, 761, 3907, 3449, 3353, 450, 1956, 1493, 2225, 3012, 2520, 3407, 4266, 1326, 262, 307, 2095, 3104, 1434, 737, 4314, 4165, 1652, 1977, 892, 4023, 781, 739, 4159, 2085, 2755, 460, 2534, 1734, 657, 2629, 2865, 2202, 306, 3573, 4134, 1549, 360, 2311, 713, 518, 4619, 3382, 1642, 2545, 2544, 1226, 28, 910, 2293, 1607, 389, 1028, 1649, 4185, 158, 3037, 3256, 4250, 1826, 1835, 898, 1966, 3221, 143, 4339, 4538, 336, 4662, 2502, 2381, 1485, 2506, 2467, 3586, 2513, 313, 1362, 3829, 1916, 4476, 1942, 3902, 3331, 4, 192, 1911, 949, 4703, 859, 1184, 3495, 1178, 2005, 3284, 2348, 3446, 3801, 1858, 777, 630, 1302, 2718, 3153, 2477, 3830, 4105, 4047, 3776, 2377, 1000, 3643, 4831, 2355, 2375, 3581, 1420, 4013, 3213, 4439, 1165, 3092, 3191, 3142, 230, 523, 4578, 3299, 4951, 3464, 2573, 3867, 2456, 3720, 728, 1984, 1968, 1606, 2318, 3580, 1864, 3782, 1044, 321, 1505, 3107, 3395, 4398, 1447, 2528, 3654, 2790, 453, 2942, 547, 4357, 683, 693, 1059, 4820, 200, 857, 3476, 3634, 3081, 4939, 4751, 2981, 1335, 3054, 4293, 2802, 3935, 4044, 4781, 2860, 2429, 4100, 3955, 591, 2961, 1775, 3911, 2484, 252, 1556, 14, 850, 700, 3542, 3400, 4240, 1651, 1374, 1848, 1710, 176, 2222, 2901, 4533, 3689, 1237, 2740, 4735, 3374, 3520, 861, 206, 1094, 4383, 1904, 240, 3961, 4554, 1243, 1040, 403, 546, 1279, 3223, 480, 1453, 1027, 2905, 309, 4894, 3528, 3433, 3453, 3674, 2516, 4562, 439, 766, 4852, 4342, 3182, 3828, 463, 1430, 4042, 4915, 492, 1416, 1545, 2585, 1151, 4083, 4593, 2728, 327, 4969, 4330, 4548, 68, 562, 4746, 532, 822, 3697, 2650, 2354, 4857, 2157, 4202, 4992, 2195, 3383, 415, 157, 318, 4561, 1406, 3211, 3126, 525, 4853, 504, 2674, 2076, 566, 180, 2765, 414, 1173, 1928, 887, 187, 1333, 4835, 1553, 3119, 4917, 1058, 4484, 4483, 4381, 3323, 4546, 3424, 765, 2356, 2541, 4359, 4310, 3582}
	bt := setUpTreeOfPredefinedInt(numbers, config)
	assert.NotEmpty(t, bt)
	assert.Equal(t, n, bt.Size())
	for i, number := range numbers {
		assert.True(t, utils.GetFirstReturned(func() (bool, int64) { return bt.Find(number) }))
		err := bt.Delete(number)
		assert.NoError(t, err)
		assert.Equal(t, n-int64(i)-1, bt.Size())
		found, _ := bt.Find(number)
		assert.False(t, found)
		valid := validateTree(bt, t)
		assert.NoError(t, valid)
		if valid != nil {
			break
		}
	}
}
