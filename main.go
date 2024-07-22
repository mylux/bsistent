package main

import (
	"fmt"
	"time"

	"github.com/mylux/bsistent/btree"
	"github.com/mylux/bsistent/interfaces"
	"golang.org/x/exp/rand"
)

var fixedItems = []int64{167, 526, 3443, 876, 1520, 870, 4505, 2441, 2051, 462, 2844, 3170, 2565, 2751, 844, 137, 1699, 4507, 999, 398, 4306, 1702, 2779, 4998, 1121, 4230, 2898, 3995, 4357, 3890, 2781, 3514, 2306, 3479, 4383, 1984, 3205, 2217, 148, 1264, 3153, 541, 4564, 1658, 1529, 2345, 1227, 584, 407, 2668, 159, 1290, 942, 2804, 1356, 3824, 4339, 3390, 3486, 2709, 1111, 2043, 1908, 4802, 794, 4167, 1864, 2027, 4147, 475, 2623, 3754, 2371, 4904, 12, 3804, 2776, 3250, 3482, 3846, 3869, 2198, 2772, 2090, 350, 4869, 3393, 4470, 2211, 2743, 2904, 1456, 2797, 2901, 3008, 1187, 1116, 3117, 1642, 857, 3702, 3778, 194, 4069, 177, 2522, 547, 652, 117, 3175, 3680, 3308, 1436, 4740, 1125, 333, 2004, 3601, 582, 3327, 72, 3646, 3142, 3507, 1064, 4731, 1139, 1844, 4160, 4785, 2240, 1930, 1955, 3764, 323, 1431, 898, 605, 4262, 2916, 2900, 4886, 4266, 3074, 4320, 3872, 3021, 4861, 1126, 988, 6, 3996, 590, 2742, 1159, 506, 4295, 1383, 2865, 4310, 917, 691, 2305, 1069, 4009, 3903, 2939, 1221, 1479, 1940, 1162, 1771, 3966, 1590, 2868, 838, 998, 4168, 2769, 181, 974, 1888, 1909, 3424, 1532, 954, 3855, 2246, 3112, 2031, 4773, 2442, 3093, 1866, 1954, 2942, 1800, 3269, 171, 910, 3599, 1933, 1168, 4925, 389, 2597, 3639, 1813, 1555, 4432, 533, 728, 236, 204, 3017, 3976, 3519, 451, 2350, 3682, 1755, 3735, 1779, 3913, 3698, 3090, 2993, 829, 1294, 1052, 1477, 1019, 2033, 2214, 937, 4136, 1072, 943, 3454, 432, 3165, 4885, 2374, 3904, 2881, 653, 3915, 1344, 4085, 1419, 3415, 538, 1226, 1972, 1944, 3070, 913, 2944, 1391, 1017, 2472, 4841, 2918, 1180, 3430, 126, 2951, 618, 746, 1022, 2777, 2501, 2479, 1714, 404, 3089, 1206, 2011, 95, 4764, 403, 2309, 3260, 2608, 2622, 4462, 3345, 2261, 935, 1087, 2697, 3102, 3409, 4750, 994, 906, 74, 2509, 2431, 3182, 813, 1393, 507, 18, 1033, 2234, 1594, 801, 19, 249, 4298, 3145, 4521, 3939, 2955, 4208, 3727, 2245, 3176, 597, 55, 258, 905, 1468, 4358, 2457, 2001, 4683, 1522, 2487, 515, 679, 4528, 1188, 967, 2375, 4, 273, 3065, 2826, 4051, 3753, 1112, 1232, 1418, 212, 3104, 891, 13, 1026, 4942, 4909, 4423, 4238, 1329, 4634, 227, 989, 436, 1863, 3412, 997, 2159, 1337, 2235, 3505, 3480, 4953, 2896, 1381, 3944, 4883, 2671, 2163, 29, 3055, 2382, 1561, 672, 3509, 3144, 3834, 1770, 3458, 4767, 3305, 3859, 4561, 2321, 1183, 2057, 2507, 4971, 3438, 1444, 1838, 2000, 2394, 4950, 973, 4128, 3960, 28, 2316, 4452, 3522, 4790, 2328, 2449, 1845, 4753, 2661, 3355, 4417, 3358, 3560, 1385, 2421, 1968, 812, 521, 3750, 3563, 3576, 3379, 3010, 1286, 2594, 3800, 3493, 1308, 846, 3548, 4460, 4805, 4718, 463, 2703, 2413, 2046, 1571, 3202, 532, 3866, 4378, 1082, 4384, 4173, 2484, 2355, 1992, 3511, 1, 2657, 2323, 2527, 4277, 907, 1597, 1053, 747, 2108, 2035, 3473, 1349, 2086, 76, 2704, 1234, 2091, 1284, 3898, 2318, 2774, 2434, 1707, 2792, 1377, 4042, 4555, 1852, 821, 1367, 1186, 3222, 4435, 1193, 944, 2912, 1423, 4892, 459, 2545, 3621, 4413, 2179, 4864, 2010, 1417, 4678}

func generateUniqueInts(size int64) []int64 {
	rand.Seed(uint64(time.Now().UnixNano()))
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

func countPage(p interfaces.Page[int64]) int {
	var res int = 0
	children := p.Children()
	if children.IsFetched() {
		if !p.IsLeaf() {
			for i := range p.Size() + 1 {
				res += countPage(p.Child(i))
			}
		}
		res += p.Size()
	}
	return res
}

func count(b *btree.Btree[int64]) int {
	return countPage(b.Root())
}

func main() {
	var strT string
	var b *btree.Btree[int64] = btree.Configuration[int64]().Grade(5).ItemSize(8).CacheSize(40).Reset().Make()
	if b.IsEmpty() {
		elements := generateUniqueInts(500)
		// elements := fixedItems //[:23]
		fmt.Printf("Elements: %v\n", elements)
		fmt.Printf("Created a Btree object with size=%d and storing data in %s file\n", b.Size(), b.StoragePath())
		fmt.Printf("Add %d random item(s) into the tree\n", len(elements))
		for _, e := range elements {
			//fmt.Printf("Add an element into the tree: %d\n", e)
			b.Add(e)
		}
		fmt.Println("Finished adding items")
		fmt.Printf("Item count %d vs %d vs %d\n", count(b), len(elements), b.Size())
		strT = fmt.Sprintf("%v\n", b)
	} else {
		fmt.Println("Btree is not empty")
		strT = fmt.Sprintf("%v\n", b)
		c := count(b)
		fmt.Printf("Item count %d vs %d\n", c, b.Size())

		if b.Size() > 0 && c < b.Root().Size() {
			fmt.Println("Tree seems to be partially loaded")
		}
	}

	fmt.Printf("%v\n", strT)
}
