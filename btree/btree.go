package btree

import (
	"fmt"
	"math"
	"reflect"
	"slices"

	"github.com/mylux/bsistent/constants"
	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/utils"
	"github.com/samber/lo"
)

type Btree[DataType any] struct {
	size        int64
	grade       int
	itemSize    int64
	storagePath string
	root        interfaces.Page[DataType]
	persistence interfaces.Persistence[DataType]
	changed     map[int64]interfaces.Page[DataType]
	rootChanged bool
	minItems    int
	minChildren int
}

func (b *Btree[DataType]) Add(value DataType) {
	if item := item[DataType](b.itemSize).Load(value); !item.IsEmpty() {
		leaf := b.findLeafFor(b.root, item)
		b.addItemToPage(leaf, item)
		b.size++
		b.persist()
	}
}

func (b *Btree[DataType]) Delete(partialItem DataType) bool {
	if destPage, index := b.find(partialItem); destPage != nil {
		if b.removeFromPage(index, destPage) {
			b.size--
			b.persist()
			return true
		}
	}
	return false
}

func (b *Btree[DataType]) Find(partialItem DataType) (bool, DataType) {
	destPage, index := b.find(partialItem)
	if destPage != nil {
		return true, destPage.Item(index).Content()
	}
	return false, reflect.Zero(reflect.TypeFor[DataType]()).Interface().(DataType)
}

func (b *Btree[DataType]) IsEmpty() bool {
	return b.root.Size() == 0
}

func (b *Btree[DataType]) LoadOffsets(offsets []int64) []interfaces.Page[DataType] {
	c := make([]interfaces.Page[DataType], len(offsets))
	for i, o := range offsets {
		c[i] = b.persistence.Load(o)
	}
	return c
}

func (b *Btree[DataType]) LoadPageChildren(page interfaces.Page[DataType]) interfaces.PageChildren[DataType] {
	children := page.Children()
	if !children.IsFetched() {
		return NewPageChildren(page, b.LoadOffsets(children.Offsets()))
	}
	return page.Children()
}

func (b *Btree[DataType]) PageNeedsAdjustment(page interfaces.Page[DataType]) bool {
	return page.Size() < b.minItems && page.NotSame(b.Root())
}

func (b *Btree[DataType]) PageCanGiveItem(page interfaces.Page[DataType]) bool {
	return page.Size() > b.minItems || page.Same(b.Root()) && !page.IsEmpty()
}

func (b *Btree[DataType]) PageIsValid(page interfaces.Page[DataType]) bool {
	pSize := page.Size()
	root := b.Root()
	cSize := page.Children().Size()
	return (pSize >= b.minItems || page.Same(root)) && (slices.Contains([]int{0, pSize + 1}, cSize))
}

func (b *Btree[DataType]) Save(value DataType) {
	b.Add(value)
}

func (b *Btree[DataType]) Size() int64 {
	return b.size
}

func (b *Btree[DataType]) Root() interfaces.Page[DataType] {
	return b.root
}

func (b *Btree[DataType]) StoragePath() string {
	return b.storagePath
}

func (b *Btree[DataType]) String() string {
	return b.genPagePrettyPrint(b.root, "")
}

func (b *Btree[DataType]) addChildToPage(page interfaces.Page[DataType], child interfaces.Page[DataType]) {
	page.AddChild(child)
	b.taintPages(page)
}

func (b *Btree[DataType]) addItemToPage(page interfaces.Page[DataType], item interfaces.Item[DataType], child ...interfaces.Page[DataType]) {
	page.Add(item)
	if len(child) > 0 && child[0] != nil {
		b.addChildToPage(page, child[0])
	}
	if page.IsFull() {
		b.splitPage(page)
	}
	b.taintPages(page)
}

func btree[DataType any](
	grade int,
	itemSize int64,
	storagePath string,
	reset bool,
	p interfaces.Persistence[DataType]) *Btree[DataType] {

	if reset {
		p.Reset()
	}
	size := utils.ReturnOrPanic[int64](p.LoadSize)
	minChildren := int(math.Ceil(float64(grade) / 2))
	minItems := minChildren - 1

	return &Btree[DataType]{
		grade:       grade,
		itemSize:    itemSize,
		storagePath: storagePath,
		persistence: p,
		changed:     map[int64]interfaces.Page[DataType]{},
		root:        utils.ReturnOrPanic[interfaces.Page[DataType]](p.LoadRoot),
		size:        size,
		minItems:    minItems,
		minChildren: minChildren,
	}
}

func (b *Btree[DataType]) determineItemToGive(selected interfaces.Page[DataType], other interfaces.Page[DataType], siblingsDelta interfaces.PageDelta) (int, int) {
	// Only works between siblings or child to parent.
	utils.PanicIf(siblingsDelta.IsError(), "could not determine item to give, because there is no delta")
	lastItem := other.Items().Last()
	slot := selected.Items().SlotFor(lastItem)
	childIndexToGive := -1
	if !selected.IsLeaf() {
		childIndexToGive = slot
	}
	itemIndexToGive := max(0, selected.Items().SlotFor(lastItem)-1)
	return itemIndexToGive, childIndexToGive
}

func (b *Btree[DataType]) findLeafFor(page interfaces.Page[DataType], item interfaces.Item[DataType]) interfaces.Page[DataType] {
	if page.IsLeaf() {
		return page
	} else {
		return b.findLeafFor(b.LoadPageChildren(page).ChildFor(item), item)
	}
}

func (b *Btree[DataType]) FindEdgeItem(page interfaces.Page[DataType], left ...bool) (interfaces.Page[DataType], int) {
	isLeft := utils.CoalesceBool(left)
	index := lo.Ternary(isLeft, page.Size()-1, 0)
	if page.IsLeaf() {
		return page, index
	} else {
		nextChildren := b.LoadPageChildren(page)
		nextChild := lo.Ternary(isLeft, nextChildren.Last(), nextChildren.First())
		return b.FindEdgeItem(nextChild, isLeft)
	}
}

func (b *Btree[DataType]) find(partialItem DataType) (interfaces.Page[DataType], int) {
	currentPage := b.Root()
	item := item[DataType](b.itemSize).Load(partialItem)
	for currentPage != nil {
		slot := currentPage.Items().SlotFor(item)
		if previousItemPos := slot - 1; slot > 0 {
			if res, err := currentPage.Item(previousItemPos).Compare(item); err == nil && res == 0 {
				return currentPage, previousItemPos
			}
		}
		currentPage = b.LoadPageChildren(currentPage).Nth(slot)
	}
	return nil, 0
}

func (b *Btree[DataType]) genPagePrettyPrint(p interfaces.Page[DataType], prefix string) string {
	var res string
	if p != nil {
		res = fmt.Sprintln(prefix, constants.PrintPrefix, p)
		newPrefix := prefix + constants.PrintSpacing
		children := b.LoadPageChildren(p)
		p = nil
		for _, c := range children.All() {
			res = res + b.genPagePrettyPrint(c, newPrefix)
		}
	}
	return res
}

func (b *Btree[DataType]) mergePages(p1 interfaces.Page[DataType], p2 interfaces.Page[DataType]) {
	parentPage := p1.Parent()
	parentSlot := p1.ParentSlotFor(p2)
	if !p2.IsLeaf() {
		p2.GiveChildren(p1, p2.Delta(p1).IsLeft())
	}
	b.pageGiveItems(p2, p1, make([]int, p2.Size())...)
	parentPage.RemoveChild(p2)
	b.safeGiveItem(parentPage, parentSlot, p1)
	b.taintPages(p1, parentPage)
}

func (b *Btree[DataType]) newPage(parent interfaces.Page[DataType]) interfaces.Page[DataType] {
	p, _ := b.persistence.NewPage()
	p.Parent(parent)
	b.taintPages(p)
	return p
}

func (b *Btree[DataType]) newRoot(page interfaces.Page[DataType]) interfaces.Page[DataType] {
	ppg := b.newPage(nil)
	b.addChildToPage(ppg, page)
	return b.setRoot(ppg)
}

func (b *Btree[DataType]) pageDeleteItem(page interfaces.Page[DataType], index int) interfaces.Item[DataType] {
	if r := page.Items().Pop(index); r != nil {
		b.taintPages(page)
		return r
	}
	return nil
}

func (b *Btree[DataType]) pageGiveItems(from interfaces.Page[DataType], to interfaces.Page[DataType], indexes ...int) {
	for _, index := range indexes {
		from.GiveItem(index, to)
	}
	b.taintPages(from, to)
}

func (b *Btree[DataType]) persist() {
	for _, p := range b.changed {
		if p.Size() > 0 {
			utils.PanicOnError(func() error { return b.persistence.Save(p) })
			if b.rootChanged {
				utils.PanicOnError(b.persistRoot)
			}
			p.Children().Unload()
		}
		delete(b.changed, p.Offset())
	}
	utils.PanicOnError(b.persistSize)
}

func (b *Btree[DataType]) persistRoot() error {
	err := b.persistence.SaveRootReference(b.root.Offset())
	if err == nil {
		b.rootChanged = false
	}
	return err
}

func (b *Btree[DataType]) persistSize() error {
	return b.persistence.SaveSize(b.Size())
}

func (b *Btree[DataType]) fixPage(page interfaces.Page[DataType]) {
	if page != nil {
		sibling := b.selectSibling(page)
		if sibling.Size() > b.minItems {
			b.transferSelectedSiblingItem(sibling, page)
		} else {
			b.mergePages(page, sibling)
			if parent := page.Parent(); parent != nil && !b.PageIsValid(parent) {
				b.fixPage(parent)
			}
		}
	}
}

func (b *Btree[DataType]) maneuverItem(page interfaces.Page[DataType], index int) (interfaces.Page[DataType], int) {
	if !page.IsLeaf() {
		children := b.LoadPageChildren(page).Pick(index, index+1).BySize()
		biggestChild := children.First()
		if b.PageCanGiveItem(biggestChild) || !biggestChild.IsLeaf() {
			newIndex := index
			IsBiggestChildLeft := biggestChild.Delta(children.Nth(1)).IsLeft()
			leaf, edgeItemIndex := b.FindEdgeItem(biggestChild, IsBiggestChildLeft)
			if edgeItemIndex > 0 { //This is the rightmost item from the left deepest child
				newIndex++ // The original item is shifted right
			}
			b.pageGiveItems(leaf, page, edgeItemIndex)
			b.pageGiveItems(page, leaf, newIndex)
			return leaf, edgeItemIndex
		} else {
			b.mergePages(biggestChild, children.Nth(1))
			if b.PageNeedsAdjustment(page) {
				b.fixPage(page)
			}
			return biggestChild, biggestChild.Size() / 2
		}
	}
	return page, index
}

func (b *Btree[DataType]) removeFromPage(index int, page interfaces.Page[DataType]) bool {
	newPage, newIndex := b.maneuverItem(page, index)
	if result := b.pageDeleteItem(newPage, newIndex); result == nil {
		return false
	}
	if !b.PageIsValid(newPage) && newPage.NotSame(b.Root()) {
		b.fixPage(newPage)
	}

	return true
}

func (b *Btree[DataType]) safeGiveItem(from interfaces.Page[DataType], itemIndex int, to interfaces.Page[DataType]) {
	// this method assumes that left and right pages from from[itemIndex] were already merged
	b.pageGiveItems(from, to, itemIndex)
	if from.Same(b.Root()) && from.IsEmpty() {
		b.shrink()
	}
}

func (b *Btree[DataType]) selectSibling(p interfaces.Page[DataType]) interfaces.Page[DataType] {
	return b.LoadPageChildren(p.Parent()).Siblings(p).BySize().First()
}

func (b *Btree[DataType]) setRoot(page interfaces.Page[DataType]) interfaces.Page[DataType] {
	b.root = page
	b.rootChanged = true
	return b.root
}

func (b *Btree[DataType]) shrink() {
	b.setRoot(b.LoadPageChildren(b.root).First())
	b.root.ResetParent()
}

func (b *Btree[DataType]) splitPage(page interfaces.Page[DataType]) {
	left, right, middle := page.Items().Split()
	pivot := page.Item(middle)
	childrenLeft, childrenRight := b.LoadPageChildren(page).Split(middle)
	ppg := page.Parent()
	if ppg == nil {
		ppg = b.newRoot(page)
	}
	newPageRight := b.newPage(ppg)
	page.Items(left...)
	newPageRight.Items(right...)
	if !page.IsLeaf() {
		page.Children(childrenLeft)
		newPageRight.Children(childrenRight)
	}
	b.addItemToPage(ppg, pivot, newPageRight)

	if ppg.IsFull() {
		b.splitPage(ppg)
	}
}

func (b *Btree[DataType]) taintPages(pages ...interfaces.Page[DataType]) {
	for _, page := range pages {
		b.changed[page.Offset()] = page
	}
}

func (b *Btree[DataType]) transferSelectedSiblingItem(selectedSibling interfaces.Page[DataType], siblingToReceive interfaces.Page[DataType]) {
	var itemIndexToGivePR int
	parentPage := selectedSibling.Parent()
	delta := selectedSibling.Delta(siblingToReceive)
	itemIndexToGiveSP, childIndexToGive := b.determineItemToGive(selectedSibling, siblingToReceive, delta)
	b.pageGiveItems(selectedSibling, parentPage, itemIndexToGiveSP)
	itemIndexToGivePR = utils.Limit(parentPage.Children().LookUp(siblingToReceive), 0, parentPage.Size()-1)
	if !selectedSibling.IsLeaf() {
		selectedSibling.GiveChild(childIndexToGive, siblingToReceive, delta.IsLeft())
	}

	b.pageGiveItems(parentPage, siblingToReceive, itemIndexToGivePR)
}
