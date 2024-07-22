package btree

import (
	"fmt"
	"reflect"

	"github.com/mylux/bsistent/constants"
	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/utils"
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
}

func (b *Btree[DataType]) Add(value DataType) {
	if item := item[DataType](b.itemSize).Load(value); !item.IsEmpty() {
		leaf := b.findLeafFor(b.root, item)
		b.addItemToPage(leaf, item)
		b.size++
		b.persist()
	}
}

func (b *Btree[DataType]) Find(partialItem DataType) (bool, DataType) {
	currentPage := b.Root()
	item := item[DataType](b.itemSize).Load(partialItem)
	for currentPage != nil {
		slot := currentPage.Items().SlotFor(item)
		if slot > 0 {
			previousItem := currentPage.Item(slot - 1)
			if res, err := previousItem.Compare(item); err == nil && res == 0 {
				return true, previousItem.Content()
			}
		}
		currentPage = b.LoadPageChildren(currentPage).Nth(slot)
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
	b.changed[page.Offset()] = page
}

func (b *Btree[DataType]) addItemToPage(page interfaces.Page[DataType], item interfaces.Item[DataType], child ...interfaces.Page[DataType]) {
	page.Add(item)
	if len(child) > 0 && child[0] != nil {
		b.addChildToPage(page, child[0])
	}
	if page.IsFull() {
		b.splitPage(page)
	}
	b.changed[page.Offset()] = page
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

	return &Btree[DataType]{
		grade:       grade,
		itemSize:    itemSize,
		storagePath: storagePath,
		persistence: p,
		changed:     map[int64]interfaces.Page[DataType]{},
		root:        utils.ReturnOrPanic[interfaces.Page[DataType]](p.LoadRoot),
		size:        utils.ReturnOrPanic[int64](p.LoadSize),
	}
}

func (b *Btree[DataType]) findLeafFor(page interfaces.Page[DataType], item interfaces.Item[DataType]) interfaces.Page[DataType] {
	if page.IsLeaf() {
		return page
	} else {
		return b.findLeafFor(b.LoadPageChildren(page).ChildFor(item), item)
	}
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

func (b *Btree[DataType]) newPage(parent interfaces.Page[DataType]) interfaces.Page[DataType] {
	p, _ := b.persistence.NewPage()
	p.Parent(parent)
	b.changed[p.Offset()] = p
	return p
}

func (b *Btree[DataType]) newRoot(page interfaces.Page[DataType]) interfaces.Page[DataType] {
	ppg := b.newPage(nil)
	b.addChildToPage(ppg, page)
	b.root = ppg
	b.rootChanged = true
	return b.root
}

func (b *Btree[DataType]) persist() {
	for _, p := range b.changed {
		utils.PanicOnError(func() error { return b.persistence.Save(p) })
		delete(b.changed, p.Offset())
		if b.rootChanged {
			utils.PanicOnError(b.persistRoot)
		}
		p.Children().Unload()
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
