package btree

import (
	"fmt"

	"github.com/mylux/bsistent/interfaces"
)

const printPrefix = "|-- "
const printSpacing = "    "

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
		b.persist()
	}
}

func (b *Btree[DataType]) IsEmpty() bool {
	return b.root.Size() == 0
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

	root, err := p.LoadRoot()
	if err != nil {
		panic(err)
	}

	b := &Btree[DataType]{
		grade:       grade,
		itemSize:    itemSize,
		storagePath: storagePath,
		persistence: p,
		changed:     map[int64]interfaces.Page[DataType]{},
		root:        root,
	}

	return b
}

func (b *Btree[DataType]) findLeafFor(page interfaces.Page[DataType], item interfaces.Item[DataType]) interfaces.Page[DataType] {
	if page.IsLeaf() {
		return page
	} else {
		b.loadPageChildren(page)
		return b.findLeafFor(page.ChildFor(item), item)
	}
}

func (b *Btree[DataType]) genPagePrettyPrint(p interfaces.Page[DataType], prefix string) string {
	var res string
	if p != nil {
		res = fmt.Sprintln(prefix, printPrefix, p)
		newPrefix := prefix + printSpacing
		for _, c := range b.loadPageChildren(p) {
			res = res + b.genPagePrettyPrint(c, newPrefix)
		}
	}
	return res
}

func (b *Btree[DataType]) loadPageChildren(page interfaces.Page[DataType]) []interfaces.Page[DataType] {
	if loaded, offsets := page.GetChildrenStatus(); !loaded {
		c := make([]interfaces.Page[DataType], len(offsets))
		for i, o := range offsets {
			c[i] = b.persistence.Load(o)
		}
		page.Children(c)
		return c
	}
	return page.Children()
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
		err := b.persistence.Save(p)
		if err != nil {
			panic(err)
		}
		delete(b.changed, p.Offset())
		if b.rootChanged {
			err := b.persistRoot()
			if err != nil {
				panic(err)
			}
		}
	}
}

func (b *Btree[DataType]) persistRoot() error {
	err := b.persistence.SaveRootReference(b.root.Offset())
	if err != nil {
		return err
	}
	b.rootChanged = false
	return nil
}

func (b *Btree[DataType]) splitPage(page interfaces.Page[DataType]) {
	left, childrenLeft, right, childrenRight, pivot := page.Items().Split()
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
