package persistence

import (
	"bytes"
	"fmt"
	"os"

	"github.com/mylux/bsistent/interfaces"
)

const (
	initialOffset     = 8
	rootPageRefOffset = 0
)

type DataFileBtreePersistence[DataType any] struct {
	path            string
	rootOffset      int64
	lastPageOffset  int64
	pageSize        int64
	locked          bool
	fd              *os.File
	pageConstructor func(int64) interfaces.Page[DataType]
	itemConstructor func() interfaces.Item[DataType]
}

func New[DataType any](
	path string,
	pc func(int64) interfaces.Page[DataType],
	ic func() interfaces.Item[DataType]) interfaces.Persistence[DataType] {
	zeroPg, err := generateEncodedZeroPage(pc(0).Capacity(), ic().Capacity())
	if err != nil {
		panic(err)
	}
	r := &DataFileBtreePersistence[DataType]{
		path:            path,
		pageSize:        int64(len(zeroPg)),
		fd:              openFile(path),
		pageConstructor: pc,
		itemConstructor: ic,
		lastPageOffset:  initialOffset,
	}
	rootRef, err := r.LoadReference()
	if err != nil || rootRef == 0 {
		err = r.SaveRootReference(0)
	}

	if err != nil {
		panic(err)
	}
	return r
}

func (d *DataFileBtreePersistence[DataType]) Load(offset int64, children ...bool) interfaces.Page[DataType] {
	defer d.Unlock()
	if !d.locked {
		d.Lock()
		b, err := d.readPageBytes(offset)
		if err != nil {
			panic(err)
		}
		sp, err := hydratePage(b)
		if err != nil {
			panic(err)
		}
		items := make([]interfaces.Item[DataType], 0, sp.Capacity)
		r := d.pageConstructor(offset)
		for _, i := range sp.Items {
			item := d.itemConstructor()
			si, err := hydrateItem(i)
			if err != nil {
				panic(err)
			}
			if !si.Empty {
				var itemValue DataType
				err = decode(bytes.NewReader(si.Content), &itemValue)
				if err != nil {
					panic(err)
				}
				item.Load(itemValue)
				items = append(items, item)
			}
		}
		r.Items(items...)
		for _, c := range sp.Children {
			if c > 0 {
				if len(children) > 0 && children[0] {
					r.AddChild(d.Load(c))
				} else {
					r.AppendChildOffset(c)
				}
			}
		}
		return r
	}
	return nil
}

func (d *DataFileBtreePersistence[DataType]) LoadRoot() (interfaces.Page[DataType], error) {
	if d.rootOffset > 0 {
		return d.Load(d.rootOffset), nil
	}
	p, err := d.NewPage(true)
	d.rootOffset = int64(p.Offset())
	d.SaveRootReference(d.rootOffset)
	return p, err
}

func (d *DataFileBtreePersistence[DataType]) LoadReference() (int64, error) {
	var r int64
	b, err := d.readBytes(rootPageRefOffset, initialOffset)
	buf := bytes.NewReader(b)
	if err != nil {
		return -1, err
	}
	err = decode(buf, &r)
	if err != nil {
		return -1, err
	}
	d.rootOffset = r
	return r, nil
}

func (d *DataFileBtreePersistence[DataType]) Lock() {
	d.locked = true
}

func (d *DataFileBtreePersistence[DataType]) NewPage(first ...bool) (interfaces.Page[DataType], error) {
	defer d.Unlock()
	if !d.locked {
		d.Lock()
		err := d.reservePage(first...)
		return d.pageConstructor(d.lastPageOffset), err
	}
	return nil, fmt.Errorf("data file temporarily locked")
}

func (d *DataFileBtreePersistence[DataType]) Reset() {
	err := d.fd.Truncate(0)
	if err != nil {
		panic(err)
	}
}

func (d *DataFileBtreePersistence[DataType]) Save(p interfaces.Page[DataType]) error {
	defer d.Unlock()
	if !d.locked {
		d.Lock()
		b, err := serializePage[DataType](p)
		if err != nil {
			return err
		}
		return d.savePageBytes(b, p.Offset())
	}
	return nil
}

func (d *DataFileBtreePersistence[DataType]) SaveRootReference(offset int64) error {
	o, err := encode(offset)
	if err != nil {
		return err
	}
	_, err = d.saveBytes(o, rootPageRefOffset)
	return err
}

func (d *DataFileBtreePersistence[DataType]) Unlock() {
	d.locked = false
}

func (d *DataFileBtreePersistence[DataType]) genNewOffset() int64 {
	d.lastPageOffset += (d.pageSize)
	return d.lastPageOffset
}

func (d *DataFileBtreePersistence[DataType]) reservePage(first ...bool) error {
	if len(first) > 0 && first[0] {
		return d.savePageBytes(make([]byte, d.pageSize), d.lastPageOffset)
	}
	return d.savePageBytes(make([]byte, d.pageSize), d.genNewOffset())
}

func (d *DataFileBtreePersistence[DataType]) saveBytes(b []byte, offset int64) (int, error) {
	return d.fd.WriteAt(b, int64(offset))
}

func (d *DataFileBtreePersistence[DataType]) savePageBytes(b []byte, offset int64) error {
	r, err := d.saveBytes(b, offset)
	if int64(r) < d.pageSize && err == nil {
		return fmt.Errorf("expected to write %d bytes, but only %d were written", d.pageSize, r)
	}
	return err
}

func (d *DataFileBtreePersistence[DataType]) readBytes(offset int64, size int64) ([]byte, error) {
	b := make([]byte, size)
	_, err := d.fd.ReadAt(b, int64(offset))
	return b, err
}

func (d *DataFileBtreePersistence[DataType]) readPageBytes(offset int64) ([]byte, error) {
	b, err := d.readBytes(offset, d.pageSize)
	return b, err
}

func openFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			newFile, createErr := os.Create(path)
			if createErr != nil {
				panic(createErr)
			}
			newFile.Close()
			file, err = os.OpenFile(path, os.O_RDWR, 0666)
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
	return file
}
