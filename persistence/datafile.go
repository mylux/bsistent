package persistence

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/mylux/bsistent/cache"
	"github.com/mylux/bsistent/interfaces"
	"github.com/mylux/bsistent/utils"
)

const (
	initialOffset     int64 = 16
	sizeOffset        int64 = 8
	rootPageRefOffset int64 = 0
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
	cache           *cache.Cache[DataType]
}

func New[DataType any](config *interfaces.PersistenceConfig[DataType]) interfaces.Persistence[DataType] {
	zeroPg := utils.ReturnOrPanic[[]byte](func() ([]byte, error) {
		return generateEncodedZeroPage(config.PageConstructor(0).Capacity(), config.ItemConstructor().Capacity())
	})

	r := &DataFileBtreePersistence[DataType]{
		path:            config.Path,
		pageSize:        int64(len(zeroPg)),
		fd:              openFile(config.Path),
		pageConstructor: config.PageConstructor,
		itemConstructor: config.ItemConstructor,
		lastPageOffset:  initialOffset,
		cache: cache.New(&cache.Config[DataType]{
			Limit:         config.CacheSize,
			PageGenerator: func() interfaces.Page[DataType] { return config.PageConstructor(0) },
		}),
	}
	utils.PanicOnError(func() error { return loadTreeSize(r) })
	utils.PanicOnError(func() error { return loadRootPageReference(r) })

	return r
}

func (d *DataFileBtreePersistence[DataType]) Load(offset int64, children ...bool) interfaces.Page[DataType] {
	if pCache := d.loadPageFromCache(offset); pCache != nil {
		return pCache
	}
	defer d.Unlock()
	if !d.locked {
		d.Lock()
		b := utils.ReturnOrPanic(func() ([]byte, error) { return d.readPageBytes(offset) })
		sp := utils.ReturnOrPanic(func() (*SerializedPage, error) { return hydratePage(b) })
		items := make([]interfaces.Item[DataType], 0, sp.Capacity)
		r := d.pageConstructor(offset)
		for _, si := range sp.Items {
			item := d.itemConstructor()
			if !si.Empty {
				var itemValue DataType
				utils.PanicOnError(func() error { return decode(si.Content, &itemValue) })
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
					r.Children().Put(c)
				}
			}
		}
		d.cache.Save(r)
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
	b, err := d.readBytes(rootPageRefOffset, int64(unsafe.Sizeof(rootPageRefOffset)))
	if err != nil {
		return -1, err
	}
	err = decode(b, &r)
	if err != nil {
		return -1, err
	}
	d.rootOffset = r
	return r, nil
}

func (d *DataFileBtreePersistence[DataType]) LoadSize() (int64, error) {
	var size int64
	b, err := d.readBytes(sizeOffset, int64(unsafe.Sizeof(sizeOffset)))
	if err != nil {
		return -1, err
	}
	err = decode(b, &size)
	return size, err
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
	d.rootOffset = 0
	d.lastPageOffset = initialOffset
	utils.PanicOnError(func() error { return d.fd.Truncate(0) })
}

func (d *DataFileBtreePersistence[DataType]) Save(p interfaces.Page[DataType]) error {
	defer d.Unlock()
	if !d.locked {
		d.Lock()
		b, err := serializePage[DataType](p)
		if err != nil {
			return err
		}
		err = d.savePageBytes(b, p.Offset())
		if err == nil {
			d.cache.Update(p)
		}
		return err
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

func (d *DataFileBtreePersistence[DataType]) SaveSize(size int64) error {
	s, err := encode(size)
	if err != nil {
		return err
	}
	_, err = d.saveBytes(s, sizeOffset)
	return err
}

func (d *DataFileBtreePersistence[DataType]) Unlock() {
	d.locked = false
}

func (d *DataFileBtreePersistence[DataType]) genNewOffset() int64 {
	d.lastPageOffset += (d.pageSize)
	return d.lastPageOffset
}

func (d *DataFileBtreePersistence[DataType]) loadPageFromCache(offset int64) interfaces.Page[DataType] {
	return d.cache.Load(offset)
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

func loadTreeSize[DataType any](d *DataFileBtreePersistence[DataType]) error {
	_, err := d.LoadSize()
	if err != nil {
		err = d.SaveSize(0)
	}
	return err
}

func loadRootPageReference[DataType any](d *DataFileBtreePersistence[DataType]) error {
	rootRef, err := d.LoadReference()
	if err != nil || rootRef == 0 {
		err = d.SaveRootReference(0)
	}
	return err
}

func openFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			newFile := utils.ReturnOrPanic[*os.File](func() (*os.File, error) { return os.Create(path) })
			newFile.Close()
			file = utils.ReturnOrPanic[*os.File](func() (*os.File, error) { return os.OpenFile(path, os.O_RDWR, 0666) })
		} else {
			panic(err)
		}
	}
	return file
}
