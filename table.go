package filecache

import (
	"io/ioutil"
	"os"
	"sync"
	"time"
)

type CacheTable struct {
	mutex              sync.RWMutex
	parent             *Cache
	name               string
	basePath           string
	expiryTime         time.Duration
	toBytes            func(interface{}) []byte
	fromBytes          func([]byte) interface{}
	startupOptions     int
	diskExpiryTime     time.Duration
	diskExpiryInterval time.Duration
	diskExpiryTimer    *time.Timer
	persistQueue       chan persistEntry
	items              map[string]*CacheItem
	started            bool
	cleanupTimer       *time.Timer
	cleanupInterval    time.Duration
	dataLoader         CacheDataLoader
	addItem            CacheItemCallback
	deleteItem         CacheItemCallback
}

func (table *CacheTable) start() error {
	table.basePath = table.parent.cacheDir + PathSeparator + table.name

	err := os.MkdirAll(table.basePath, 0777)
	if err != nil {
		return err
	}

	// The background persistence channel
	table.started = true
	go func() {
		for table.started {
			e := <-table.persistQueue
			table.persist(e)
		}
	}()

	// Startup options.
	// Note we only start the disk expiry timer as the default as the other options will
	// start it when they complete.
	// The methods are called in a go routine so the application isn't held up whilst the
	// cleanup is being performed
	switch table.startupOptions {
	case FlushCacheOnStart:
		go table.FlushDisk()
	case ExpireCacheOnStart:
		go table.ExpireDisk()
	case LoadCacheOnStart:
		go table.loadCache(table.expiryTime)
	case LoadEntireCacheOnStart:
		go table.loadCache(0)
	default:
		table.startDiskExpiryTimer()
	}

	return nil
}

func (table *CacheTable) stop() {
	if table.started {
		table.stopDiskExpiryTimer()
		table.started = false
	}
}

type persistEntry struct {
	key string
	val []byte
}

func (table *CacheTable) persist(e persistEntry) {
	dir, fileName := table.getPath(e.key)

	_ = os.MkdirAll(dir, 0777)

	_ = ioutil.WriteFile(dir+PathSeparator+fileName, e.val, 0655)
}

// dataLoader used by the memory cache to read from disk when an entry is not on disk
func (table *CacheTable) diskLoader(key string) *CacheItem {
	file, err := os.Open(table.getFilePath(key))
	if err != nil {
		return nil
	}
	defer file.Close()

	b, err := ioutil.ReadAll(file)
	if err != nil {
		return nil
	}

	info, err := file.Stat()
	if err != nil {
		return nil
	}

	val := table.fromBytes(b)
	if val != nil {
		return NewCreatedCacheItem(key, table.expiryTime, val, info.ModTime())
	}

	return nil
}

// Count returns how many items are in memory
func (table *CacheTable) Count() int {
	table.mutex.RLock()
	defer table.mutex.RUnlock()
	return len(table.items)
}

// Foreach calls a CacheItemWalker for each key,value in memory
func (table *CacheTable) Foreach(f CacheItemWalker) {
	table.mutex.RLock()
	defer table.mutex.RUnlock()

	for k, v := range table.items {
		f(k, v)
	}
}

func (table *CacheTable) ForeachDisk(f CacheItemWalker) {
	table.mutex.RLock()
	defer table.mutex.RUnlock()

	_ = table.walk(func(key, path string, info os.FileInfo, err error) error {
		f(key, NewCreatedCacheItem(key, table.expiryTime, nil, info.ModTime()))
		return nil
	})

}

func (table *CacheTable) add(item *CacheItem) *CacheItem {
	// Careful: do not run this method unless the table-mutex is locked!
	// It will unlock it for the caller before running the callbacks and checks
	table.items[item.key] = item

	// Cache values so we don't keep blocking the mutex.
	expDur := table.cleanupInterval
	addItem := table.addItem
	table.mutex.Unlock()

	if addItem != nil {
		addItem(item)
	}

	// If we haven't set up any expiration check timer or found a more imminent item.
	if item.lifeSpan > 0 && (expDur == 0 || item.lifeSpan < expDur) {
		table.expireMemory()
	}

	b := table.toBytes(item.data)
	if b != nil {
		table.persistQueue <- persistEntry{item.key, b}
	}

	return item
}

// Add adds a key/value pair to the cache using the default expiry time for this table.
// This returns the CacheItem just added or nil if there was an error, usually the key is invalid
// or data is nil
func (table *CacheTable) Add(key string, data interface{}) *CacheItem {
	return table.AddExpiry(key, table.expiryTime, data)
}

// AddExpiry adds a key/value pair with the specified lifeSpan.
// This returns the CacheItem just added or nil if there was an error, usually the key is invalid
// the lifeSpan is negative or data is nil
func (table *CacheTable) AddExpiry(key string, lifeSpan time.Duration, data interface{}) *CacheItem {
	item := NewCacheItem(key, lifeSpan, data)
	if !item.IsValid() {
		return nil
	}

	// Add item to cache.
	table.mutex.Lock()
	return table.add(item)
}

// NotFoundAdd will add a key, value pair to the cache only if the key does not already exist either in memory or disk.
func (table *CacheTable) NotFoundAdd(key string, data interface{}) bool {
	return table.NotFoundAddExpiry(key, table.expiryTime, data)
}

// NotFoundAddExpiry will add a key, value pair to the cache only if the key does not already exist either in memory or disk.
func (table *CacheTable) NotFoundAddExpiry(key string, lifeSpan time.Duration, data interface{}) bool {
	table.mutex.Lock()

	_, ok := table.items[key]

	if !ok {
		_, err := os.Stat(table.getFilePath(key))
		ok = !os.IsNotExist(err)
	}

	if ok {
		table.mutex.Unlock()
		return false
	}

	return table.add(NewCacheItem(key, lifeSpan, data)) != nil
}

func (table *CacheTable) delete(key string) {
	r, ok := table.items[key]
	if !ok {
		return
	}

	// No callbacks then just delete it
	if table.deleteItem == nil && r.aboutToExpire == nil {
		delete(table.items, key)
		return
	}

	// Call callbacks outside of the lock so unlock then on defer relock & then do the actual delete
	table.mutex.Unlock()
	defer func() {
		table.mutex.Lock()
		delete(table.items, key)
	}()

	if table.deleteItem != nil {
		table.deleteItem(r)
	}

	if r.aboutToExpire != nil {
		r.aboutToExpire(key)
	}
}

// DeleteFromMemoryAndDisk deletes an item from the cache. Unlike DeleteFromMemory this will also delete it from the disk.
func (table *CacheTable) DeleteFromMemoryAndDisk(key string) {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	table.delete(key)
	_ = os.Remove(table.getFilePath(key))
}

// Delete an item from memory only. The entry on disk is kept
func (table *CacheTable) DeleteFromMemory(key string) {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	table.delete(key)
}

// Exists returns whether an item exists in the cache, either in memory or on disk.
// Unlike the Get method Exists neither tries to fetch data via the dataLoader callback nor does it
// keep the item alive in the cache.
func (table *CacheTable) Exists(key string) bool {
	table.mutex.RLock()
	defer table.mutex.RUnlock()
	_, ok := table.items[key]

	if !ok {
		_, err := os.Stat(table.getFilePath(key))
		ok = !os.IsNotExist(err)
	}

	return ok
}

// ExistsInMemory returns whether an item exists in memory.
// Unlike the Exists or Get methods ExistsInMemory neither checks the disk nor tries to
// fetch data via the dataLoader callback nor does it keep the item alive in the cache.
func (table *CacheTable) ExistsInMemory(key string) bool {
	table.mutex.RLock()
	defer table.mutex.RUnlock()
	_, ok := table.items[key]
	return ok
}

// Get returns an item from the cache and marks it to be kept alive. You can
// pass additional arguments to your DataLoader callback function.
func (table *CacheTable) Get(key string, args ...interface{}) (*CacheItem, error) {
	table.mutex.RLock()
	r, ok := table.items[key]
	table.mutex.RUnlock()

	if ok {
		r.KeepAlive()
		return r, nil
	}

	item := table.diskLoader(key)

	if item == nil && table.dataLoader != nil {
		item = table.dataLoader(key, args...)
	}

	if item != nil && item.IsValid() {
		table.mutex.Lock()
		item = table.add(item)
		return item, nil
	}

	return nil, ErrKeyNotFound
}
