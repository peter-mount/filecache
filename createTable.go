package filecache

import (
	"fmt"
	"time"
)

type CacheTableConfig struct {
	// The unique name for this cache
	Name string
	// How long entries remain in memory
	ExpiryTime time.Duration
	// Optional function to convert values to a []byte slice.
	// If not supplied then json will be presumed.
	ToBytes func(interface{}) []byte
	// Function to unmarshal the value from disk.
	// Unlike ToBytes this is required as you need to supply the underlying object to the various
	// unmarshallers
	FromBytes func([]byte) interface{}
	// The startup options for this cache
	StartupOptions int
	// How long to keep entries in the disk cache.
	// If not supplied then the default of 24 hours is used.
	DiskExpiryTime time.Duration
	// How often does the disk cache get scanned for expired entries.
	// If not set then this defaults to once an hour
	DiscExpiryInterval time.Duration
	// The queue size for persistence. Default is 1
	PersistQueueSize int
	// Optional dataLoader called when a key doesn't exist in either memory or disk
	DataLoader CacheDataLoader
	// Optional callback called when an item is added
	AddItem CacheItemCallback
	// Optional callback called when an item is about to be removed from memory (but not disk)
	DeleteItem CacheItemCallback
}

const (
	// On cache start flush the cache removing all entries from it
	FlushCacheOnStart = iota
	// On cache start expire entries on disk that have expired
	ExpireCacheOnStart
	// Load the disk cache into memory if they are newer than the memory expiry time
	LoadCacheOnStart
	// Load the disk cache into memory regardless of age.
	// Be warned this may cause memory issues if the disk cache is large
	LoadEntireCacheOnStart
)

// AddCache adds a new CacheTable to the cache.
// If a cache of the same name exists then this will return an error
func (c *Cache) AddCache(cfg CacheTableConfig) (*CacheTable, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.tables[cfg.Name]; exists {
		return nil, fmt.Errorf("cache %s already exists", cfg.Name)
	}

	toBytes := cfg.ToBytes
	if toBytes == nil {
		toBytes = ToJsonBytes
	}

	persistQueueSize := cfg.PersistQueueSize
	if persistQueueSize <= 0 {
		persistQueueSize = 1
	}

	expiryTime := cfg.ExpiryTime
	if expiryTime < 0 {
		expiryTime = 0
	}

	diskExpiryTime := cfg.DiskExpiryTime
	if diskExpiryTime <= 0 {
		diskExpiryTime = 24 * time.Hour
	}

	diskExpiryInterval := cfg.DiscExpiryInterval
	if diskExpiryInterval <= 0 {
		diskExpiryInterval = time.Hour
	}

	t := &CacheTable{
		parent:             c,
		name:               cfg.Name,
		items:              make(map[string]*CacheItem),
		toBytes:            toBytes,
		fromBytes:          cfg.FromBytes,
		startupOptions:     cfg.StartupOptions,
		expiryTime:         expiryTime,
		persistQueue:       make(chan persistEntry, persistQueueSize),
		diskExpiryInterval: diskExpiryInterval,
		diskExpiryTime:     diskExpiryTime,
		dataLoader:         cfg.DataLoader,
		addItem:            cfg.AddItem,
		deleteItem:         cfg.DeleteItem,
	}

	c.tables[t.name] = t

	// Start the cache if we have already started
	if c.started {
		err := t.start()
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}
