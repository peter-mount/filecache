/*
 * An in-memory cache library with persistence to disk & entry expiration.
 *
 * (C) 2019 Peter Mount
 */
package filecache

import (
	"errors"
	"sync"
)

// Cache is an in-memory cache which is also persisted by the underlying filesystem
type Cache struct {
	cacheDir string
	mutex    sync.RWMutex
	tables   map[string]*CacheTable
	started  bool
}

// CacheConfig mutable config for creating the cache
type CacheConfig struct {
	// The required path to where all caches will be located on disk
	CacheDir string
}

type CacheDataLoader func(key string, args ...interface{}) *CacheItem

type CacheItemCallback func(item *CacheItem)

type CacheKeyCallback func(key string)

type CacheItemWalker func(key string, item *CacheItem)

var (
	// ErrKeyNotFound gets returned when a specific key couldn't be found
	ErrKeyNotFound = errors.New("keynotfound")
)

// NewCache creates a new Cache based on the supplied config
func NewCache(cfg CacheConfig) *Cache {
	f := &Cache{
		cacheDir: cfg.CacheDir,
		tables:   map[string]*CacheTable{},
	}

	return f
}

// Start starts the cache
func (c *Cache) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.started {
		return errors.New("cache already started")
	}

	err := c.initCacheDir()
	if err != nil {
		return err
	}

	// Start all tables
	for _, t := range c.tables {
		err = t.start()
		if err != nil {
			return err
		}
	}

	c.started = true

	return nil
}

// Stop stops the cache
func (c *Cache) Stop() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.started {
		return
	}

	for _, t := range c.tables {
		t.stop()
	}

	c.started = false
}

// GetCache returns the named CacheTable or nil if it doesn't exist
func (c *Cache) GetCache(n string) *CacheTable {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.tables[n]
}
