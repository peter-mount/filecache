package service

import (
	"errors"
	"flag"
	"github.com/peter-mount/filecache"
	"github.com/peter-mount/golib/kernel"
	"os"
)

// Cache is an in-memory cache which is also persisted by the underlying filesystem
type FileCacheService struct {
	cacheDir *string
	cache    *filecache.Cache
}

func (c *FileCacheService) Name() string {
	return "FileCacheService"
}

func (c *FileCacheService) Init(k *kernel.Kernel) error {
	c.cacheDir = flag.String("cacheDirectory", "", "Directory to store caches")
	return nil
}

func (c *FileCacheService) PostInit() error {
	if c.cacheDir == nil || *c.cacheDir == "" {
		*c.cacheDir = os.Getenv("CACHEDIR")
	}
	if c.cacheDir == nil || *c.cacheDir == "" {
		return errors.New("-cacheDirectory is required")
	}

	c.cache = filecache.NewCache(filecache.CacheConfig{
		CacheDir: *c.cacheDir,
	})

	return nil
}

func (c *FileCacheService) Start() error {
	return c.cache.Start()
}

func (c *FileCacheService) Stop() {
	c.cache.Stop()
}

func (c *FileCacheService) Cache() *filecache.Cache {
	return c.cache
}
