package filecache

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	PathSeparator = string(os.PathSeparator)
)

func (table *CacheTable) getPath(key string) (string, string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	b := hex.EncodeToString(h.Sum(nil))
	return table.basePath + PathSeparator + b[0:1] + PathSeparator + b[1:3], key
}

func (table *CacheTable) getFilePath(key string) string {
	dir, fn := table.getPath(key)
	return dir + PathSeparator + fn
}

type walkFunc func(key, path string, info os.FileInfo, err error) error

func (table *CacheTable) walk(f walkFunc) error {
	return filepath.Walk(table.basePath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			key := strings.SplitN(path, PathSeparator, 3)
			if len(key) == 3 {
				err = f(key[2], path, info, err)
				return err
			}
		}

		return nil
	})
}

func (table *CacheTable) loadCache(maxAge time.Duration) {
	table.stopDiskExpiryTimer()
	table.mutex.Lock()
	defer func() {
		table.mutex.Unlock()
		table.startDiskExpiryTimer()
		table.expireMemory()
	}()

	if maxAge > 0 {
		maxAge = -maxAge
	}
	loadTime := time.Now().Add(maxAge)

	_ = table.walk(func(key, path string, info os.FileInfo, err error) error {

		if maxAge == 0 || info.ModTime().After(loadTime) {
			item := table.diskLoader(key)
			if item != nil {
				table.items[key] = item
			}
		}

		return nil
	})
}

func (c *Cache) initCacheDir() error {
	err := os.MkdirAll(c.cacheDir, 0777)
	if err != nil {
		return err
	}

	stat, err := os.Stat(c.cacheDir)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return fmt.Errorf("%s is not a directory", c.cacheDir)
	}

	// Test we can write to it
	tmpName := c.cacheDir + PathSeparator + "__tmpfile__"
	tmpFile, err := os.Create(tmpName)
	if err != nil {
		return err
	}
	err = tmpFile.Close()
	if err != nil {
		return err
	}
	return os.Remove(tmpName)
}
