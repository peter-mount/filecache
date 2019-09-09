package filecache

import (
	"os"
	"time"
)

func (table *CacheTable) expireMemory() {
	table.mutex.Lock()
	defer table.mutex.Unlock()

	table.stopMemoryExpiryTimer()

	now := time.Now()
	smallestDuration := 0 * time.Second

	for key, item := range table.items {
		item.mutex.RLock()
		lifeSpan := item.lifeSpan
		accessedOn := item.accessedOn
		item.mutex.RUnlock()

		if lifeSpan == 0 {
			continue
		}

		if now.Sub(accessedOn) >= lifeSpan {
			table.delete(key)
		} else {
			if smallestDuration == 0 || lifeSpan-now.Sub(accessedOn) < smallestDuration {
				smallestDuration = lifeSpan - now.Sub(accessedOn)
			}
		}
	}

	table.cleanupInterval = smallestDuration
	if smallestDuration > 0 {
		table.cleanupTimer = time.AfterFunc(smallestDuration, func() {
			go table.expireMemory()
		})
	}
}

func (table *CacheTable) stopMemoryExpiryTimer() {
	if table.cleanupTimer != nil {
		table.cleanupTimer.Stop()
	}
}

// ExpireDisk removes any entry on disk who's modified time is older than diskExpiryTime
// and is not currently in memory.
// This isn't exact as when the in memory copy is removed due lack of use then the disk copy
// becomes available for expiry (i.e. deletion) even if it's only just expired.
func (table *CacheTable) ExpireDisk() int {
	return table.ExpireDiskMaxAge(table.diskExpiryTime)
}

func (table *CacheTable) ExpireDiskMaxAge(maxAge time.Duration) int {
	table.stopDiskExpiryTimer()
	defer table.startDiskExpiryTimer()

	if maxAge > 0 {
		maxAge = -maxAge
	}
	expireTime := time.Now().Add(maxAge)

	expired := 0

	_ = table.walk(func(key, path string, info os.FileInfo, err error) error {

		if info.ModTime().Before(expireTime) {
			// nre-feeds#21 remove from memory as well as disk
			table.DeleteFromMemoryAndDisk(key)
			expired++
		}

		return nil
	})

	return expired
}

func (table *CacheTable) stopDiskExpiryTimer() {
	table.mutex.Lock()
	defer table.mutex.Unlock()

	if table.diskExpiryTimer != nil {
		table.diskExpiryTimer.Stop()
	}
}

func (table *CacheTable) startDiskExpiryTimer() {
	table.mutex.Lock()
	defer table.mutex.Unlock()

	table.diskExpiryTimer = time.AfterFunc(table.diskExpiryInterval, func() {
		go table.ExpireDisk()
	})
}

func (table *CacheTable) FlushMemoryAndDisk() {
	table.stopDiskExpiryTimer()
	table.mutex.Lock()
	defer func() {
		table.mutex.Unlock()
		table.startDiskExpiryTimer()
	}()

	table.flushMemory()
	table.flushDisk()
}

func (table *CacheTable) FlushMemory() {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	table.flushMemory()
}

func (table *CacheTable) flushMemory() {
	table.items = make(map[string]*CacheItem)
	table.cleanupInterval = 0
	table.stopMemoryExpiryTimer()
}

func (table *CacheTable) FlushDisk() {
	table.stopDiskExpiryTimer()
	table.mutex.Lock()
	defer func() {
		table.mutex.Unlock()
		table.startDiskExpiryTimer()
	}()
	table.flushDisk()
}

func (table *CacheTable) flushDisk() {
	_ = table.walk(func(key, path string, info os.FileInfo, err error) error {
		_ = os.Remove(path)
		return nil
	})
}
