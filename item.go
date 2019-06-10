package filecache

import (
	"strings"
	"sync"
	"time"
)

// CacheItem is an individual cache item
type CacheItem struct {
	mutex         sync.RWMutex
	key           string
	data          interface{}
	lifeSpan      time.Duration
	createdOn     time.Time
	accessedOn    time.Time
	accessCount   int64
	aboutToExpire CacheKeyCallback
}

func NewCacheItem(key string, lifeSpan time.Duration, data interface{}) *CacheItem {
	t := time.Now()
	return &CacheItem{
		key:           key,
		lifeSpan:      lifeSpan,
		createdOn:     t,
		accessedOn:    t,
		accessCount:   0,
		aboutToExpire: nil,
		data:          data,
	}
}

func NewCreatedCacheItem(key string, lifeSpan time.Duration, data interface{}, created time.Time) *CacheItem {
	return &CacheItem{
		key:           key,
		lifeSpan:      lifeSpan,
		createdOn:     created,
		accessedOn:    time.Now(),
		accessCount:   0,
		aboutToExpire: nil,
		data:          data,
	}
}

// IsValid returns true of the key is valid.
// As we store entries on disk with the key as the filename then we have to prevent certain characters
// so that we don't break things or expose some filesystem attack.
// So, "" and any key starting with "." are prohibited.
// Otherwise the following characters are prohibited anywhere in the key.
// null (0x0) is also prohibited (Unix)
// / \ < > : " | ? *
// Although windows doesn't like characters 1..31 we don't check for them.
func (item *CacheItem) IsValid() bool {
	return item != nil &&
		item.key != "" &&
		item.key[0] != '.' &&
		!strings.ContainsAny(item.key, "/\\<>:\"|?*\000") &&
		item.data != nil &&
		item.lifeSpan > 0
}

func (item *CacheItem) KeepAlive() {
	item.mutex.Lock()
	defer item.mutex.Unlock()
	item.accessedOn = time.Now()
	item.accessCount++
}

func (item *CacheItem) LifeSpan() time.Duration {
	return item.lifeSpan
}

func (item *CacheItem) AccessedOn() time.Time {
	item.mutex.RLock()
	defer item.mutex.RUnlock()
	return item.accessedOn
}

func (item *CacheItem) CreatedOn() time.Time {
	return item.createdOn
}

func (item *CacheItem) AccessCount() int64 {
	item.mutex.RLock()
	defer item.mutex.RUnlock()
	return item.accessCount
}

func (item *CacheItem) Key() string {
	return item.key
}

func (item *CacheItem) Data() interface{} {
	return item.data
}

// SetAboutToExpireCallback configures a callback, which will be called right before the item is about to be removed from the cache.
func (item *CacheItem) SetAboutToExpireCallback(f CacheKeyCallback) {
	item.mutex.Lock()
	defer item.mutex.Unlock()
	item.aboutToExpire = f
}
