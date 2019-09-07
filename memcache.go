// This is a thread safe memory cache which is of a fixed size. When a item is pushed in, the following will happen:
//   - If there is enough memory for all items in the cache and the new item, it will be appended to the cache.
//   - If the item is larger than the cache, it will not be pushed into the cache.
//   - If the item is not larger than the cache but the cache doesn't have enough room for the item, it will loop through removing items from the cache in a "First In, First Out" manor.

package main

import (
	"sync"
	"unsafe"
)

var Cache *InMemoryCache

type InMemoryCache struct {
	Lock *sync.RWMutex
	TotalBytes int64
	UsedBytes int64
	Map map[string]*[]byte
}

func NewMemoryCache() {
	mutex := sync.RWMutex{}
	Cache = &InMemoryCache{
		Lock:       &mutex,
		TotalBytes: 100000000,
		UsedBytes:  0,
		Map: map[string]*[]byte{},
	}
}

func (c *InMemoryCache) DeleteNonThreadSafe(key string) {
	// Gets the size in memory.
	mem := unsafe.Sizeof(c.Map[key])

	// Subtracts the memory usage of this item from used memory.
	c.TotalBytes -= int64(mem)

	// Deletes from the cache.
	delete(c.Map, key)
}

func (c *InMemoryCache) Delete(key string) {
	// Locks the thread lock.
	c.Lock.Lock()

	// Delete the item.
	c.DeleteNonThreadSafe(key)

	// Unlocks the thread lock.
	c.Lock.Unlock()
}

func (c *InMemoryCache) Get(key string) *[]byte {
	// Locks the thread lock.
	c.Lock.RLock()

	// Gets the item.
	i := c.Map[key]

	// Unlocks the thread lock.
	c.Lock.RUnlock()

	// Returns the item.
	return i
}

func (c *InMemoryCache) Set(key string, value []byte) {
	// Locks the thread lock.
	c.Lock.Lock()

	// Gets the values memory usage.
	mem := int64(unsafe.Sizeof(value))

	// If the memory usage is larger than the cache, return.
	if mem > c.TotalBytes {
		c.Lock.Unlock()
		return
	}

	// Iterate through until there's enough memory for the item.
	if mem + c.UsedBytes > c.TotalBytes {
		for k := range c.Map {
			TotalUsed := mem + c.UsedBytes
			if c.TotalBytes >= TotalUsed {
				break
			}
			c.DeleteNonThreadSafe(k)
		}
	}

	// Insert the item into the map.
	c.Map[key] = &value

	// Unlocks the thread lock.
	c.Lock.Unlock()
}
