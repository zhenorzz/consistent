package consistent

import (
	"errors"
	"hash/crc32"
	"sort"
	"fmt"
)

var NoHosts = errors.New("no host added")

type Consistent struct {
	NumOfVNode      int
	hashSortedSlice []uint32
	circle          map[uint32]string
	nodes           map[string]bool
}

func New() *Consistent {
	return &Consistent{
		NumOfVNode: 20,
		circle:     make(map[uint32]string),
		nodes:      make(map[string]bool),
	}
}

func (c *Consistent) Get(key string) (string, error) {
	if len(c.nodes) == 0 {
		return "", NoHosts
	}
	nearbyIndex := c.searchNearbyIndex(key)
	nearHost := c.circle[c.hashSortedSlice[nearbyIndex]]
	return nearHost, nil
}

func (c *Consistent) Add(node string) {
	if _, ok := c.nodes[node]; ok {
		return
	}
	c.nodes[node] = true
	// add virtual node
	for i := 0; i < c.NumOfVNode; i++ {
		virtualKey := getVirtualKey(i, node)
		c.circle[virtualKey] = node
		c.hashSortedSlice = append(c.hashSortedSlice, virtualKey)
	}

	sort.Slice(c.hashSortedSlice, func(i, j int) bool {
		return c.hashSortedSlice[i] < c.hashSortedSlice[j]
	})
}

func (c *Consistent) Remove(node string) {
	if _, ok := c.nodes[node]; ok {
		return
	}
	delete(c.nodes, node)

	for i := 0; i < c.NumOfVNode; i++ {
		virtualKey := getVirtualKey(i, node)
		delete(c.circle, virtualKey)
	}

	c.refreshHashSlice()
}

func (c *Consistent) ListNodes() []string {
	var nodes []string
	for node := range c.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func getVirtualKey(index int, node string) uint32 {
	return hashKey(fmt.Sprintf("%s#%d", node, index))
}

func (c *Consistent) searchNearbyIndex(key string) int {
	hashKey := hashKey(key)
	targetIndex := sort.Search(len(c.hashSortedSlice), func(i int) bool {
		return c.hashSortedSlice[i] >= hashKey
	})

	if targetIndex >= len(c.hashSortedSlice) {
		targetIndex = 0
	}
	return targetIndex
}

func (c *Consistent) refreshHashSlice() {
	c.hashSortedSlice = nil
	for virtualKey := range c.circle {
		c.hashSortedSlice = append(c.hashSortedSlice, virtualKey)
	}
	sort.Slice(c.hashSortedSlice, func(i, j int) bool {
		return c.hashSortedSlice[i] < c.hashSortedSlice[j]
	})
}

func hashKey(key string) uint32 {
	scratch := []byte(key)
	return crc32.ChecksumIEEE(scratch)
}
