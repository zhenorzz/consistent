package consistent

import (
	"errors"
	"hash/crc32"
	"sort"
	"fmt"
)

type Consistent struct {
	numOfVirtualNode int
	hashSortedNodes  []uint32
	circle           map[uint32]string
	nodes            map[string]bool
}

func New() *Consistent {
	return &Consistent{
		numOfVirtualNode: 20,
		circle:           make(map[uint32]string),
		nodes:            make(map[string]bool),
	}
}

//get the nearby node
func (c *Consistent) Get(key string) (string, error) {
	if len(c.nodes) == 0 {
		return "", errors.New("no host added")
	}
	nearbyIndex := c.searchNearbyIndex(key)
	nearHost := c.circle[c.hashSortedNodes[nearbyIndex]]
	return nearHost, nil
}

//add the node
func (c *Consistent) Add(node string) error {
	if _, ok := c.nodes[node]; ok {
		return errors.New("host already existed")
	}
	c.nodes[node] = true
	// add virtual node
	for i := 0; i < c.numOfVirtualNode; i++ {
		virtualKey := getVirtualKey(i, node)
		c.circle[virtualKey] = node
		c.hashSortedNodes = append(c.hashSortedNodes, virtualKey)
	}

	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})
	return nil
}

//remove the node
func (c *Consistent) Remove(node string) error {
	if _, ok := c.nodes[node]; ok {
		return errors.New("host is not existed")
	}
	delete(c.nodes, node)

	for i := 0; i < c.numOfVirtualNode; i++ {
		virtualKey := getVirtualKey(i, node)
		delete(c.circle, virtualKey)
	}

	c.refreshHashSlice()

	return nil
}

//list the nodes already existed
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
	targetIndex := sort.Search(len(c.hashSortedNodes), func(i int) bool {
		return c.hashSortedNodes[i] >= hashKey
	})

	if targetIndex >= len(c.hashSortedNodes) {
		targetIndex = 0
	}
	return targetIndex
}

func (c *Consistent) refreshHashSlice() {
	c.hashSortedNodes = nil
	for virtualKey := range c.circle {
		c.hashSortedNodes = append(c.hashSortedNodes, virtualKey)
	}
	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})
}

func hashKey(key string) uint32 {
	scratch := []byte(key)
	return crc32.ChecksumIEEE(scratch)
}
