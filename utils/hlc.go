package utils

import (
	"sync"
	"time"
)

// HLC structure
type HLC struct {
	physical int64
	logical  int64
	mu       sync.Mutex
}

// Init a HLC
func NewHLC() *HLC {
	return &HLC{
		physical: time.Now().UnixNano(),
		logical:  0,
	}
}

// Generate HLC timestamp for local events
func (c *HLC) Now() (int64, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentPhysical := time.Now().UnixNano()

	if currentPhysical > c.physical {
		c.physical = currentPhysical
		c.logical = 0
	} else {
		c.logical++
	}

	return c.physical, c.logical
}

// Update the node HLC based on received timestamp
func (c *HLC) Update(recvP, recvL int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	localPhysical := time.Now().UnixNano()
	hlcPhysical := c.physical

	c.physical = max(localPhysical, max(c.physical, recvP))

	// if localPhysical > c.physical {
	// 	c.physical = localPhysical
	// 	c.logical = 0
	// }

	// if recvP > c.physical {
	// 	c.physical = recvP
	// 	c.logical = recvL + 1
	// } else if recvP == c.physical {
	// 	c.logical = max(c.logical, recvL) + 1
	// } else {
	// 	c.logical++
	// }

	if c.physical == localPhysical {
		c.logical = 0
	} else if c.physical == hlcPhysical && c.physical == recvP {
		c.logical = max(c.logical, recvL) + 1
	} else if c.physical == hlcPhysical {
		c.logical++
	} else if c.physical == recvP {
		c.logical = recvL + 1
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
