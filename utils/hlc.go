package utils

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"
)

// HLC structure
type HLC struct {
	Physical int64
	Logical  int64
	mu       sync.Mutex
}

// Encode the HLC
func (h *HLC) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Encode the struct without the mutex field
	err := enc.Encode(struct {
		Physical int64
		Logical  int64
	}{
		Physical: h.Physical,
		Logical:  h.Logical,
	})

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode HLC
func (h *HLC) Decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	h.mu.Lock()
	defer h.mu.Unlock()

	// Decode into a temporary struct that does not include the mutex
	temp := struct {
		Physical int64
		Logical  int64
	}{}

	err := dec.Decode(&temp)
	if err != nil {
		return err
	}

	// Assign the decoded values back to the struct
	h.Physical = temp.Physical
	h.Logical = temp.Logical
	return nil
}

// Init a HLC
func NewHLC() *HLC {
	return &HLC{
		Physical: time.Now().UnixNano(),
		Logical:  0,
	}
}

// Generate HLC timestamp for local events
func (c *HLC) Now() (int64, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentPhysical := time.Now().UnixNano()

	if currentPhysical > c.Physical {
		c.Physical = currentPhysical
		c.Logical = 0
	} else {
		c.Logical++
	}

	return c.Physical, c.Logical
}

// Update the node HLC based on received timestamp
func (c *HLC) Update(recvP, recvL int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	localPhysical := time.Now().UnixNano()
	hlcPhysical := c.Physical

	c.Physical = max(localPhysical, max(c.Physical, recvP))

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

	if c.Physical == localPhysical {
		c.Logical = 0
	} else if c.Physical == hlcPhysical && c.Physical == recvP {
		c.Logical = max(c.Logical, recvL) + 1
	} else if c.Physical == hlcPhysical {
		c.Logical++
	} else if c.Physical == recvP {
		c.Logical = recvL + 1
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
