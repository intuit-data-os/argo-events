package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBatchDurationReached(t *testing.T) {
	in := make(chan int)
	defer close(in)

	out := Batch(5, 1*time.Second, in)

	in <- 0
	assert.Equal(t, []int{0}, <-out)

	in <- 1
	in <- 2
	assert.Equal(t, []int{1, 2}, <-out)

	in <- 3
	in <- 4
	in <- 5
	assert.Equal(t, []int{3, 4, 5}, <-out)
}

func TestBatchSizeReached(t *testing.T) {
	in := make(chan int)
	defer close(in)

	out := Batch(2, 1*time.Second, in)

	in <- 0
	in <- 1
	assert.Equal(t, <-out, []int{0, 1})

	in <- 2
	in <- 3
	in <- 4
	in <- 5
	assert.Equal(t, []int{2, 3}, <-out)
	assert.Equal(t, []int{4, 5}, <-out)
}

func TestBatchMaintainsOrder(t *testing.T) {
	in := make(chan string)
	defer close(in)

	out := Batch(10, 1*time.Second, in)

	in <- "a"
	in <- "b"
	in <- "c"
	in <- "d"
	in <- "e"
	in <- "f"
	in <- "g"
	in <- "h"
	in <- "i"
	in <- "j"
	assert.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}, <-out)
}

func TestBatchChannelCleanedUp(t *testing.T) {
	in := make(chan string)
	out := Batch(10, 1*time.Second, in)

	close(in)
	assert.Equal(t, []string(nil), <-out)
}
