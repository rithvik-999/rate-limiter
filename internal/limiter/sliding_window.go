package limiter

import (
	"sync"
	"time"
)

// SlidingWindowLimiter implements a sliding window rate limiter.
// It allows at most 'limit' requests within the last 'window' duration for each user.
type SlidingWindowLimiter struct {
	limit  int
	window time.Duration

	users map[string][]time.Time

	mu sync.Mutex
}

// NewSlidingWindowLimiter creates a new sliding window rate limiter.
// limit: maximum number of requests allowed
// window: time duration for the sliding window
func NewSlidingWindowLimiter(limit int, window time.Duration) *SlidingWindowLimiter {
	return &SlidingWindowLimiter{
		limit:  limit,
		window: window,
		users:  make(map[string][]time.Time),
	}
}

// Allow checks if a request from the given user is allowed.
// Returns true if the request is allowed, false if it should be rejected.
func (swl *SlidingWindowLimiter) Allow(userID string) bool {
	// Step 1: Acquire mutex
	swl.mu.Lock()
	defer swl.mu.Unlock()

	// Step 2: Read current time
	currentTime := time.Now()

	// Step 3: Find user's timestamp list
	timestamps, exists := swl.users[userID]
	if !exists {
		timestamps = []time.Time{}
	}

	// Step 4: Compute the beginning of the sliding window
	cutoff := currentTime.Add(-swl.window)

	// Step 5 & 6: Find and remove expired timestamps
	// Instead of deleting one-by-one, reslice for better performance
	index := 0
	for index < len(timestamps) && timestamps[index].Before(cutoff) {
		index++
	}
	timestamps = timestamps[index:]

	// Step 7: Check current request count
	if len(timestamps) >= swl.limit {
		// Store the updated queue before rejecting
		swl.users[userID] = timestamps
		return false
	}

	// Step 8: Accept request and append current timestamp
	timestamps = append(timestamps, currentTime)

	// Step 9: Store updated queue
	swl.users[userID] = timestamps

	// Step 10: Release mutex (handled by defer) and return true
	return true
}

// Reset clears all rate limit data for all users.
func (swl *SlidingWindowLimiter) Reset() {
	swl.mu.Lock()
	defer swl.mu.Unlock()
	swl.users = make(map[string][]time.Time)
}

// GetUserRequestCount returns the current number of valid requests within the window for a user.
func (swl *SlidingWindowLimiter) GetUserRequestCount(userID string) int {
	swl.mu.Lock()
	defer swl.mu.Unlock()

	timestamps, exists := swl.users[userID]
	if !exists {
		return 0
	}

	// Remove expired timestamps
	cutoff := time.Now().Add(-swl.window)
	index := 0
	for index < len(timestamps) && timestamps[index].Before(cutoff) {
		index++
	}

	return len(timestamps) - index
}
