package limiter

import (
	"sync"
	"time"
)

// RingBuffer is a fixed-size circular buffer for storing timestamps.
// It avoids allocations and garbage collection overhead compared to slices.
type RingBuffer struct {
	buffer []*time.Time
	head   int // Points to the next write position
	size   int // Current number of elements in the buffer
	cap    int // Capacity of the buffer
}

// NewRingBuffer creates a new ring buffer with the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		buffer: make([]*time.Time, capacity),
		head:   0,
		size:   0,
		cap:    capacity,
	}
}

// Push adds a timestamp to the ring buffer.
func (rb *RingBuffer) Push(t time.Time) {
	if rb.size < rb.cap {
		rb.buffer[rb.size] = &t
		rb.size++
	} else {
		// Buffer is full, overwrite oldest element
		rb.buffer[rb.head] = &t
	}
	rb.head = (rb.head + 1) % rb.cap
}

// RemoveExpired removes all timestamps before the cutoff time and returns the count of remaining timestamps.
func (rb *RingBuffer) RemoveExpired(cutoff time.Time) int {
	if rb.size == 0 {
		return 0
	}

	// Find the first non-expired timestamp
	firstValid := 0
	for firstValid < rb.size && rb.buffer[firstValid].Before(cutoff) {
		firstValid++
	}

	// Shift remaining timestamps to the front
	if firstValid > 0 {
		copy(rb.buffer[0:], rb.buffer[firstValid:rb.size])
		rb.size -= firstValid
		rb.head = 0
	}

	return rb.size
}

// Count returns the current number of timestamps in the buffer.
func (rb *RingBuffer) Count() int {
	return rb.size
}

// Clear resets the ring buffer.
func (rb *RingBuffer) Clear() {
	rb.size = 0
	rb.head = 0
}

// UserLimiter holds the rate limit state for a single user.
type UserLimiter struct {
	buffer       *RingBuffer
	mu           sync.Mutex
	lastActivity time.Time
}

// Metrics holds rate limiting statistics.
type Metrics struct {
	AllowedRequests int64
	BlockedRequests int64
	ActiveUsers     int
}

// ImprovedSlidingWindowLimiter implements an improved sliding window rate limiter with:
// - Ring buffer for efficient timestamp storage
// - Per-user locks for better concurrency
// - Inactive user cleanup
// - Metrics collection
type ImprovedSlidingWindowLimiter struct {
	limit           int
	window          time.Duration
	cleanupInterval time.Duration
	inactivityTTL   time.Duration

	users map[string]*UserLimiter

	mu sync.RWMutex

	metrics struct {
		mu              sync.Mutex
		allowedRequests int64
		blockedRequests int64
		lastCleanupTime time.Time
	}

	stopCleanup chan bool
}

// NewImprovedSlidingWindowLimiter creates a new improved sliding window rate limiter.
// limit: maximum number of requests allowed
// window: time duration for the sliding window
// cleanupInterval: how often to cleanup inactive users
// inactivityTTL: how long a user can be inactive before cleanup
func NewImprovedSlidingWindowLimiter(limit int, window, cleanupInterval, inactivityTTL time.Duration) *ImprovedSlidingWindowLimiter {
	limiter := &ImprovedSlidingWindowLimiter{
		limit:           limit,
		window:          window,
		cleanupInterval: cleanupInterval,
		inactivityTTL:   inactivityTTL,
		users:           make(map[string]*UserLimiter),
		stopCleanup:     make(chan bool, 1),
	}

	limiter.metrics.lastCleanupTime = time.Now()

	// Start background cleanup goroutine
	go limiter.cleanupInactiveUsers()

	return limiter
}

// Allow checks if a request from the given user is allowed.
func (iwl *ImprovedSlidingWindowLimiter) Allow(userID string) bool {
	currentTime := time.Now()

	// Get or create user limiter
	iwl.mu.Lock()
	userLimiter, exists := iwl.users[userID]
	if !exists {
		userLimiter = &UserLimiter{
			buffer:       NewRingBuffer(iwl.limit),
			lastActivity: currentTime,
		}
		iwl.users[userID] = userLimiter
	}
	iwl.mu.Unlock()

	// Acquire per-user lock
	userLimiter.mu.Lock()
	defer userLimiter.mu.Unlock()

	// Update last activity time
	userLimiter.lastActivity = currentTime

	// Compute cutoff time
	cutoff := currentTime.Add(-iwl.window)

	// Remove expired timestamps and get remaining count
	validCount := userLimiter.buffer.RemoveExpired(cutoff)

	// Check if request is allowed
	if validCount >= iwl.limit {
		iwl.recordBlockedRequest()
		return false
	}

	// Add current timestamp
	userLimiter.buffer.Push(currentTime)

	iwl.recordAllowedRequest()
	return true
}

// recordAllowedRequest increments the allowed requests counter.
func (iwl *ImprovedSlidingWindowLimiter) recordAllowedRequest() {
	iwl.metrics.mu.Lock()
	defer iwl.metrics.mu.Unlock()
	iwl.metrics.allowedRequests++
}

// recordBlockedRequest increments the blocked requests counter.
func (iwl *ImprovedSlidingWindowLimiter) recordBlockedRequest() {
	iwl.metrics.mu.Lock()
	defer iwl.metrics.mu.Unlock()
	iwl.metrics.blockedRequests++
}

// GetMetrics returns a snapshot of the current metrics.
func (iwl *ImprovedSlidingWindowLimiter) GetMetrics() Metrics {
	iwl.metrics.mu.Lock()
	allowed := iwl.metrics.allowedRequests
	blocked := iwl.metrics.blockedRequests
	iwl.metrics.mu.Unlock()

	iwl.mu.RLock()
	activeUsers := len(iwl.users)
	iwl.mu.RUnlock()

	return Metrics{
		AllowedRequests: allowed,
		BlockedRequests: blocked,
		ActiveUsers:     activeUsers,
	}
}

// GetUserRequestCount returns the current number of valid requests within the window for a user.
func (iwl *ImprovedSlidingWindowLimiter) GetUserRequestCount(userID string) int {
	iwl.mu.RLock()
	userLimiter, exists := iwl.users[userID]
	iwl.mu.RUnlock()

	if !exists {
		return 0
	}

	userLimiter.mu.Lock()
	defer userLimiter.mu.Unlock()

	cutoff := time.Now().Add(-iwl.window)
	return userLimiter.buffer.RemoveExpired(cutoff)
}

// Reset clears all rate limit data for all users.
func (iwl *ImprovedSlidingWindowLimiter) Reset() {
	iwl.mu.Lock()
	defer iwl.mu.Unlock()
	iwl.users = make(map[string]*UserLimiter)
}

// cleanupInactiveUsers runs a background goroutine to remove inactive users.
func (iwl *ImprovedSlidingWindowLimiter) cleanupInactiveUsers() {
	ticker := time.NewTicker(iwl.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			iwl.performCleanup()
		case <-iwl.stopCleanup:
			return
		}
	}
}

// performCleanup removes users that have been inactive for longer than inactivityTTL.
func (iwl *ImprovedSlidingWindowLimiter) performCleanup() {
	iwl.mu.Lock()
	defer iwl.mu.Unlock()

	now := time.Now()
	iwl.metrics.mu.Lock()
	iwl.metrics.lastCleanupTime = now
	iwl.metrics.mu.Unlock()

	for userID, userLimiter := range iwl.users {
		userLimiter.mu.Lock()
		if now.Sub(userLimiter.lastActivity) > iwl.inactivityTTL {
			userLimiter.mu.Unlock()
			delete(iwl.users, userID)
		} else {
			userLimiter.mu.Unlock()
		}
	}
}

// Stop stops the background cleanup goroutine.
func (iwl *ImprovedSlidingWindowLimiter) Stop() {
	select {
	case iwl.stopCleanup <- true:
	default:
	}
}
