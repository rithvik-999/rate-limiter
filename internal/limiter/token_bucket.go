package limiter

import (
	"sync"
	"time"
)

// Clock abstracts time retrieval so tests can inject deterministic time.
type Clock interface {
	Now() time.Time
}

// RealClock provides wall-clock time.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time {
	return time.Now()
}

// TokenBucketMetrics exposes limiter counters.
type TokenBucketMetrics struct {
	AllowedRequests int64
	BlockedRequests int64
	ActiveBuckets   int
}

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
	lastAccess time.Time
}

// TokenBucketLimiter implements a per-user token bucket rate limiter.
type TokenBucketLimiter struct {
	capacity   float64
	refillRate float64 // tokens per second
	clock      Clock

	buckets map[string]*tokenBucket
	mu      sync.RWMutex

	allowed int64
	blocked int64

	cleanupInterval time.Duration
	inactivityTTL   time.Duration
	stopCleanup     chan struct{}
}

// NewTokenBucketLimiter creates a limiter with wall-clock time and no background cleanup.
func NewTokenBucketLimiter(capacity int, refillRate float64) *TokenBucketLimiter {
	return NewTokenBucketLimiterWithOptions(capacity, refillRate, RealClock{}, 0, 0)
}

// NewTokenBucketLimiterWithClock creates a limiter with a custom clock and no background cleanup.
func NewTokenBucketLimiterWithClock(capacity int, refillRate float64, clock Clock) *TokenBucketLimiter {
	return NewTokenBucketLimiterWithOptions(capacity, refillRate, clock, 0, 0)
}

// NewTokenBucketLimiterWithOptions creates a limiter with optional background cleanup.
func NewTokenBucketLimiterWithOptions(capacity int, refillRate float64, clock Clock, cleanupInterval, inactivityTTL time.Duration) *TokenBucketLimiter {
	if clock == nil {
		clock = RealClock{}
	}

	if capacity < 1 {
		capacity = 1
	}
	if refillRate < 0 {
		refillRate = 0
	}

	tbl := &TokenBucketLimiter{
		capacity:        float64(capacity),
		refillRate:      refillRate,
		clock:           clock,
		buckets:         make(map[string]*tokenBucket),
		cleanupInterval: cleanupInterval,
		inactivityTTL:   inactivityTTL,
		stopCleanup:     make(chan struct{}),
	}

	if cleanupInterval > 0 && inactivityTTL > 0 {
		go tbl.cleanupLoop()
	}

	return tbl
}

// Allow consumes one token for key when available.
func (t *TokenBucketLimiter) Allow(key string) bool {
	now := t.clock.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	bucket, ok := t.buckets[key]
	if !ok {
		bucket = &tokenBucket{
			tokens:     t.capacity,
			lastRefill: now,
			lastAccess: now,
		}
		t.buckets[key] = bucket
	}

	t.refill(bucket, now)
	bucket.lastAccess = now

	if bucket.tokens >= 1 {
		bucket.tokens -= 1
		t.allowed++
		return true
	}

	t.blocked++
	return false
}

func (t *TokenBucketLimiter) refill(bucket *tokenBucket, now time.Time) {
	if t.refillRate == 0 {
		bucket.lastRefill = now
		return
	}

	elapsed := now.Sub(bucket.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}

	bucket.tokens += elapsed * t.refillRate
	if bucket.tokens > t.capacity {
		bucket.tokens = t.capacity
	}
	bucket.lastRefill = now
}

func (t *TokenBucketLimiter) cleanupLoop() {
	ticker := time.NewTicker(t.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.CleanupInactiveBuckets()
		case <-t.stopCleanup:
			return
		}
	}
}

// CleanupInactiveBuckets deletes buckets inactive for longer than inactivityTTL.
func (t *TokenBucketLimiter) CleanupInactiveBuckets() {
	if t.inactivityTTL <= 0 {
		return
	}

	now := t.clock.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	for key, bucket := range t.buckets {
		if now.Sub(bucket.lastAccess) > t.inactivityTTL {
			delete(t.buckets, key)
		}
	}
}

// Metrics returns a snapshot of counters and active buckets.
func (t *TokenBucketLimiter) Metrics() TokenBucketMetrics {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return TokenBucketMetrics{
		AllowedRequests: t.allowed,
		BlockedRequests: t.blocked,
		ActiveBuckets:   len(t.buckets),
	}
}

// ActiveBuckets returns the current number of active user buckets.
func (t *TokenBucketLimiter) ActiveBuckets() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.buckets)
}

// Stop halts background cleanup goroutine if enabled.
func (t *TokenBucketLimiter) Stop() {
	select {
	case <-t.stopCleanup:
		return
	default:
		close(t.stopCleanup)
	}
}
