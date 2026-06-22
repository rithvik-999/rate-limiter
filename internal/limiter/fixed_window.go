package limiter

import (
	"sync"
	"time"
	"fmt"
)

type UserWindow struct{
	Count int
	WindowStart time.Time
}

type FixedWindowLimiter struct{
	limit int
	window time.Duration

	users map[string]*UserWindow
	Allowed int64
	Blocked int64

	mu sync.Mutex
}

func NewFixedWindowLimiter(limit int, window time.Duration,) *FixedWindowLimiter{
	return &FixedWindowLimiter{
		limit: limit,
		window: window,
		users: make(map[string]*UserWindow),
	}
}

func (f *FixedWindowLimiter) Allow(userID string,) bool{
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now()

	user, exists := f.users[userID]

	if !exists {
		f.users[userID] = &UserWindow{
			Count: 1,
			WindowStart: now,
		}
		f.Allowed++

		return true;
	}

	if now.Sub(user.WindowStart) >= f.window{
		user.Count = 1
		user.WindowStart = now
		f.Allowed++

		return true
	}

	if user.Count >= f.limit {
		f.Blocked++
		return false
	}

	user.Count++
	f.Allowed++

	fmt.Printf(
		"User=%s Count=%d\n",
		userID,
		user.Count,
	)
	return true
}

func (f *FixedWindowLimiter) CleanUp() {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now()

	for userId, user := range f.users {
		if now.Sub(user.WindowStart) > f.window{
			delete(f.users, userId)
		}
	}
}
