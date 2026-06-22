package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/rithvik-999/rate-limiter/internal/limiter"
	"github.com/rithvik-999/rate-limiter/internal/middleware"
)

func main() {
	
	rl := limiter.NewFixedWindowLimiter(5, time.Minute,)

	apiHandler := http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("Success"))
		},
	)

	rateLimitHandler := middleware.RateLimit(rl)(apiHandler)

	http.Handle("/api", rateLimitHandler,)
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "allowed = %d | blocked = %d\n", rl.Allowed, rl.Blocked)
	})

	fmt.Println("Server running on :8080")

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			rl.CleanUp()
		}
	}()

	http.ListenAndServe(":8080", nil,)
}

