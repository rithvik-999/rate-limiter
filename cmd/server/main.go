package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/rithvik-999/rate-limiter/internal/limiter"
)

func main() {
	rl := limiter.NewFixedWindowLimiter(5, time.Minute,)

	http.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("user")

		if userID == "" {
			http.Error(w, "user parameter required", http.StatusBadRequest)
			return
		}

		if !rl.Allow(userID){
			http.Error(w, "Rate Limit Exceeded", http.StatusTooManyRequests)
			return
		}

		fmt.Fprintf(w, "Request allowed for %s", userID,)

	})

	fmt.Println("Server running on :8080")

	http.ListenAndServe(":8080", nil,)
}