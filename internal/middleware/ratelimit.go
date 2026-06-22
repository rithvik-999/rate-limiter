package middleware

import (
	"net/http"

	"github.com/rithvik-999/rate-limiter/internal/limiter"
)

func RateLimit(l limiter.Limiter) func(http.Handler) http.Handler{
	
	return func (next http.Handler) http.Handler {

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userID := r.URL.Query().Get("user")

			if userID == ""{
				http.Error(w, "user parameter required", http.StatusBadRequest)
				return
			}

			if !l.Allow(userID) {
				http.Error(w, "Rate limit exceeded", http.StatusBadRequest)
				return
			}

			next.ServeHTTP(w,r)
		})
		
	}
}