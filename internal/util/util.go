// Package util provides shared helpers.
package util

import (
	"os"
	"strings"
)

// StringToMap parses comma-separated key:value entries from input into a map.
// It trims whitespace, preserves colons in values, and skips entries without a
// colon or with an empty key. Duplicate keys use the last value; empty input
// returns an empty, writable map.
func StringToMap(input string) map[string]string {
	result := make(map[string]string)

	for _, entry := range strings.Split(input, ",") {
		key, value, ok := strings.Cut(entry, ":")
		key = strings.TrimSpace(key)

		if !ok || key == "" {
			continue
		}

		result[key] = strings.TrimSpace(value)
	}

	return result
}


// Env returns the value of the environment variable key, or fallback if key is unset or empty.
func Env(key, fallback string) string {

	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
