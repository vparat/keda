// Package util provides shared helpers.
package util

import "strings"

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
