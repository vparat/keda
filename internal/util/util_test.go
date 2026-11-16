package util

import (
	"reflect"
	"testing"
)

// TestStringToMap checks parsing and malformed entries using t; it returns no values.
func TestStringToMap(t *testing.T) {

	for _, tt := range []struct {
		name, input string
		want        map[string]string
	}{
		{name: "pairs", input: "key1:value1,key2:value2", want: map[string]string{"key1": "value1", "key2": "value2"}},
		{name: "empty", want: map[string]string{}},
		{name: "whitespace", input: " key : value , other : two ", want: map[string]string{"key": "value", "other": "two"}},
		{name: "colon in value", input: "url:https://example.com:8080", want: map[string]string{"url": "https://example.com:8080"}},
		{name: "empty value", input: "key:", want: map[string]string{"key": ""}},
		{name: "duplicate", input: "key:first,key:last", want: map[string]string{"key": "last"}},
		{name: "malformed", input: ",missing-colon,:value, :bad,key:valid,", want: map[string]string{"key": "valid"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := StringToMap(tt.input)

			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("StringToMap(%q) = %#v, want %#v", tt.input, got, tt.want)
			}

			got["writable"] = "yes"
		})
	}
}
