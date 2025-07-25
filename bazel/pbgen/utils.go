package main

import (
	"strings"
	"unicode"
)

func pascalToSnakeCase(s string) string {
	var builder strings.Builder
	// Iterate through the string character by character
	for i, r := range s {
		if !unicode.IsUpper(r) {
			builder.WriteRune(r)
			continue
		}
		if i == 0 {
			builder.WriteRune(unicode.ToLower(r))
			continue
		}

		prevRune := rune(s[i-1]) // Get the previous character for comparison

		// Condition 1: Transition from lowercase to uppercase (e.g., "pascalCase" -> "pascal_Case")
		isNewWordStart := unicode.IsLower(prevRune) && unicode.IsUpper(r)

		// Condition 2: Transition from an acronym to a new word (e.g., "RPCServer" -> "RPC_Server")
		// This occurs if the previous char was uppercase, current is uppercase, and the next is lowercase.
		isAcronymBoundary := unicode.IsUpper(prevRune) && unicode.IsUpper(r) &&
			i+1 < len(s) && unicode.IsLower(rune(s[i+1]))

		// Condition 3: Transition from a digit to an uppercase letter (e.g., "2023Foo" -> "2023_Foo")
		isDigitToUppercase := unicode.IsDigit(prevRune) && unicode.IsUpper(r)

		if isNewWordStart || isAcronymBoundary || isDigitToUppercase {
			builder.WriteRune('_')
		}
		builder.WriteRune(unicode.ToLower(r))
	}
	return builder.String()
}
