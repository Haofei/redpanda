package main

import "testing"

func TestToLower(t *testing.T) {
	testCases := map[string]string{
		"PascalCase":      "pascal_case",
		"RPCServer":       "rpc_server",
		"HTTPResponse":    "http_response",
		"ID":              "id",
		"MyID":            "my_id",
		"SimpleString":    "simple_string",
		"AnotherTest":     "another_test",
		"XMLHTTPRequest":  "xmlhttp_request", // How am I supposed to know?
		"APIClient":       "api_client",
		"URLParser":       "url_parser",
		"":                "",
		"A":               "a",
		"a":               "a",
		"CamelCaseString": "camel_case_string",
		"LongAcronymTEST": "long_acronym_test",
		"AcronymTESTWord": "acronym_test_word",
		"Edition2023Foo":  "edition2023_foo",
		"Foo123Bar":       "foo123_bar",
		"V1Beta1API":      "v1_beta1_api",
	}

	for input, expected := range testCases {
		result := pascalToSnakeCase(input)
		if result != expected {
			t.Errorf("converting %q to snake case, got: %q want: %q", input, result, expected)
		}
	}
}
