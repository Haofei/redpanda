package kafka

import "testing"

func Test_oauthBearerToken(t *testing.T) {
	tests := []struct {
		name     string
		password string
		want     string
	}{
		{
			name:     "token prefix stripped",
			password: "token:eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test",
			want:     "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test",
		},
		{
			name:     "raw token returned as-is",
			password: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test",
			want:     "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test",
		},
		{
			name:     "empty password returns empty",
			password: "",
			want:     "",
		},
		{
			name:     "token prefix only returns empty",
			password: "token:",
			want:     "",
		},
		{
			name:     "token prefix is case-sensitive",
			password: "Token:abc",
			want:     "Token:abc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := oauthBearerToken(tt.password)
			if got != tt.want {
				t.Errorf("oauthBearerToken(%q) = %q, want %q", tt.password, got, tt.want)
			}
		})
	}
}
