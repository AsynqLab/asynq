package dash

import "testing"

func TestTruncate(t *testing.T) {
	tests := []struct {
		s    string
		max  int
		want string
	}{
		{
			s:    "hello world!",
			max:  15,
			want: "hello world!",
		},
		{
			s:    "hello world!",
			max:  6,
			want: "hello…",
		},
	}

	for _, tc := range tests {
		got := truncate(tc.s, tc.max)
		if tc.want != got {
			t.Errorf("truncate(%q, %d) = %q, want %q", tc.s, tc.max, got, tc.want)
		}
	}
}
