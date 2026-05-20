package control

import (
	"testing"
	"time"
)

func TestMigrationTaskNextRetryUnix(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)

	if got := migrationTaskNextRetryUnix(now, -1*time.Second); got != now.Unix() {
		t.Fatalf("retry after -1s = %d, want %d", got, now.Unix())
	}

	if got := migrationTaskNextRetryUnix(now, 0); got != now.Unix() {
		t.Fatalf("retry after 0s = %d, want %d", got, now.Unix())
	}

	if got := migrationTaskNextRetryUnix(now, 300*time.Millisecond); got != now.Add(time.Second).Unix() {
		t.Fatalf("retry after 300ms = %d, want %d", got, now.Add(time.Second).Unix())
	}

	if got := migrationTaskNextRetryUnix(now, 2*time.Second); got != now.Add(2*time.Second).Unix() {
		t.Fatalf("retry after 2s = %d, want %d", got, now.Add(2*time.Second).Unix())
	}
}
