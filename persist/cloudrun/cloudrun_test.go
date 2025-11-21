package cloudrun

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNew_LocalFallback(t *testing.T) {
	ctx := context.Background()

	// Ensure K_SERVICE is not set
	oldVal := os.Getenv("K_SERVICE")
	os.Unsetenv("K_SERVICE")
	defer func() {
		if oldVal != "" {
			os.Setenv("K_SERVICE", oldVal)
		}
	}()

	p, err := New[string, string](ctx, "test-cache")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer p.Close()

	// Verify it's using local files by checking the Location format
	loc := p.Location("test-key")
	if !strings.Contains(loc, "/") && !strings.Contains(loc, "\\") {
		t.Errorf("expected file path in location, got: %s", loc)
	}
}

func TestNew_CloudRunWithoutDatastore(t *testing.T) {
	ctx := context.Background()

	// Set K_SERVICE to simulate Cloud Run
	oldVal := os.Getenv("K_SERVICE")
	os.Setenv("K_SERVICE", "test-service")
	defer func() {
		if oldVal != "" {
			os.Setenv("K_SERVICE", oldVal)
		} else {
			os.Unsetenv("K_SERVICE")
		}
	}()

	// This should try Datastore, fail (no credentials), then fall back to localfs
	p, err := New[string, string](ctx, "test-cache")
	if err != nil {
		t.Fatalf("New() should fall back to localfs even when datastore fails: %v", err)
	}
	defer p.Close()

	// Verify it fell back to local files
	loc := p.Location("test-key")
	if !strings.Contains(loc, "/") && !strings.Contains(loc, "\\") {
		t.Errorf("expected file path in location after fallback, got: %s", loc)
	}
}

func TestNew_BasicOperations(t *testing.T) {
	ctx := context.Background()

	p, err := New[string, int](ctx, "test-ops")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer p.Close()

	// Test basic store/load cycle
	key := "answer"
	value := 42

	err = p.Store(ctx, key, value, time.Time{})
	if err != nil {
		t.Fatalf("Store() failed: %v", err)
	}

	got, _, found, err := p.Load(ctx, key)
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}
	if !found {
		t.Fatal("Load() should find stored value")
	}
	if got != value {
		t.Errorf("Load() = %v, want %v", got, value)
	}
}
