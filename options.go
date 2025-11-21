package bdcache

import (
	"time"
)

// Options configures a Cache instance.
type Options struct {
	Persister      any
	MemorySize     int
	DefaultTTL     time.Duration
	WarmupLimit    int
	CleanupMaxAge  time.Duration
	CleanupEnabled bool
}

// Option is a functional option for configuring a Cache.
type Option func(*Options)

// WithMemorySize sets the maximum number of items in the memory cache.
func WithMemorySize(n int) Option {
	return func(o *Options) {
		o.MemorySize = n
	}
}

// WithDefaultTTL sets the default TTL for cache items.
func WithDefaultTTL(d time.Duration) Option {
	return func(o *Options) {
		o.DefaultTTL = d
	}
}

// WithPersistence sets the persistence layer for the cache.
// Pass a PersistenceLayer implementation from packages like:
//   - github.com/codeGROOVE-dev/bdcache/persist/localfs
//   - github.com/codeGROOVE-dev/bdcache/persist/datastore
//
// Example:
//
//	p, _ := localfs.New[string, int]("myapp")
//	cache, _ := bdcache.New[string, int](ctx, bdcache.WithPersistence(p))
func WithPersistence[K comparable, V any](p PersistenceLayer[K, V]) Option {
	return func(o *Options) {
		o.Persister = p
	}
}

// WithWarmup enables cache warmup by loading the N most recently updated entries from persistence on startup.
// By default, warmup is disabled (0). Set to a positive number to load that many entries.
func WithWarmup(n int) Option {
	return func(o *Options) {
		o.WarmupLimit = n
	}
}

// WithCleanup enables background cleanup of expired entries at startup.
// maxAge should be set to your maximum TTL value - entries older than this are deleted.
// This is a safety net for expired data and works alongside native Datastore TTL policies.
// If native TTL is properly configured, this cleanup will be fast (no-op).
func WithCleanup(maxAge time.Duration) Option {
	return func(o *Options) {
		o.CleanupEnabled = true
		o.CleanupMaxAge = maxAge
	}
}
