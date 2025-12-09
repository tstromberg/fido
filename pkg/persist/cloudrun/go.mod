module github.com/codeGROOVE-dev/sfcache/pkg/persist/cloudrun

go 1.25.4

require (
	github.com/codeGROOVE-dev/sfcache/pkg/persist/datastore v1.3.0
	github.com/codeGROOVE-dev/sfcache/pkg/persist/localfs v1.3.0
)

require github.com/codeGROOVE-dev/ds9 v0.8.0 // indirect

replace github.com/codeGROOVE-dev/sfcache/pkg/persist/datastore => ../datastore

replace github.com/codeGROOVE-dev/sfcache/pkg/persist/localfs => ../localfs
