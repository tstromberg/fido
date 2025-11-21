module github.com/codeGROOVE-dev/bdcache/persist/cloudrun

go 1.25.4

require (
	github.com/codeGROOVE-dev/bdcache v0.0.0
	github.com/codeGROOVE-dev/bdcache/persist/datastore v0.0.0
	github.com/codeGROOVE-dev/bdcache/persist/localfs v0.0.0
)

require github.com/codeGROOVE-dev/ds9 v0.7.1 // indirect

replace github.com/codeGROOVE-dev/bdcache => ../..

replace github.com/codeGROOVE-dev/bdcache/persist/datastore => ../datastore

replace github.com/codeGROOVE-dev/bdcache/persist/localfs => ../localfs
