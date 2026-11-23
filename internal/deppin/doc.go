// Package deppin keeps modules in go.mod that no package imports yet.
//
// It is temporary. The stage 2 services (normalize and conv) will import the
// Redis client and the in-memory Redis for tests. Until they do, go mod tidy
// would remove both modules, so pin.go imports them under the build tag
// deppin. Delete this package once a real package imports both modules.
package deppin
