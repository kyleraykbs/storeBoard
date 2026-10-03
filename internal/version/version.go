// Package version carries the build's version string.
package version

// Version is the storeboard version. A plain `go build` leaves it "dev"; the
// Nix build stamps the flake's version through -X ldflags.
var Version = "dev"
