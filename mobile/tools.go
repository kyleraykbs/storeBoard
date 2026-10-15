//go:build tools

// gomobile, which binds this package for Android, generates code that imports
// golang.org/x/mobile/bind — so the requirement has to be in the module
// graph, and this file is what stops `go mod tidy` from dropping it. Nothing
// in a normal build, test or release imports it: the build tag keeps it out.

package mobile

import _ "golang.org/x/mobile/bind"
