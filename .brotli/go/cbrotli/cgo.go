// Copyright 2017 Google Inc. All Rights Reserved.
//
// Distributed under MIT license.
// See file LICENSE for detail or copy at https://opensource.org/licenses/MIT

package cbrotli

// Inform golang build system that it should link brotli libraries.

// #cgo CFLAGS: -I../../c/include
// #cgo LDFLAGS: -L../../dist -lbrotlidec-static -lbrotlicommon-static
// #cgo LDFLAGS: -L../../dist -lbrotlienc-static -lbrotlicommon-static -lm
import "C"
