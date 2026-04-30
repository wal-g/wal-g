//go:build !windows
// +build !windows

package postgres

import "syscall"

const SIGUSR1 = syscall.SIGUSR1
