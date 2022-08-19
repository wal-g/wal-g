// The MIT License (MIT)
//
// Copyright (c) 2016 Tevin Zhang
//
// Source: https://github.com/tevino/abool/
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package abool provides atomic Boolean type for cleaner code and
// better performance.
package abool

import "sync/atomic"

// New creates an AtomicBool with default set to false.
func New() *AtomicBool {
	return new(AtomicBool)
}

// NewBool creates an AtomicBool with given default value.
func NewBool(ok bool) *AtomicBool {
	ab := New()
	if ok {
		ab.Set()
	}
	return ab
}

// AtomicBool is an atomic Boolean.
// Its methods are all atomic, thus safe to be called by multiple goroutines simultaneously.
// Note: When embedding into a struct one should always use *AtomicBool to avoid copy.
type AtomicBool int32

// Set sets the Boolean to true.
func (ab *AtomicBool) Set() {
	atomic.StoreInt32((*int32)(ab), 1)
}

// UnSet sets the Boolean to false.
func (ab *AtomicBool) UnSet() {
	atomic.StoreInt32((*int32)(ab), 0)
}

// IsSet returns whether the Boolean is true.
func (ab *AtomicBool) IsSet() bool {
	return atomic.LoadInt32((*int32)(ab))&1 == 1
}

// IsNotSet returns whether the Boolean is false.
func (ab *AtomicBool) IsNotSet() bool {
	return !ab.IsSet()
}

// SetTo sets the boolean with given Boolean.
func (ab *AtomicBool) SetTo(yes bool) {
	if yes {
		atomic.StoreInt32((*int32)(ab), 1)
	} else {
		atomic.StoreInt32((*int32)(ab), 0)
	}
}

// Toggle inverts the Boolean then returns the value before inverting.
func (ab *AtomicBool) Toggle() bool {
	return atomic.AddInt32((*int32)(ab), 1)&1 == 0
}

// SetToIf sets the Boolean to new only if the Boolean matches the old.
// Returns whether the set was done.
func (ab *AtomicBool) SetToIf(old, new bool) (set bool) {
	var o, n int32
	if old {
		o = 1
	}
	if new {
		n = 1
	}
	return atomic.CompareAndSwapInt32((*int32)(ab), o, n)
}
