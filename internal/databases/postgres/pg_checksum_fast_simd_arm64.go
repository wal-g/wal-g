//go:build goexperiment.simd && arm64

package postgres

import (
	"simd/archsimd"
	"unsafe"
)

// pgChecksumBlockFast calculates all 32 independent FNV sums four at a time.
//
// Advanced SIMD (NEON) is required by the ARM64 architecture, so unlike the
// AMD64 implementation there is no runtime feature check to make here.
func pgChecksumBlockFast(page *PgDatabasePage) uint32 {
	sums := [8]archsimd.Uint32x4{
		archsimd.LoadUint32x4Array((*[4]uint32)(unsafe.Pointer(&checksumBaseOffsets[0]))),
		archsimd.LoadUint32x4Array((*[4]uint32)(unsafe.Pointer(&checksumBaseOffsets[4]))),
		archsimd.LoadUint32x4Array((*[4]uint32)(unsafe.Pointer(&checksumBaseOffsets[8]))),
		archsimd.LoadUint32x4Array((*[4]uint32)(unsafe.Pointer(&checksumBaseOffsets[12]))),
		archsimd.LoadUint32x4Array((*[4]uint32)(unsafe.Pointer(&checksumBaseOffsets[16]))),
		archsimd.LoadUint32x4Array((*[4]uint32)(unsafe.Pointer(&checksumBaseOffsets[20]))),
		archsimd.LoadUint32x4Array((*[4]uint32)(unsafe.Pointer(&checksumBaseOffsets[24]))),
		archsimd.LoadUint32x4Array((*[4]uint32)(unsafe.Pointer(&checksumBaseOffsets[28]))),
	}
	pageForChecksum := *(*PgChecksummablePage)(unsafe.Pointer(page))
	hashIterationsCount := DatabasePageSize / int64(NSums*sizeofInt32)
	prime := archsimd.LoadUint32x4Array(&[4]uint32{FnvPrime, FnvPrime, FnvPrime, FnvPrime})

	for i := int64(0); i < hashIterationsCount; i++ {
		row := pageForChecksum[i]
		for group := range sums {
			values := archsimd.LoadUint32x4Array((*[4]uint32)(unsafe.Pointer(&row[group*4])))
			tmp := sums[group].Xor(values)
			sums[group] = tmp.Mul(prime).Xor(tmp.ShiftAllRight(17))
		}
	}

	zero := archsimd.LoadUint32x4Array(&[4]uint32{})
	for i := 0; i < 2; i++ {
		for group := range sums {
			tmp := sums[group].Xor(zero)
			sums[group] = tmp.Mul(prime).Xor(tmp.ShiftAllRight(17))
		}
	}

	var result uint32
	for group := range sums {
		var values [4]uint32
		sums[group].StoreArray(&values)
		for _, value := range values {
			result ^= value
		}
	}
	return result
}
