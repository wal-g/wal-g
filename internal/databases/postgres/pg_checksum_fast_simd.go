//go:build goexperiment.simd && amd64

package postgres

import (
	"simd/archsimd"
	"unsafe"
)

func pgChecksumBlockFast(page *PgDatabasePage) uint32 {
	if !archsimd.X86.AVX2() {
		return pgChecksumBlockScalar(page)
	}

	sums := [4]archsimd.Uint32x8{
		archsimd.LoadUint32x8Array((*[8]uint32)(unsafe.Pointer(&checksumBaseOffsets[0]))),
		archsimd.LoadUint32x8Array((*[8]uint32)(unsafe.Pointer(&checksumBaseOffsets[8]))),
		archsimd.LoadUint32x8Array((*[8]uint32)(unsafe.Pointer(&checksumBaseOffsets[16]))),
		archsimd.LoadUint32x8Array((*[8]uint32)(unsafe.Pointer(&checksumBaseOffsets[24]))),
	}
	pageForChecksum := *(*PgChecksummablePage)(unsafe.Pointer(page))
	hashIterationsCount := DatabasePageSize / int64(NSums*sizeofInt32)
	prime := archsimd.LoadUint32x8Array(&[8]uint32{FnvPrime, FnvPrime, FnvPrime, FnvPrime, FnvPrime, FnvPrime, FnvPrime, FnvPrime})
	shift := archsimd.LoadUint32x8Array(&[8]uint32{17, 17, 17, 17, 17, 17, 17, 17})

	for i := int64(0); i < hashIterationsCount; i++ {
		row := pageForChecksum[i]
		for group := 0; group < len(sums); group++ {
			values := archsimd.LoadUint32x8Array((*[8]uint32)(unsafe.Pointer(&row[group*8])))
			tmp := sums[group].Xor(values)
			sums[group] = tmp.Mul(prime).Xor(tmp.ShiftRight(shift))
		}
	}

	zero := archsimd.LoadUint32x8Array(&[8]uint32{})
	for i := 0; i < 2; i++ {
		for group := range sums {
			tmp := sums[group].Xor(zero)
			sums[group] = tmp.Mul(prime).Xor(tmp.ShiftRight(shift))
		}
	}

	var folded [8]uint32
	for group := range sums {
		var values [8]uint32
		sums[group].StoreArray(&values)
		for lane, value := range values {
			folded[lane] ^= value
		}
	}
	var result uint32
	for _, value := range folded {
		result ^= value
	}
	return result
}
