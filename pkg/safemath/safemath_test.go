package safemath_test

import (
	"math"
	"testing"

	"github.com/RyanW02/vectory/pkg/safemath"
	"github.com/stretchr/testify/assert"
)

// checkOK asserts a successful conversion: no error and correct value.
func checkOK[T comparable](t *testing.T, got T, err error, want T) {
	t.Helper()
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

// checkFail asserts a failed conversion: error returned and zero value.
func checkFail[T comparable](t *testing.T, got T, err error) {
	t.Helper()
	assert.Error(t, err)
	var zero T
	assert.Equal(t, zero, got)
}

// =============================================================================
// Unsigned → int
// =============================================================================

func TestUint64ToInt(t *testing.T) {
	got, err := safemath.Uint64ToInt(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToInt(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint64ToInt(math.MaxInt)
	checkOK(t, got, err, math.MaxInt)
	got, err = safemath.Uint64ToInt(uint64(math.MaxInt) + 1)
	checkFail(t, got, err)
}

func TestUint32ToInt(t *testing.T) {
	// On 64-bit uint32 always fits in int.
	got, err := safemath.Uint32ToInt(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint32ToInt(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint32ToInt(math.MaxUint32)
	checkOK(t, got, err, math.MaxUint32)
}

func TestUint16ToInt(t *testing.T) {
	got, err := safemath.Uint16ToInt(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint16ToInt(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint16ToInt(math.MaxUint16)
	checkOK(t, got, err, math.MaxUint16)
}

func TestUint8ToInt(t *testing.T) {
	got, err := safemath.Uint8ToInt(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint8ToInt(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Uint8ToInt(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
}

func TestUintToInt(t *testing.T) {
	got, err := safemath.UintToInt(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintToInt(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.UintToInt(math.MaxInt)
	checkOK(t, got, err, math.MaxInt)
	got, err = safemath.UintToInt(uint(math.MaxInt) + 1)
	checkFail(t, got, err)
}

func TestUintptrToInt(t *testing.T) {
	got, err := safemath.UintptrToInt(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintptrToInt(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.UintptrToInt(math.MaxInt)
	checkOK(t, got, err, math.MaxInt)
	got, err = safemath.UintptrToInt(uintptr(math.MaxInt) + 1)
	checkFail(t, got, err)
}

// =============================================================================
// Unsigned → int64
// =============================================================================

func TestUint64ToInt64(t *testing.T) {
	got, err := safemath.Uint64ToInt64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToInt64(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint64ToInt64(math.MaxInt64)
	checkOK(t, got, err, math.MaxInt64)
	got, err = safemath.Uint64ToInt64(uint64(math.MaxInt64) + 1)
	checkFail(t, got, err)
}

func TestUint32ToInt64(t *testing.T) {
	got, err := safemath.Uint32ToInt64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint32ToInt64(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint32ToInt64(math.MaxUint32)
	checkOK(t, got, err, math.MaxUint32)
}

func TestUint16ToInt64(t *testing.T) {
	got, err := safemath.Uint16ToInt64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint16ToInt64(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint16ToInt64(math.MaxUint16)
	checkOK(t, got, err, math.MaxUint16)
}

func TestUint8ToInt64(t *testing.T) {
	got, err := safemath.Uint8ToInt64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint8ToInt64(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Uint8ToInt64(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
}

func TestUintToInt64(t *testing.T) {
	got, err := safemath.UintToInt64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintToInt64(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.UintToInt64(math.MaxInt64)
	checkOK(t, got, err, math.MaxInt64)
	got, err = safemath.UintToInt64(uint(math.MaxInt64) + 1)
	checkFail(t, got, err)
}

func TestUintptrToInt64(t *testing.T) {
	got, err := safemath.UintptrToInt64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintptrToInt64(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.UintptrToInt64(math.MaxInt64)
	checkOK(t, got, err, math.MaxInt64)
	got, err = safemath.UintptrToInt64(uintptr(math.MaxInt64) + 1)
	checkFail(t, got, err)
}

// =============================================================================
// Unsigned → int32
// =============================================================================

func TestUint64ToInt32(t *testing.T) {
	got, err := safemath.Uint64ToInt32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToInt32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint64ToInt32(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.Uint64ToInt32(math.MaxInt32 + 1)
	checkFail(t, got, err)
}

func TestUint32ToInt32(t *testing.T) {
	got, err := safemath.Uint32ToInt32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint32ToInt32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint32ToInt32(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.Uint32ToInt32(math.MaxInt32 + 1)
	checkFail(t, got, err)
}

func TestUint16ToInt32(t *testing.T) {
	got, err := safemath.Uint16ToInt32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint16ToInt32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint16ToInt32(math.MaxUint16)
	checkOK(t, got, err, math.MaxUint16)
}

func TestUint8ToInt32(t *testing.T) {
	got, err := safemath.Uint8ToInt32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint8ToInt32(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Uint8ToInt32(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
}

func TestUintToInt32(t *testing.T) {
	got, err := safemath.UintToInt32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintToInt32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.UintToInt32(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.UintToInt32(math.MaxInt32 + 1)
	checkFail(t, got, err)
}

func TestUintptrToInt32(t *testing.T) {
	got, err := safemath.UintptrToInt32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintptrToInt32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.UintptrToInt32(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.UintptrToInt32(math.MaxInt32 + 1)
	checkFail(t, got, err)
}

// =============================================================================
// Unsigned → int16
// =============================================================================

func TestUint64ToInt16(t *testing.T) {
	got, err := safemath.Uint64ToInt16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToInt16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint64ToInt16(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Uint64ToInt16(math.MaxInt16 + 1)
	checkFail(t, got, err)
}

func TestUint32ToInt16(t *testing.T) {
	got, err := safemath.Uint32ToInt16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint32ToInt16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint32ToInt16(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Uint32ToInt16(math.MaxInt16 + 1)
	checkFail(t, got, err)
}

func TestUint16ToInt16(t *testing.T) {
	got, err := safemath.Uint16ToInt16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint16ToInt16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint16ToInt16(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Uint16ToInt16(math.MaxInt16 + 1)
	checkFail(t, got, err)
}

func TestUint8ToInt16(t *testing.T) {
	got, err := safemath.Uint8ToInt16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint8ToInt16(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Uint8ToInt16(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
}

func TestUintToInt16(t *testing.T) {
	got, err := safemath.UintToInt16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintToInt16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.UintToInt16(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.UintToInt16(math.MaxInt16 + 1)
	checkFail(t, got, err)
}

func TestUintptrToInt16(t *testing.T) {
	got, err := safemath.UintptrToInt16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintptrToInt16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.UintptrToInt16(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.UintptrToInt16(math.MaxInt16 + 1)
	checkFail(t, got, err)
}

// =============================================================================
// Unsigned → int8
// =============================================================================

func TestUint64ToInt8(t *testing.T) {
	got, err := safemath.Uint64ToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToInt8(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Uint64ToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Uint64ToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
}

func TestUint32ToInt8(t *testing.T) {
	got, err := safemath.Uint32ToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint32ToInt8(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Uint32ToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Uint32ToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
}

func TestUint16ToInt8(t *testing.T) {
	got, err := safemath.Uint16ToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint16ToInt8(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Uint16ToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Uint16ToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
}

func TestUint8ToInt8(t *testing.T) {
	got, err := safemath.Uint8ToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint8ToInt8(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Uint8ToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Uint8ToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
}

func TestUintToInt8(t *testing.T) {
	got, err := safemath.UintToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintToInt8(50)
	checkOK(t, got, err, 50)
	got, err = safemath.UintToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.UintToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
}

func TestUintptrToInt8(t *testing.T) {
	got, err := safemath.UintptrToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.UintptrToInt8(50)
	checkOK(t, got, err, 50)
	got, err = safemath.UintptrToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.UintptrToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
}

// =============================================================================
// Signed → uint64
// =============================================================================

func TestIntToUint64(t *testing.T) {
	got, err := safemath.IntToUint64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.IntToUint64(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.IntToUint64(math.MaxInt)
	checkOK(t, got, err, math.MaxInt)
	got, err = safemath.IntToUint64(-1)
	checkFail(t, got, err)
	got, err = safemath.IntToUint64(math.MinInt)
	checkFail(t, got, err)
}

func TestInt64ToUint64(t *testing.T) {
	got, err := safemath.Int64ToUint64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToUint64(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int64ToUint64(math.MaxInt64)
	checkOK(t, got, err, math.MaxInt64)
	got, err = safemath.Int64ToUint64(-1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToUint64(math.MinInt64)
	checkFail(t, got, err)
}

func TestInt32ToUint64(t *testing.T) {
	got, err := safemath.Int32ToUint64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int32ToUint64(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int32ToUint64(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.Int32ToUint64(-1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToUint64(math.MinInt32)
	checkFail(t, got, err)
}

func TestInt16ToUint64(t *testing.T) {
	got, err := safemath.Int16ToUint64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int16ToUint64(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int16ToUint64(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Int16ToUint64(-1)
	checkFail(t, got, err)
	got, err = safemath.Int16ToUint64(math.MinInt16)
	checkFail(t, got, err)
}

func TestInt8ToUint64(t *testing.T) {
	got, err := safemath.Int8ToUint64(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int8ToUint64(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Int8ToUint64(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Int8ToUint64(-1)
	checkFail(t, got, err)
	got, err = safemath.Int8ToUint64(math.MinInt8)
	checkFail(t, got, err)
}

// =============================================================================
// Signed → uint32
// =============================================================================

func TestIntToUint32(t *testing.T) {
	got, err := safemath.IntToUint32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.IntToUint32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.IntToUint32(math.MaxUint32)
	checkOK(t, got, err, math.MaxUint32)
	got, err = safemath.IntToUint32(math.MaxUint32 + 1)
	checkFail(t, got, err)
	got, err = safemath.IntToUint32(-1)
	checkFail(t, got, err)
	got, err = safemath.IntToUint32(math.MinInt)
	checkFail(t, got, err)
}

func TestInt64ToUint32(t *testing.T) {
	got, err := safemath.Int64ToUint32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToUint32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int64ToUint32(math.MaxUint32)
	checkOK(t, got, err, math.MaxUint32)
	got, err = safemath.Int64ToUint32(math.MaxUint32 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToUint32(-1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToUint32(math.MinInt64)
	checkFail(t, got, err)
}

func TestInt32ToUint32(t *testing.T) {
	got, err := safemath.Int32ToUint32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int32ToUint32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int32ToUint32(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.Int32ToUint32(-1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToUint32(math.MinInt32)
	checkFail(t, got, err)
}

func TestInt16ToUint32(t *testing.T) {
	got, err := safemath.Int16ToUint32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int16ToUint32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int16ToUint32(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Int16ToUint32(-1)
	checkFail(t, got, err)
	got, err = safemath.Int16ToUint32(math.MinInt16)
	checkFail(t, got, err)
}

func TestInt8ToUint32(t *testing.T) {
	got, err := safemath.Int8ToUint32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int8ToUint32(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Int8ToUint32(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Int8ToUint32(-1)
	checkFail(t, got, err)
	got, err = safemath.Int8ToUint32(math.MinInt8)
	checkFail(t, got, err)
}

// =============================================================================
// Signed → uint16
// =============================================================================

func TestIntToUint16(t *testing.T) {
	got, err := safemath.IntToUint16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.IntToUint16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.IntToUint16(math.MaxUint16)
	checkOK(t, got, err, math.MaxUint16)
	got, err = safemath.IntToUint16(math.MaxUint16 + 1)
	checkFail(t, got, err)
	got, err = safemath.IntToUint16(-1)
	checkFail(t, got, err)
	got, err = safemath.IntToUint16(math.MinInt)
	checkFail(t, got, err)
}

func TestInt64ToUint16(t *testing.T) {
	got, err := safemath.Int64ToUint16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToUint16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int64ToUint16(math.MaxUint16)
	checkOK(t, got, err, math.MaxUint16)
	got, err = safemath.Int64ToUint16(math.MaxUint16 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToUint16(-1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToUint16(math.MinInt64)
	checkFail(t, got, err)
}

func TestInt32ToUint16(t *testing.T) {
	got, err := safemath.Int32ToUint16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int32ToUint16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int32ToUint16(math.MaxUint16)
	checkOK(t, got, err, math.MaxUint16)
	got, err = safemath.Int32ToUint16(math.MaxUint16 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToUint16(-1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToUint16(math.MinInt32)
	checkFail(t, got, err)
}

func TestInt16ToUint16(t *testing.T) {
	got, err := safemath.Int16ToUint16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int16ToUint16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int16ToUint16(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Int16ToUint16(-1)
	checkFail(t, got, err)
	got, err = safemath.Int16ToUint16(math.MinInt16)
	checkFail(t, got, err)
}

func TestInt8ToUint16(t *testing.T) {
	got, err := safemath.Int8ToUint16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int8ToUint16(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Int8ToUint16(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Int8ToUint16(-1)
	checkFail(t, got, err)
	got, err = safemath.Int8ToUint16(math.MinInt8)
	checkFail(t, got, err)
}

// =============================================================================
// Signed → uint8
// =============================================================================

func TestIntToUint8(t *testing.T) {
	got, err := safemath.IntToUint8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.IntToUint8(100)
	checkOK(t, got, err, 100)
	got, err = safemath.IntToUint8(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
	got, err = safemath.IntToUint8(math.MaxUint8 + 1)
	checkFail(t, got, err)
	got, err = safemath.IntToUint8(-1)
	checkFail(t, got, err)
	got, err = safemath.IntToUint8(math.MinInt)
	checkFail(t, got, err)
}

func TestInt64ToUint8(t *testing.T) {
	got, err := safemath.Int64ToUint8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToUint8(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Int64ToUint8(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
	got, err = safemath.Int64ToUint8(math.MaxUint8 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToUint8(-1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToUint8(math.MinInt64)
	checkFail(t, got, err)
}

func TestInt32ToUint8(t *testing.T) {
	got, err := safemath.Int32ToUint8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int32ToUint8(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Int32ToUint8(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
	got, err = safemath.Int32ToUint8(math.MaxUint8 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToUint8(-1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToUint8(math.MinInt32)
	checkFail(t, got, err)
}

func TestInt16ToUint8(t *testing.T) {
	got, err := safemath.Int16ToUint8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int16ToUint8(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Int16ToUint8(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
	got, err = safemath.Int16ToUint8(math.MaxUint8 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int16ToUint8(-1)
	checkFail(t, got, err)
	got, err = safemath.Int16ToUint8(math.MinInt16)
	checkFail(t, got, err)
}

func TestInt8ToUint8(t *testing.T) {
	got, err := safemath.Int8ToUint8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int8ToUint8(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Int8ToUint8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Int8ToUint8(-1)
	checkFail(t, got, err)
	got, err = safemath.Int8ToUint8(math.MinInt8)
	checkFail(t, got, err)
}

// =============================================================================
// Signed → uint / uintptr
// =============================================================================

func TestIntToUint(t *testing.T) {
	got, err := safemath.IntToUint(0)
	checkOK(t, got, err, 0)
	got, err = safemath.IntToUint(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.IntToUint(math.MaxInt)
	checkOK(t, got, err, math.MaxInt)
	got, err = safemath.IntToUint(-1)
	checkFail(t, got, err)
	got, err = safemath.IntToUint(math.MinInt)
	checkFail(t, got, err)
}

func TestIntToUintptr(t *testing.T) {
	got, err := safemath.IntToUintptr(0)
	checkOK(t, got, err, 0)
	got, err = safemath.IntToUintptr(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.IntToUintptr(math.MaxInt)
	checkOK(t, got, err, math.MaxInt)
	got, err = safemath.IntToUintptr(-1)
	checkFail(t, got, err)
	got, err = safemath.IntToUintptr(math.MinInt)
	checkFail(t, got, err)
}

func TestInt64ToUint(t *testing.T) {
	got, err := safemath.Int64ToUint(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToUint(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int64ToUint(math.MaxInt64)
	checkOK(t, got, err, math.MaxInt64)
	got, err = safemath.Int64ToUint(-1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToUint(math.MinInt64)
	checkFail(t, got, err)
}

func TestInt64ToUintptr(t *testing.T) {
	got, err := safemath.Int64ToUintptr(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToUintptr(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int64ToUintptr(math.MaxInt64)
	checkOK(t, got, err, math.MaxInt64)
	got, err = safemath.Int64ToUintptr(-1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToUintptr(math.MinInt64)
	checkFail(t, got, err)
}

func TestInt32ToUint(t *testing.T) {
	got, err := safemath.Int32ToUint(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int32ToUint(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int32ToUint(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.Int32ToUint(-1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToUint(math.MinInt32)
	checkFail(t, got, err)
}

func TestInt32ToUintptr(t *testing.T) {
	got, err := safemath.Int32ToUintptr(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int32ToUintptr(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int32ToUintptr(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.Int32ToUintptr(-1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToUintptr(math.MinInt32)
	checkFail(t, got, err)
}

func TestInt16ToUint(t *testing.T) {
	got, err := safemath.Int16ToUint(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int16ToUint(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int16ToUint(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Int16ToUint(-1)
	checkFail(t, got, err)
	got, err = safemath.Int16ToUint(math.MinInt16)
	checkFail(t, got, err)
}

func TestInt16ToUintptr(t *testing.T) {
	got, err := safemath.Int16ToUintptr(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int16ToUintptr(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Int16ToUintptr(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Int16ToUintptr(-1)
	checkFail(t, got, err)
	got, err = safemath.Int16ToUintptr(math.MinInt16)
	checkFail(t, got, err)
}

func TestInt8ToUint(t *testing.T) {
	got, err := safemath.Int8ToUint(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int8ToUint(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Int8ToUint(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Int8ToUint(-1)
	checkFail(t, got, err)
	got, err = safemath.Int8ToUint(math.MinInt8)
	checkFail(t, got, err)
}

func TestInt8ToUintptr(t *testing.T) {
	got, err := safemath.Int8ToUintptr(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int8ToUintptr(50)
	checkOK(t, got, err, 50)
	got, err = safemath.Int8ToUintptr(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Int8ToUintptr(-1)
	checkFail(t, got, err)
	got, err = safemath.Int8ToUintptr(math.MinInt8)
	checkFail(t, got, err)
}

// =============================================================================
// Signed narrowing
// =============================================================================

func TestInt64ToInt(t *testing.T) {
	// On 64-bit int == int64; all values fit.
	got, err := safemath.Int64ToInt(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToInt(1)
	checkOK(t, got, err, 1)
	got, err = safemath.Int64ToInt(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.Int64ToInt(math.MaxInt)
	checkOK(t, got, err, math.MaxInt)
	got, err = safemath.Int64ToInt(math.MinInt)
	checkOK(t, got, err, math.MinInt)
}

func TestInt64ToInt32(t *testing.T) {
	got, err := safemath.Int64ToInt32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToInt32(1)
	checkOK(t, got, err, 1)
	got, err = safemath.Int64ToInt32(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.Int64ToInt32(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.Int64ToInt32(math.MinInt32)
	checkOK(t, got, err, math.MinInt32)
	got, err = safemath.Int64ToInt32(math.MaxInt32 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToInt32(math.MinInt32 - 1)
	checkFail(t, got, err)
}

func TestInt64ToInt16(t *testing.T) {
	got, err := safemath.Int64ToInt16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToInt16(1)
	checkOK(t, got, err, 1)
	got, err = safemath.Int64ToInt16(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.Int64ToInt16(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Int64ToInt16(math.MinInt16)
	checkOK(t, got, err, math.MinInt16)
	got, err = safemath.Int64ToInt16(math.MaxInt16 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToInt16(math.MinInt16 - 1)
	checkFail(t, got, err)
}

func TestInt64ToInt8(t *testing.T) {
	got, err := safemath.Int64ToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int64ToInt8(1)
	checkOK(t, got, err, 1)
	got, err = safemath.Int64ToInt8(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.Int64ToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Int64ToInt8(math.MinInt8)
	checkOK(t, got, err, math.MinInt8)
	got, err = safemath.Int64ToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int64ToInt8(math.MinInt8 - 1)
	checkFail(t, got, err)
}

func TestIntToInt32(t *testing.T) {
	got, err := safemath.IntToInt32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.IntToInt32(1)
	checkOK(t, got, err, 1)
	got, err = safemath.IntToInt32(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.IntToInt32(math.MaxInt32)
	checkOK(t, got, err, math.MaxInt32)
	got, err = safemath.IntToInt32(math.MinInt32)
	checkOK(t, got, err, math.MinInt32)
	got, err = safemath.IntToInt32(math.MaxInt32 + 1)
	checkFail(t, got, err)
	got, err = safemath.IntToInt32(math.MinInt32 - 1)
	checkFail(t, got, err)
}

func TestIntToInt16(t *testing.T) {
	got, err := safemath.IntToInt16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.IntToInt16(1)
	checkOK(t, got, err, 1)
	got, err = safemath.IntToInt16(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.IntToInt16(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.IntToInt16(math.MinInt16)
	checkOK(t, got, err, math.MinInt16)
	got, err = safemath.IntToInt16(math.MaxInt16 + 1)
	checkFail(t, got, err)
	got, err = safemath.IntToInt16(math.MinInt16 - 1)
	checkFail(t, got, err)
}

func TestIntToInt8(t *testing.T) {
	got, err := safemath.IntToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.IntToInt8(1)
	checkOK(t, got, err, 1)
	got, err = safemath.IntToInt8(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.IntToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.IntToInt8(math.MinInt8)
	checkOK(t, got, err, math.MinInt8)
	got, err = safemath.IntToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
	got, err = safemath.IntToInt8(math.MinInt8 - 1)
	checkFail(t, got, err)
}

func TestInt32ToInt16(t *testing.T) {
	got, err := safemath.Int32ToInt16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int32ToInt16(1)
	checkOK(t, got, err, 1)
	got, err = safemath.Int32ToInt16(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.Int32ToInt16(math.MaxInt16)
	checkOK(t, got, err, math.MaxInt16)
	got, err = safemath.Int32ToInt16(math.MinInt16)
	checkOK(t, got, err, math.MinInt16)
	got, err = safemath.Int32ToInt16(math.MaxInt16 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToInt16(math.MinInt16 - 1)
	checkFail(t, got, err)
}

func TestInt32ToInt8(t *testing.T) {
	got, err := safemath.Int32ToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int32ToInt8(1)
	checkOK(t, got, err, 1)
	got, err = safemath.Int32ToInt8(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.Int32ToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Int32ToInt8(math.MinInt8)
	checkOK(t, got, err, math.MinInt8)
	got, err = safemath.Int32ToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int32ToInt8(math.MinInt8 - 1)
	checkFail(t, got, err)
}

func TestInt16ToInt8(t *testing.T) {
	got, err := safemath.Int16ToInt8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Int16ToInt8(1)
	checkOK(t, got, err, 1)
	got, err = safemath.Int16ToInt8(-1)
	checkOK(t, got, err, -1)
	got, err = safemath.Int16ToInt8(math.MaxInt8)
	checkOK(t, got, err, math.MaxInt8)
	got, err = safemath.Int16ToInt8(math.MinInt8)
	checkOK(t, got, err, math.MinInt8)
	got, err = safemath.Int16ToInt8(math.MaxInt8 + 1)
	checkFail(t, got, err)
	got, err = safemath.Int16ToInt8(math.MinInt8 - 1)
	checkFail(t, got, err)
}

// =============================================================================
// Unsigned narrowing
// =============================================================================

func TestUint64ToUint32(t *testing.T) {
	got, err := safemath.Uint64ToUint32(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToUint32(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint64ToUint32(math.MaxUint32)
	checkOK(t, got, err, math.MaxUint32)
	got, err = safemath.Uint64ToUint32(math.MaxUint32 + 1)
	checkFail(t, got, err)
}

func TestUint64ToUint16(t *testing.T) {
	got, err := safemath.Uint64ToUint16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToUint16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint64ToUint16(math.MaxUint16)
	checkOK(t, got, err, math.MaxUint16)
	got, err = safemath.Uint64ToUint16(math.MaxUint16 + 1)
	checkFail(t, got, err)
}

func TestUint64ToUint8(t *testing.T) {
	got, err := safemath.Uint64ToUint8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToUint8(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Uint64ToUint8(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
	got, err = safemath.Uint64ToUint8(math.MaxUint8 + 1)
	checkFail(t, got, err)
}

func TestUint64ToUint(t *testing.T) {
	// On 64-bit uint == uint64; always succeeds.
	got, err := safemath.Uint64ToUint(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToUint(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint64ToUint(math.MaxUint64)
	checkOK(t, got, err, math.MaxUint64)
}

func TestUint64ToUintptr(t *testing.T) {
	// On 64-bit uintptr == uint64; always succeeds.
	got, err := safemath.Uint64ToUintptr(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint64ToUintptr(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint64ToUintptr(math.MaxUint64)
	checkOK(t, got, err, math.MaxUint64)
}

func TestUint32ToUint16(t *testing.T) {
	got, err := safemath.Uint32ToUint16(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint32ToUint16(1000)
	checkOK(t, got, err, 1000)
	got, err = safemath.Uint32ToUint16(math.MaxUint16)
	checkOK(t, got, err, math.MaxUint16)
	got, err = safemath.Uint32ToUint16(math.MaxUint16 + 1)
	checkFail(t, got, err)
}

func TestUint32ToUint8(t *testing.T) {
	got, err := safemath.Uint32ToUint8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint32ToUint8(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Uint32ToUint8(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
	got, err = safemath.Uint32ToUint8(math.MaxUint8 + 1)
	checkFail(t, got, err)
}

func TestUint16ToUint8(t *testing.T) {
	got, err := safemath.Uint16ToUint8(0)
	checkOK(t, got, err, 0)
	got, err = safemath.Uint16ToUint8(100)
	checkOK(t, got, err, 100)
	got, err = safemath.Uint16ToUint8(math.MaxUint8)
	checkOK(t, got, err, math.MaxUint8)
	got, err = safemath.Uint16ToUint8(math.MaxUint8 + 1)
	checkFail(t, got, err)
}
