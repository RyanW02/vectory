package safemath

import (
	"fmt"
	"math"
)

// -----------------------------------------------------------------------------
// Concrete conversions — unsigned → int
// -----------------------------------------------------------------------------

func Uint64ToInt(v uint64) (int, error) {
	if v > math.MaxInt {
		return 0, fmt.Errorf("safeconv: %d overflows int (max %d)", v, math.MaxInt)
	}
	return int(v), nil
}

func Uint32ToInt(v uint32) (int, error) {
	if uint(v) > ^uint(0)>>1 {
		return 0, fmt.Errorf("safeconv: %d overflows int (max %d)", v, int(^uint(0)>>1))
	}
	return int(v), nil
}

func Uint16ToInt(v uint16) (int, error) {
	return int(v), nil
}

func Uint8ToInt(v uint8) (int, error) {
	return int(v), nil
}

func UintToInt(v uint) (int, error) {
	if v > math.MaxInt {
		return 0, fmt.Errorf("safeconv: %d overflows int (max %d)", v, math.MaxInt)
	}
	return int(v), nil
}

func UintptrToInt(v uintptr) (int, error) {
	if v > math.MaxInt {
		return 0, fmt.Errorf("safeconv: %d overflows int (max %d)", v, math.MaxInt)
	}
	return int(v), nil
}

// -----------------------------------------------------------------------------
// Concrete conversions — unsigned → int64
// -----------------------------------------------------------------------------

func Uint64ToInt64(v uint64) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("safeconv: %d overflows int64 (max %d)", v, math.MaxInt64)
	}
	return int64(v), nil
}

func Uint32ToInt64(v uint32) (int64, error) {
	return int64(v), nil
}

func Uint16ToInt64(v uint16) (int64, error) {
	return int64(v), nil
}

func Uint8ToInt64(v uint8) (int64, error) {
	return int64(v), nil
}

func UintToInt64(v uint) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("safeconv: %d overflows int64 (max %d)", v, math.MaxInt64)
	}
	return int64(v), nil
}

func UintptrToInt64(v uintptr) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("safeconv: %d overflows int64 (max %d)", v, math.MaxInt64)
	}
	return int64(v), nil
}

// -----------------------------------------------------------------------------
// Concrete conversions — unsigned → int32
// -----------------------------------------------------------------------------

func Uint64ToInt32(v uint64) (int32, error) {
	if v > math.MaxInt32 {
		return 0, fmt.Errorf("safeconv: %d overflows int32 (max %d)", v, math.MaxInt32)
	}
	return int32(v), nil
}

func Uint32ToInt32(v uint32) (int32, error) {
	if v > math.MaxInt32 {
		return 0, fmt.Errorf("safeconv: %d overflows int32 (max %d)", v, math.MaxInt32)
	}
	return int32(v), nil
}

func Uint16ToInt32(v uint16) (int32, error) {
	return int32(v), nil
}

func Uint8ToInt32(v uint8) (int32, error) {
	return int32(v), nil
}

func UintToInt32(v uint) (int32, error) {
	if v > math.MaxInt32 {
		return 0, fmt.Errorf("safeconv: %d overflows int32 (max %d)", v, math.MaxInt32)
	}
	return int32(v), nil
}

func UintptrToInt32(v uintptr) (int32, error) {
	if v > math.MaxInt32 {
		return 0, fmt.Errorf("safeconv: %d overflows int32 (max %d)", v, math.MaxInt32)
	}
	return int32(v), nil
}

// -----------------------------------------------------------------------------
// Concrete conversions — unsigned → int16
// -----------------------------------------------------------------------------

func Uint64ToInt16(v uint64) (int16, error) {
	if v > math.MaxInt16 {
		return 0, fmt.Errorf("safeconv: %d overflows int16 (max %d)", v, math.MaxInt16)
	}
	return int16(v), nil
}

func Uint32ToInt16(v uint32) (int16, error) {
	if v > math.MaxInt16 {
		return 0, fmt.Errorf("safeconv: %d overflows int16 (max %d)", v, math.MaxInt16)
	}
	return int16(v), nil
}

func Uint16ToInt16(v uint16) (int16, error) {
	if v > math.MaxInt16 {
		return 0, fmt.Errorf("safeconv: %d overflows int16 (max %d)", v, math.MaxInt16)
	}
	return int16(v), nil
}

func Uint8ToInt16(v uint8) (int16, error) {
	return int16(v), nil
}

func UintToInt16(v uint) (int16, error) {
	if v > math.MaxInt16 {
		return 0, fmt.Errorf("safeconv: %d overflows int16 (max %d)", v, math.MaxInt16)
	}
	return int16(v), nil
}

func UintptrToInt16(v uintptr) (int16, error) {
	if v > math.MaxInt16 {
		return 0, fmt.Errorf("safeconv: %d overflows int16 (max %d)", v, math.MaxInt16)
	}
	return int16(v), nil
}

// -----------------------------------------------------------------------------
// Concrete conversions — unsigned → int8
// -----------------------------------------------------------------------------

func Uint64ToInt8(v uint64) (int8, error) {
	if v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d overflows int8 (max %d)", v, math.MaxInt8)
	}
	return int8(v), nil
}

func Uint32ToInt8(v uint32) (int8, error) {
	if v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d overflows int8 (max %d)", v, math.MaxInt8)
	}
	return int8(v), nil
}

func Uint16ToInt8(v uint16) (int8, error) {
	if v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d overflows int8 (max %d)", v, math.MaxInt8)
	}
	return int8(v), nil
}

func Uint8ToInt8(v uint8) (int8, error) {
	if v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d overflows int8 (max %d)", v, math.MaxInt8)
	}
	return int8(v), nil
}

func UintToInt8(v uint) (int8, error) {
	if v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d overflows int8 (max %d)", v, math.MaxInt8)
	}
	return int8(v), nil
}

func UintptrToInt8(v uintptr) (int8, error) {
	if v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d overflows int8 (max %d)", v, math.MaxInt8)
	}
	return int8(v), nil
}

// -----------------------------------------------------------------------------
// Concrete conversions — signed → unsigned
// -----------------------------------------------------------------------------

func IntToUint64(v int) (uint64, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint64 (min 0)", v)
	}
	return uint64(v), nil
}

func IntToUint32(v int) (uint32, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint32 (min 0)", v)
	}
	if v > math.MaxUint32 {
		return 0, fmt.Errorf("safeconv: %d overflows uint32 (max %d)", v, math.MaxUint32)
	}
	return uint32(v), nil
}

func IntToUint16(v int) (uint16, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint16 (min 0)", v)
	}
	if v > math.MaxUint16 {
		return 0, fmt.Errorf("safeconv: %d overflows uint16 (max %d)", v, math.MaxUint16)
	}
	return uint16(v), nil
}

func IntToUint8(v int) (uint8, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint8 (min 0)", v)
	}
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("safeconv: %d overflows uint8 (max %d)", v, math.MaxUint8)
	}
	return uint8(v), nil
}

func IntToUint(v int) (uint, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint (min 0)", v)
	}
	return uint(v), nil
}

func IntToUintptr(v int) (uintptr, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uintptr (min 0)", v)
	}
	return uintptr(v), nil
}

func Int64ToUint64(v int64) (uint64, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint64 (min 0)", v)
	}
	return uint64(v), nil
}

func Int64ToUint32(v int64) (uint32, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint32 (min 0)", v)
	}
	if v > math.MaxUint32 {
		return 0, fmt.Errorf("safeconv: %d overflows uint32 (max %d)", v, math.MaxUint32)
	}
	return uint32(v), nil
}

func Int64ToUint16(v int64) (uint16, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint16 (min 0)", v)
	}
	if v > math.MaxUint16 {
		return 0, fmt.Errorf("safeconv: %d overflows uint16 (max %d)", v, math.MaxUint16)
	}
	return uint16(v), nil
}

func Int64ToUint8(v int64) (uint8, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint8 (min 0)", v)
	}
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("safeconv: %d overflows uint8 (max %d)", v, math.MaxUint8)
	}
	return uint8(v), nil
}

func Int64ToUint(v int64) (uint, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint (min 0)", v)
	}
	if uint64(v) > math.MaxUint {
		return 0, fmt.Errorf("safeconv: %d overflows uint (max %d)", v, uint(math.MaxUint))
	}
	return uint(v), nil
}

func Int64ToUintptr(v int64) (uintptr, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uintptr (min 0)", v)
	}
	return uintptr(v), nil
}

func Int32ToUint64(v int32) (uint64, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint64 (min 0)", v)
	}
	return uint64(v), nil
}

func Int32ToUint32(v int32) (uint32, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint32 (min 0)", v)
	}
	return uint32(v), nil
}

func Int32ToUint16(v int32) (uint16, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint16 (min 0)", v)
	}
	if v > math.MaxUint16 {
		return 0, fmt.Errorf("safeconv: %d overflows uint16 (max %d)", v, math.MaxUint16)
	}
	return uint16(v), nil
}

func Int32ToUint8(v int32) (uint8, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint8 (min 0)", v)
	}
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("safeconv: %d overflows uint8 (max %d)", v, math.MaxUint8)
	}
	return uint8(v), nil
}

func Int32ToUint(v int32) (uint, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint (min 0)", v)
	}
	return uint(v), nil
}

func Int32ToUintptr(v int32) (uintptr, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uintptr (min 0)", v)
	}
	return uintptr(v), nil
}

func Int16ToUint64(v int16) (uint64, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint64 (min 0)", v)
	}
	return uint64(v), nil
}

func Int16ToUint32(v int16) (uint32, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint32 (min 0)", v)
	}
	return uint32(v), nil
}

func Int16ToUint16(v int16) (uint16, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint16 (min 0)", v)
	}
	return uint16(v), nil
}

func Int16ToUint8(v int16) (uint8, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint8 (min 0)", v)
	}
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("safeconv: %d overflows uint8 (max %d)", v, math.MaxUint8)
	}
	return uint8(v), nil
}

func Int16ToUint(v int16) (uint, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint (min 0)", v)
	}
	return uint(v), nil
}

func Int16ToUintptr(v int16) (uintptr, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uintptr (min 0)", v)
	}
	return uintptr(v), nil
}

func Int8ToUint64(v int8) (uint64, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint64 (min 0)", v)
	}
	return uint64(v), nil
}

func Int8ToUint32(v int8) (uint32, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint32 (min 0)", v)
	}
	return uint32(v), nil
}

func Int8ToUint16(v int8) (uint16, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint16 (min 0)", v)
	}
	return uint16(v), nil
}

func Int8ToUint8(v int8) (uint8, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint8 (min 0)", v)
	}
	return uint8(v), nil
}

func Int8ToUint(v int8) (uint, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uint (min 0)", v)
	}
	return uint(v), nil
}

func Int8ToUintptr(v int8) (uintptr, error) {
	if v < 0 {
		return 0, fmt.Errorf("safeconv: %d underflows uintptr (min 0)", v)
	}
	return uintptr(v), nil
}

// -----------------------------------------------------------------------------
// Concrete conversions — signed narrowing
// -----------------------------------------------------------------------------

func Int64ToInt(v int64) (int, error) {
	if v < math.MinInt || v > math.MaxInt {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt, math.MaxInt)
	}
	return int(v), nil
}

func Int64ToInt32(v int64) (int32, error) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt32, math.MaxInt32)
	}
	return int32(v), nil
}

func Int64ToInt16(v int64) (int16, error) {
	if v < math.MinInt16 || v > math.MaxInt16 {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt16, math.MaxInt16)
	}
	return int16(v), nil
}

func Int64ToInt8(v int64) (int8, error) {
	if v < math.MinInt8 || v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt8, math.MaxInt8)
	}
	return int8(v), nil
}

func IntToInt32(v int) (int32, error) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt32, math.MaxInt32)
	}
	return int32(v), nil
}

func IntToInt16(v int) (int16, error) {
	if v < math.MinInt16 || v > math.MaxInt16 {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt16, math.MaxInt16)
	}
	return int16(v), nil
}

func IntToInt8(v int) (int8, error) {
	if v < math.MinInt8 || v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt8, math.MaxInt8)
	}
	return int8(v), nil
}

func Int32ToInt16(v int32) (int16, error) {
	if v < math.MinInt16 || v > math.MaxInt16 {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt16, math.MaxInt16)
	}
	return int16(v), nil
}

func Int32ToInt8(v int32) (int8, error) {
	if v < math.MinInt8 || v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt8, math.MaxInt8)
	}
	return int8(v), nil
}

func Int16ToInt8(v int16) (int8, error) {
	if v < math.MinInt8 || v > math.MaxInt8 {
		return 0, fmt.Errorf("safeconv: %d out of range [%d, %d]", v, math.MinInt8, math.MaxInt8)
	}
	return int8(v), nil
}

// -----------------------------------------------------------------------------
// Concrete conversions — unsigned narrowing
// -----------------------------------------------------------------------------

func Uint64ToUint32(v uint64) (uint32, error) {
	if v > math.MaxUint32 {
		return 0, fmt.Errorf("safeconv: %d overflows uint32 (max %d)", v, math.MaxUint32)
	}
	return uint32(v), nil
}

func Uint64ToUint16(v uint64) (uint16, error) {
	if v > math.MaxUint16 {
		return 0, fmt.Errorf("safeconv: %d overflows uint16 (max %d)", v, math.MaxUint16)
	}
	return uint16(v), nil
}

func Uint64ToUint8(v uint64) (uint8, error) {
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("safeconv: %d overflows uint8 (max %d)", v, math.MaxUint8)
	}
	return uint8(v), nil
}

func Uint64ToUint(v uint64) (uint, error) {
	if v > math.MaxUint {
		return 0, fmt.Errorf("safeconv: %d overflows uint (max %d)", v, uint(math.MaxUint))
	}
	return uint(v), nil
}

func Uint64ToUintptr(v uint64) (uintptr, error) {
	if v > math.MaxUint {
		return 0, fmt.Errorf("safeconv: %d overflows uintptr (max %d)", v, uint(math.MaxUint))
	}
	return uintptr(v), nil
}

func Uint32ToUint16(v uint32) (uint16, error) {
	if v > math.MaxUint16 {
		return 0, fmt.Errorf("safeconv: %d overflows uint16 (max %d)", v, math.MaxUint16)
	}
	return uint16(v), nil
}

func Uint32ToUint8(v uint32) (uint8, error) {
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("safeconv: %d overflows uint8 (max %d)", v, math.MaxUint8)
	}
	return uint8(v), nil
}

func Uint16ToUint8(v uint16) (uint8, error) {
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("safeconv: %d overflows uint8 (max %d)", v, math.MaxUint8)
	}
	return uint8(v), nil
}
