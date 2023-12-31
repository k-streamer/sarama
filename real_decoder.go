package sarama

import (
	"encoding/binary"
	"math"

	"github.com/rcrowley/go-metrics"
)

var (
	errInvalidArrayLength     = PacketDecodingError{"invalid array length"}
	errInvalidByteSliceLength = PacketDecodingError{"invalid byteslice length"}
	errInvalidStringLength    = PacketDecodingError{"invalid string length"}
	errVarintOverflow         = PacketDecodingError{"varint overflow"}
	errUVarintOverflow        = PacketDecodingError{"uvarint overflow"}
	errInvalidBool            = PacketDecodingError{"invalid bool"}
)

type RealDecoder struct {
	Raw      []byte
	off      int
	stack    []pushDecoder
	registry metrics.Registry
}

// primitives

func (rd *RealDecoder) getInt8() (int8, error) {
	if rd.remaining() < 1 {
		rd.off = len(rd.Raw)
		return -1, ErrInsufficientData
	}
	tmp := int8(rd.Raw[rd.off])
	rd.off++
	return tmp, nil
}

func (rd *RealDecoder) getInt16() (int16, error) {
	if rd.remaining() < 2 {
		rd.off = len(rd.Raw)
		return -1, ErrInsufficientData
	}
	tmp := int16(binary.BigEndian.Uint16(rd.Raw[rd.off:]))
	rd.off += 2
	return tmp, nil
}

func (rd *RealDecoder) getInt32() (int32, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.Raw)
		return -1, ErrInsufficientData
	}
	tmp := int32(binary.BigEndian.Uint32(rd.Raw[rd.off:]))
	rd.off += 4
	return tmp, nil
}

func (rd *RealDecoder) getInt64() (int64, error) {
	if rd.remaining() < 8 {
		rd.off = len(rd.Raw)
		return -1, ErrInsufficientData
	}
	tmp := int64(binary.BigEndian.Uint64(rd.Raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *RealDecoder) getVarint() (int64, error) {
	tmp, n := binary.Varint(rd.Raw[rd.off:])
	if n == 0 {
		rd.off = len(rd.Raw)
		return -1, ErrInsufficientData
	}
	if n < 0 {
		rd.off -= n
		return -1, errVarintOverflow
	}
	rd.off += n
	return tmp, nil
}

func (rd *RealDecoder) getUVarint() (uint64, error) {
	tmp, n := binary.Uvarint(rd.Raw[rd.off:])
	if n == 0 {
		rd.off = len(rd.Raw)
		return 0, ErrInsufficientData
	}

	if n < 0 {
		rd.off -= n
		return 0, errUVarintOverflow
	}

	rd.off += n
	return tmp, nil
}

func (rd *RealDecoder) getFloat64() (float64, error) {
	if rd.remaining() < 8 {
		rd.off = len(rd.Raw)
		return -1, ErrInsufficientData
	}
	tmp := math.Float64frombits(binary.BigEndian.Uint64(rd.Raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *RealDecoder) getArrayLength() (int, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.Raw)
		return -1, ErrInsufficientData
	}
	tmp := int(int32(binary.BigEndian.Uint32(rd.Raw[rd.off:])))
	rd.off += 4
	if tmp > rd.remaining() {
		rd.off = len(rd.Raw)
		return -1, ErrInsufficientData
	} else if tmp > 2*math.MaxUint16 {
		return -1, errInvalidArrayLength
	}
	return tmp, nil
}

func (rd *RealDecoder) getCompactArrayLength() (int, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return 0, err
	}

	if n == 0 {
		return 0, nil
	}

	return int(n) - 1, nil
}

func (rd *RealDecoder) getBool() (bool, error) {
	b, err := rd.getInt8()
	if err != nil || b == 0 {
		return false, err
	}
	if b != 1 {
		return false, errInvalidBool
	}
	return true, nil
}

func (rd *RealDecoder) getEmptyTaggedFieldArray() (int, error) {
	tagCount, err := rd.getUVarint()
	if err != nil {
		return 0, err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for i := uint64(0); i < tagCount; i++ {
		// fetch and ignore tag identifier
		_, err := rd.getUVarint()
		if err != nil {
			return 0, err
		}
		length, err := rd.getUVarint()
		if err != nil {
			return 0, err
		}
		if _, err := rd.getRawBytes(int(length)); err != nil {
			return 0, err
		}
	}

	return 0, nil
}

// collections

func (rd *RealDecoder) getBytes() ([]byte, error) {
	tmp, err := rd.getInt32()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.getRawBytes(int(tmp))
}

func (rd *RealDecoder) getVarintBytes() ([]byte, error) {
	tmp, err := rd.getVarint()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.getRawBytes(int(tmp))
}

func (rd *RealDecoder) getCompactBytes() ([]byte, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)
	return rd.getRawBytes(length)
}

func (rd *RealDecoder) getStringLength() (int, error) {
	length, err := rd.getInt16()
	if err != nil {
		return 0, err
	}

	n := int(length)

	switch {
	case n < -1:
		return 0, errInvalidStringLength
	case n > rd.remaining():
		rd.off = len(rd.Raw)
		return 0, ErrInsufficientData
	}

	return n, nil
}

func (rd *RealDecoder) getString() (string, error) {
	n, err := rd.getStringLength()
	if err != nil || n == -1 {
		return "", err
	}

	tmpStr := string(rd.Raw[rd.off : rd.off+n])
	rd.off += n
	return tmpStr, nil
}

func (rd *RealDecoder) getNullableString() (*string, error) {
	n, err := rd.getStringLength()
	if err != nil || n == -1 {
		return nil, err
	}

	tmpStr := string(rd.Raw[rd.off : rd.off+n])
	rd.off += n
	return &tmpStr, err
}

func (rd *RealDecoder) getCompactString() (string, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return "", err
	}

	length := int(n - 1)
	if length < 0 {
		return "", errInvalidByteSliceLength
	}
	tmpStr := string(rd.Raw[rd.off : rd.off+length])
	rd.off += length
	return tmpStr, nil
}

func (rd *RealDecoder) getCompactNullableString() (*string, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)

	if length < 0 {
		return nil, err
	}

	tmpStr := string(rd.Raw[rd.off : rd.off+length])
	rd.off += length
	return &tmpStr, err
}

func (rd *RealDecoder) getCompactInt32Array() ([]int32, error) {
	n, err := rd.getUVarint()
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, nil
	}

	arrayLength := int(n) - 1

	ret := make([]int32, arrayLength)

	for i := range ret {
		ret[i] = int32(binary.BigEndian.Uint32(rd.Raw[rd.off:]))
		rd.off += 4
	}
	return ret, nil
}

func (rd *RealDecoder) getInt32Array() ([]int32, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.Raw)
		return nil, ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.Raw[rd.off:]))
	rd.off += 4

	if rd.remaining() < 4*n {
		rd.off = len(rd.Raw)
		return nil, ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int32, n)
	for i := range ret {
		ret[i] = int32(binary.BigEndian.Uint32(rd.Raw[rd.off:]))
		rd.off += 4
	}
	return ret, nil
}

func (rd *RealDecoder) getInt64Array() ([]int64, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.Raw)
		return nil, ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.Raw[rd.off:]))
	rd.off += 4

	if rd.remaining() < 8*n {
		rd.off = len(rd.Raw)
		return nil, ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int64, n)
	for i := range ret {
		ret[i] = int64(binary.BigEndian.Uint64(rd.Raw[rd.off:]))
		rd.off += 8
	}
	return ret, nil
}

func (rd *RealDecoder) getStringArray() ([]string, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.Raw)
		return nil, ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.Raw[rd.off:]))
	rd.off += 4

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]string, n)
	for i := range ret {
		str, err := rd.getString()
		if err != nil {
			return nil, err
		}

		ret[i] = str
	}
	return ret, nil
}

// subsets

func (rd *RealDecoder) remaining() int {
	return len(rd.Raw) - rd.off
}

func (rd *RealDecoder) getSubset(length int) (packetDecoder, error) {
	buf, err := rd.getRawBytes(length)
	if err != nil {
		return nil, err
	}
	return &RealDecoder{Raw: buf}, nil
}

func (rd *RealDecoder) getRawBytes(length int) ([]byte, error) {
	if length < 0 {
		return nil, errInvalidByteSliceLength
	} else if length > rd.remaining() {
		rd.off = len(rd.Raw)
		return nil, ErrInsufficientData
	}

	start := rd.off
	rd.off += length
	return rd.Raw[start:rd.off], nil
}

func (rd *RealDecoder) peek(offset, length int) (packetDecoder, error) {
	if rd.remaining() < offset+length {
		return nil, ErrInsufficientData
	}
	off := rd.off + offset
	return &RealDecoder{Raw: rd.Raw[off : off+length]}, nil
}

func (rd *RealDecoder) peekInt8(offset int) (int8, error) {
	const byteLen = 1
	if rd.remaining() < offset+byteLen {
		return -1, ErrInsufficientData
	}
	return int8(rd.Raw[rd.off+offset]), nil
}

// stacks

func (rd *RealDecoder) push(in pushDecoder) error {
	in.saveOffset(rd.off)

	var reserve int
	if dpd, ok := in.(dynamicPushDecoder); ok {
		if err := dpd.Decode(rd); err != nil {
			return err
		}
	} else {
		reserve = in.reserveLength()
		if rd.remaining() < reserve {
			rd.off = len(rd.Raw)
			return ErrInsufficientData
		}
	}

	rd.stack = append(rd.stack, in)

	rd.off += reserve

	return nil
}

func (rd *RealDecoder) pop() error {
	// this is go's ugly pop pattern (the inverse of append)
	in := rd.stack[len(rd.stack)-1]
	rd.stack = rd.stack[:len(rd.stack)-1]

	return in.check(rd.off, rd.Raw)
}

func (rd *RealDecoder) metricRegistry() metrics.Registry {
	return rd.registry
}
