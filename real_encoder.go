package sarama

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/rcrowley/go-metrics"
)

type RealEncoder struct {
	Raw      []byte
	off      int
	stack    []pushEncoder
	registry metrics.Registry
}

// primitives

func (re *RealEncoder) putInt8(in int8) {
	re.Raw[re.off] = byte(in)
	re.off++
}

func (re *RealEncoder) putInt16(in int16) {
	binary.BigEndian.PutUint16(re.Raw[re.off:], uint16(in))
	re.off += 2
}

func (re *RealEncoder) putInt32(in int32) {
	binary.BigEndian.PutUint32(re.Raw[re.off:], uint32(in))
	re.off += 4
}

func (re *RealEncoder) putInt64(in int64) {
	binary.BigEndian.PutUint64(re.Raw[re.off:], uint64(in))
	re.off += 8
}

func (re *RealEncoder) putVarint(in int64) {
	re.off += binary.PutVarint(re.Raw[re.off:], in)
}

func (re *RealEncoder) putUVarint(in uint64) {
	re.off += binary.PutUvarint(re.Raw[re.off:], in)
}

func (re *RealEncoder) putFloat64(in float64) {
	binary.BigEndian.PutUint64(re.Raw[re.off:], math.Float64bits(in))
	re.off += 8
}

func (re *RealEncoder) putArrayLength(in int) error {
	re.putInt32(int32(in))
	return nil
}

func (re *RealEncoder) putCompactArrayLength(in int) {
	// 0 represents a null array, so +1 has to be added
	re.putUVarint(uint64(in + 1))
}

func (re *RealEncoder) putBool(in bool) {
	if in {
		re.putInt8(1)
		return
	}
	re.putInt8(0)
}

// collection

func (re *RealEncoder) putRawBytes(in []byte) error {
	copy(re.Raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *RealEncoder) putBytes(in []byte) error {
	if in == nil {
		re.putInt32(-1)
		return nil
	}
	re.putInt32(int32(len(in)))
	return re.putRawBytes(in)
}

func (re *RealEncoder) putVarintBytes(in []byte) error {
	if in == nil {
		re.putVarint(-1)
		return nil
	}
	re.putVarint(int64(len(in)))
	return re.putRawBytes(in)
}

func (re *RealEncoder) putCompactBytes(in []byte) error {
	re.putUVarint(uint64(len(in) + 1))
	return re.putRawBytes(in)
}

func (re *RealEncoder) putCompactString(in string) error {
	re.putCompactArrayLength(len(in))
	return re.putRawBytes([]byte(in))
}

func (re *RealEncoder) putNullableCompactString(in *string) error {
	if in == nil {
		re.putInt8(0)
		return nil
	}
	return re.putCompactString(*in)
}

func (re *RealEncoder) putString(in string) error {
	re.putInt16(int16(len(in)))
	copy(re.Raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *RealEncoder) putNullableString(in *string) error {
	if in == nil {
		re.putInt16(-1)
		return nil
	}
	return re.putString(*in)
}

func (re *RealEncoder) putStringArray(in []string) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, val := range in {
		if err := re.putString(val); err != nil {
			return err
		}
	}

	return nil
}

func (re *RealEncoder) putCompactInt32Array(in []int32) error {
	if in == nil {
		return errors.New("expected int32 array to be non null")
	}
	// 0 represents a null array, so +1 has to be added
	re.putUVarint(uint64(len(in)) + 1)
	for _, val := range in {
		re.putInt32(val)
	}
	return nil
}

func (re *RealEncoder) putNullableCompactInt32Array(in []int32) error {
	if in == nil {
		re.putUVarint(0)
		return nil
	}
	// 0 represents a null array, so +1 has to be added
	re.putUVarint(uint64(len(in)) + 1)
	for _, val := range in {
		re.putInt32(val)
	}
	return nil
}

func (re *RealEncoder) putInt32Array(in []int32) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.putInt32(val)
	}
	return nil
}

func (re *RealEncoder) putInt64Array(in []int64) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.putInt64(val)
	}
	return nil
}

func (re *RealEncoder) putEmptyTaggedFieldArray() {
	re.putUVarint(0)
}

func (re *RealEncoder) offset() int {
	return re.off
}

// stacks

func (re *RealEncoder) push(in pushEncoder) {
	in.saveOffset(re.off)
	re.off += in.reserveLength()
	re.stack = append(re.stack, in)
}

func (re *RealEncoder) pop() error {
	// this is go's ugly pop pattern (the inverse of append)
	in := re.stack[len(re.stack)-1]
	re.stack = re.stack[:len(re.stack)-1]

	return in.run(re.off, re.Raw)
}

// we do record metrics during the real encoder pass
func (re *RealEncoder) metricRegistry() metrics.Registry {
	return re.registry
}
