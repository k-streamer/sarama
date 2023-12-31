package sarama

import (
	"fmt"

	"github.com/rcrowley/go-metrics"
)

// Encoder is the interface that wraps the basic Encode method.
// Anything implementing Encoder can be turned into bytes using Kafka's encoding rules.
type Encoder interface {
	Encode(pe packetEncoder) error
}

type EncoderWithHeader interface {
	Encoder
	HeaderVersion() int16
}

// Encode takes an Encoder and turns it into bytes while potentially recording metrics.
func Encode(e Encoder, metricRegistry metrics.Registry) ([]byte, error) {
	if e == nil {
		return nil, nil
	}

	var prepEnc prepEncoder
	var realEnc RealEncoder

	err := e.Encode(&prepEnc)
	if err != nil {
		return nil, err
	}

	if prepEnc.length < 0 || prepEnc.length > int(MaxRequestSize) {
		return nil, PacketEncodingError{fmt.Sprintf("invalid request size (%d)", prepEnc.length)}
	}

	realEnc.Raw = make([]byte, prepEnc.length)
	fmt.Println("Prep encoder length:", prepEnc.length)
	realEnc.registry = metricRegistry
	err = e.Encode(&realEnc)
	if err != nil {
		return nil, err
	}

	return realEnc.Raw, nil
}

// Decoder is the interface that wraps the basic Decode method.
// Anything implementing Decoder can be extracted from bytes using Kafka's encoding rules.
type Decoder interface {
	Decode(pd packetDecoder) error
}

type VersionedDecoder interface {
	Decode(pd packetDecoder, version int16) error
}

// Decode takes bytes and a Decoder and fills the fields of the Decoder from the bytes,
// interpreted using Kafka's encoding rules.
func Decode(buf []byte, in Decoder, metricRegistry metrics.Registry) error {
	if buf == nil {
		return nil
	}

	helper := RealDecoder{
		Raw:      buf,
		registry: metricRegistry,
	}
	err := in.Decode(&helper)
	if err != nil {
		return err
	}

	if helper.off != len(buf) {
		return PacketDecodingError{"invalid length"}
	}

	return nil
}

func VersionedDecode(buf []byte, in VersionedDecoder, version int16, metricRegistry metrics.Registry) error {
	if buf == nil {
		return nil
	}

	helper := RealDecoder{
		Raw:      buf,
		registry: metricRegistry,
	}
	err := in.Decode(&helper, version)
	if err != nil {
		return err
	}

	if helper.off != len(buf) {
		return PacketDecodingError{
			Info: fmt.Sprintf("invalid length (off=%d, len=%d)", helper.off, len(buf)),
		}
	}

	return nil
}
