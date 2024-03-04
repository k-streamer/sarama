package sarama

import "fmt"

type Response struct {
	Length        int32
	CorrelationID int32
	Version       int16
	Body          ProtocolBody
	BodyVersion   int16
}

func (r *Response) Encode(pe packetEncoder) (err error) {
	pe.push(&lengthField{})
	pe.putInt32(r.CorrelationID)

	if r.Version >= 1 {
		pe.putEmptyTaggedFieldArray()
	}
	// Length:        int32(len(responseBody) + 8),
	err = r.Body.Encode(pe)
	if err != nil {
		return err
	}
	return pe.pop()
}

func (r *Response) Decode(pd packetDecoder, version int16) (err error) {
	r.Length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.Length <= 4 || r.Length > MaxResponseSize {
		return PacketDecodingError{fmt.Sprintf("message of length %d too large or too small", r.Length)}
	}

	r.CorrelationID, err = pd.getInt32()

	if version >= 1 {
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	return r.Body.Decode(pd, r.BodyVersion)
}
