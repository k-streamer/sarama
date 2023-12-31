package sarama

import (
	"time"
)

type EndTxnResponse struct {
	Version      int16
	ThrottleTime time.Duration
	Err          KError
}

func (e *EndTxnResponse) Encode(pe packetEncoder) error {
	pe.putInt32(int32(e.ThrottleTime / time.Millisecond))
	pe.putInt16(int16(e.Err))
	return nil
}

func (e *EndTxnResponse) Decode(pd packetDecoder, version int16) (err error) {
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	e.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}
	e.Err = KError(kerr)

	return nil
}

func (e *EndTxnResponse) APIKey() int16 {
	return 26
}

func (e *EndTxnResponse) APIVersion() int16 {
	return e.Version
}

func (r *EndTxnResponse) HeaderVersion() int16 {
	return 0
}

func (e *EndTxnResponse) IsValidVersion() bool {
	return e.Version >= 0 && e.Version <= 2
}

func (e *EndTxnResponse) RequiredVersion() KafkaVersion {
	switch e.Version {
	case 2:
		return V2_7_0_0
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}

func (r *EndTxnResponse) throttleTime() time.Duration {
	return r.ThrottleTime
}
