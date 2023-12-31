package sarama

import (
	"time"
)

// AddOffsetsToTxnResponse is a response type for adding offsets to txns
type AddOffsetsToTxnResponse struct {
	Version      int16
	ThrottleTime time.Duration
	Err          KError
}

func (a *AddOffsetsToTxnResponse) Encode(pe packetEncoder) error {
	pe.putInt32(int32(a.ThrottleTime / time.Millisecond))
	pe.putInt16(int16(a.Err))
	return nil
}

func (a *AddOffsetsToTxnResponse) Decode(pd packetDecoder, version int16) (err error) {
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	a.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}
	a.Err = KError(kerr)

	return nil
}

func (a *AddOffsetsToTxnResponse) APIKey() int16 {
	return 25
}

func (a *AddOffsetsToTxnResponse) APIVersion() int16 {
	return a.Version
}

func (a *AddOffsetsToTxnResponse) HeaderVersion() int16 {
	return 0
}

func (a *AddOffsetsToTxnResponse) IsValidVersion() bool {
	return a.Version >= 0 && a.Version <= 2
}

func (a *AddOffsetsToTxnResponse) RequiredVersion() KafkaVersion {
	switch a.Version {
	case 2:
		return V2_7_0_0
	case 1:
		return V2_0_0_0
	case 0:
		return V0_11_0_0
	default:
		return V2_7_0_0
	}
}

func (r *AddOffsetsToTxnResponse) throttleTime() time.Duration {
	return r.ThrottleTime
}
