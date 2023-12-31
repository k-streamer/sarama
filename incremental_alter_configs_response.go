package sarama

import "time"

// IncrementalAlterConfigsResponse is a response type for incremental alter config
type IncrementalAlterConfigsResponse struct {
	Version      int16
	ThrottleTime time.Duration
	Resources    []*AlterConfigsResourceResponse
}

func (a *IncrementalAlterConfigsResponse) Encode(pe packetEncoder) error {
	pe.putInt32(int32(a.ThrottleTime / time.Millisecond))

	if err := pe.putArrayLength(len(a.Resources)); err != nil {
		return err
	}

	for _, v := range a.Resources {
		if err := v.Encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (a *IncrementalAlterConfigsResponse) Decode(pd packetDecoder, version int16) error {
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	a.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	responseCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	a.Resources = make([]*AlterConfigsResourceResponse, responseCount)

	for i := range a.Resources {
		a.Resources[i] = new(AlterConfigsResourceResponse)

		if err := a.Resources[i].Decode(pd, version); err != nil {
			return err
		}
	}

	return nil
}

func (a *IncrementalAlterConfigsResponse) APIKey() int16 {
	return 44
}

func (a *IncrementalAlterConfigsResponse) APIVersion() int16 {
	return a.Version
}

func (a *IncrementalAlterConfigsResponse) HeaderVersion() int16 {
	return 0
}

func (a *IncrementalAlterConfigsResponse) IsValidVersion() bool {
	return a.Version == 0
}

func (a *IncrementalAlterConfigsResponse) RequiredVersion() KafkaVersion {
	return V2_3_0_0
}

func (r *IncrementalAlterConfigsResponse) throttleTime() time.Duration {
	return r.ThrottleTime
}
