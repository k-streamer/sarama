package sarama

// ConsumerMetadataRequest is used for metadata requests
type ConsumerMetadataRequest struct {
	Version       int16
	ConsumerGroup string
}

func (r *ConsumerMetadataRequest) Encode(pe packetEncoder) error {
	tmp := new(FindCoordinatorRequest)
	tmp.CoordinatorKey = r.ConsumerGroup
	tmp.CoordinatorType = CoordinatorGroup
	tmp.Version = r.Version
	return tmp.Encode(pe)
}

func (r *ConsumerMetadataRequest) Decode(pd packetDecoder, version int16) (err error) {
	tmp := new(FindCoordinatorRequest)
	if err := tmp.Decode(pd, version); err != nil {
		return err
	}
	r.ConsumerGroup = tmp.CoordinatorKey
	return nil
}

func (r *ConsumerMetadataRequest) APIKey() int16 {
	return 10
}

func (r *ConsumerMetadataRequest) APIVersion() int16 {
	return r.Version
}

func (r *ConsumerMetadataRequest) HeaderVersion() int16 {
	return 1
}

func (r *ConsumerMetadataRequest) IsValidVersion() bool {
	return r.Version >= 0 && r.Version <= 2
}

func (r *ConsumerMetadataRequest) RequiredVersion() KafkaVersion {
	switch r.Version {
	case 2:
		return V2_0_0_0
	case 1:
		return V0_11_0_0
	default:
		return V0_8_2_0
	}
}
