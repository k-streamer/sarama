package sarama

type SaslHandshakeRequest struct {
	Mechanism string
	Version   int16
}

func (r *SaslHandshakeRequest) Encode(pe packetEncoder) error {
	if err := pe.putString(r.Mechanism); err != nil {
		return err
	}

	return nil
}

func (r *SaslHandshakeRequest) Decode(pd packetDecoder, version int16) (err error) {
	if r.Mechanism, err = pd.getString(); err != nil {
		return err
	}

	return nil
}

func (r *SaslHandshakeRequest) APIKey() int16 {
	return 17
}

func (r *SaslHandshakeRequest) APIVersion() int16 {
	return r.Version
}

func (r *SaslHandshakeRequest) HeaderVersion() int16 {
	return 1
}

func (r *SaslHandshakeRequest) IsValidVersion() bool {
	return r.Version >= 0 && r.Version <= 1
}

func (r *SaslHandshakeRequest) RequiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V1_0_0_0
	default:
		return V0_10_0_0
	}
}
