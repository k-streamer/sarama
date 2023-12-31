package sarama

type SaslAuthenticateRequest struct {
	// Version defines the protocol version to use for encode and decode
	Version       int16
	SaslAuthBytes []byte
}

// APIKeySASLAuth is the API key for the SaslAuthenticate Kafka API
const APIKeySASLAuth = 36

func (r *SaslAuthenticateRequest) Encode(pe packetEncoder) error {
	return pe.putBytes(r.SaslAuthBytes)
}

func (r *SaslAuthenticateRequest) Decode(pd packetDecoder, version int16) (err error) {
	r.Version = version
	r.SaslAuthBytes, err = pd.getBytes()
	return err
}

func (r *SaslAuthenticateRequest) APIKey() int16 {
	return APIKeySASLAuth
}

func (r *SaslAuthenticateRequest) APIVersion() int16 {
	return r.Version
}

func (r *SaslAuthenticateRequest) HeaderVersion() int16 {
	return 1
}

func (r *SaslAuthenticateRequest) IsValidVersion() bool {
	return r.Version >= 0 && r.Version <= 1
}

func (r *SaslAuthenticateRequest) RequiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V2_2_0_0
	default:
		return V1_0_0_0
	}
}
