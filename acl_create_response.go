package sarama

import "time"

// CreateAclsResponse is a an acl response creation type
type CreateAclsResponse struct {
	Version              int16
	ThrottleTime         time.Duration
	AclCreationResponses []*AclCreationResponse
}

func (c *CreateAclsResponse) Encode(pe packetEncoder) error {
	pe.putInt32(int32(c.ThrottleTime / time.Millisecond))

	if err := pe.putArrayLength(len(c.AclCreationResponses)); err != nil {
		return err
	}

	for _, aclCreationResponse := range c.AclCreationResponses {
		if err := aclCreationResponse.Encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (c *CreateAclsResponse) Decode(pd packetDecoder, version int16) (err error) {
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	c.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	c.AclCreationResponses = make([]*AclCreationResponse, n)
	for i := 0; i < n; i++ {
		c.AclCreationResponses[i] = new(AclCreationResponse)
		if err := c.AclCreationResponses[i].Decode(pd, version); err != nil {
			return err
		}
	}

	return nil
}

func (c *CreateAclsResponse) APIKey() int16 {
	return 30
}

func (c *CreateAclsResponse) APIVersion() int16 {
	return c.Version
}

func (c *CreateAclsResponse) HeaderVersion() int16 {
	return 0
}

func (c *CreateAclsResponse) IsValidVersion() bool {
	return c.Version >= 0 && c.Version <= 1
}

func (c *CreateAclsResponse) RequiredVersion() KafkaVersion {
	switch c.Version {
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}

func (r *CreateAclsResponse) throttleTime() time.Duration {
	return r.ThrottleTime
}

// AclCreationResponse is an acl creation response type
type AclCreationResponse struct {
	Err    KError
	ErrMsg *string
}

func (a *AclCreationResponse) Encode(pe packetEncoder) error {
	pe.putInt16(int16(a.Err))

	if err := pe.putNullableString(a.ErrMsg); err != nil {
		return err
	}

	return nil
}

func (a *AclCreationResponse) Decode(pd packetDecoder, version int16) (err error) {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}
	a.Err = KError(kerr)

	if a.ErrMsg, err = pd.getNullableString(); err != nil {
		return err
	}

	return nil
}
