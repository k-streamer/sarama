package sarama

// CreateAclsRequest is an acl creation request
type CreateAclsRequest struct {
	Version      int16
	AclCreations []*AclCreation
}

func (c *CreateAclsRequest) Encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(c.AclCreations)); err != nil {
		return err
	}

	for _, aclCreation := range c.AclCreations {
		if err := aclCreation.encode(pe, c.Version); err != nil {
			return err
		}
	}

	return nil
}

func (c *CreateAclsRequest) Decode(pd packetDecoder, version int16) (err error) {
	c.Version = version
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	c.AclCreations = make([]*AclCreation, n)

	for i := 0; i < n; i++ {
		c.AclCreations[i] = new(AclCreation)
		if err := c.AclCreations[i].Decode(pd, version); err != nil {
			return err
		}
	}

	return nil
}

func (c *CreateAclsRequest) APIKey() int16 {
	return 30
}

func (c *CreateAclsRequest) APIVersion() int16 {
	return c.Version
}

func (c *CreateAclsRequest) HeaderVersion() int16 {
	return 1
}

func (c *CreateAclsRequest) IsValidVersion() bool {
	return c.Version >= 0 && c.Version <= 1
}

func (c *CreateAclsRequest) RequiredVersion() KafkaVersion {
	switch c.Version {
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}

// AclCreation is a wrapper around Resource and Acl type
type AclCreation struct {
	Resource
	Acl
}

func (a *AclCreation) encode(pe packetEncoder, version int16) error {
	if err := a.Resource.encode(pe, version); err != nil {
		return err
	}
	if err := a.Acl.Encode(pe); err != nil {
		return err
	}

	return nil
}

func (a *AclCreation) Decode(pd packetDecoder, version int16) (err error) {
	if err := a.Resource.Decode(pd, version); err != nil {
		return err
	}
	if err := a.Acl.Decode(pd, version); err != nil {
		return err
	}

	return nil
}
