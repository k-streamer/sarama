package sarama

// DescribeAclsRequest is a describe acl request type
type DescribeAclsRequest struct {
	Version int
	AclFilter
}

func (d *DescribeAclsRequest) Encode(pe packetEncoder) error {
	d.AclFilter.Version = d.Version
	return d.AclFilter.Encode(pe)
}

func (d *DescribeAclsRequest) Decode(pd packetDecoder, version int16) (err error) {
	d.Version = int(version)
	d.AclFilter.Version = int(version)
	return d.AclFilter.Decode(pd, version)
}

func (d *DescribeAclsRequest) APIKey() int16 {
	return 29
}

func (d *DescribeAclsRequest) APIVersion() int16 {
	return int16(d.Version)
}

func (d *DescribeAclsRequest) HeaderVersion() int16 {
	return 1
}

func (d *DescribeAclsRequest) IsValidVersion() bool {
	return d.Version >= 0 && d.Version <= 1
}

func (d *DescribeAclsRequest) RequiredVersion() KafkaVersion {
	switch d.Version {
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}
