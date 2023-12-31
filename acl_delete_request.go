package sarama

// DeleteAclsRequest is a delete acl request
type DeleteAclsRequest struct {
	Version int
	Filters []*AclFilter
}

func (d *DeleteAclsRequest) Encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(d.Filters)); err != nil {
		return err
	}

	for _, filter := range d.Filters {
		filter.Version = d.Version
		if err := filter.Encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (d *DeleteAclsRequest) Decode(pd packetDecoder, version int16) (err error) {
	d.Version = int(version)
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	d.Filters = make([]*AclFilter, n)
	for i := 0; i < n; i++ {
		d.Filters[i] = new(AclFilter)
		d.Filters[i].Version = int(version)
		if err := d.Filters[i].Decode(pd, version); err != nil {
			return err
		}
	}

	return nil
}

func (d *DeleteAclsRequest) APIKey() int16 {
	return 31
}

func (d *DeleteAclsRequest) APIVersion() int16 {
	return int16(d.Version)
}

func (d *DeleteAclsRequest) HeaderVersion() int16 {
	return 1
}

func (d *DeleteAclsRequest) IsValidVersion() bool {
	return d.Version >= 0 && d.Version <= 1
}

func (d *DeleteAclsRequest) RequiredVersion() KafkaVersion {
	switch d.Version {
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}
