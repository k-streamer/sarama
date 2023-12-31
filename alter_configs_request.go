package sarama

// AlterConfigsRequest is an alter config request type
type AlterConfigsRequest struct {
	Version      int16
	Resources    []*AlterConfigsResource
	ValidateOnly bool
}

// AlterConfigsResource is an alter config resource type
type AlterConfigsResource struct {
	Type          ConfigResourceType
	Name          string
	ConfigEntries map[string]*string
}

func (a *AlterConfigsRequest) Encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(a.Resources)); err != nil {
		return err
	}

	for _, r := range a.Resources {
		if err := r.Encode(pe); err != nil {
			return err
		}
	}

	pe.putBool(a.ValidateOnly)
	return nil
}

func (a *AlterConfigsRequest) Decode(pd packetDecoder, version int16) error {
	resourceCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	a.Resources = make([]*AlterConfigsResource, resourceCount)
	for i := range a.Resources {
		r := &AlterConfigsResource{}
		err = r.Decode(pd, version)
		if err != nil {
			return err
		}
		a.Resources[i] = r
	}

	validateOnly, err := pd.getBool()
	if err != nil {
		return err
	}

	a.ValidateOnly = validateOnly

	return nil
}

func (a *AlterConfigsResource) Encode(pe packetEncoder) error {
	pe.putInt8(int8(a.Type))

	if err := pe.putString(a.Name); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(a.ConfigEntries)); err != nil {
		return err
	}
	for configKey, configValue := range a.ConfigEntries {
		if err := pe.putString(configKey); err != nil {
			return err
		}
		if err := pe.putNullableString(configValue); err != nil {
			return err
		}
	}

	return nil
}

func (a *AlterConfigsResource) Decode(pd packetDecoder, version int16) error {
	t, err := pd.getInt8()
	if err != nil {
		return err
	}
	a.Type = ConfigResourceType(t)

	name, err := pd.getString()
	if err != nil {
		return err
	}
	a.Name = name

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	if n > 0 {
		a.ConfigEntries = make(map[string]*string, n)
		for i := 0; i < n; i++ {
			configKey, err := pd.getString()
			if err != nil {
				return err
			}
			if a.ConfigEntries[configKey], err = pd.getNullableString(); err != nil {
				return err
			}
		}
	}
	return err
}

func (a *AlterConfigsRequest) APIKey() int16 {
	return 33
}

func (a *AlterConfigsRequest) APIVersion() int16 {
	return a.Version
}

func (a *AlterConfigsRequest) HeaderVersion() int16 {
	return 1
}

func (a *AlterConfigsRequest) IsValidVersion() bool {
	return a.Version >= 0 && a.Version <= 1
}

func (a *AlterConfigsRequest) RequiredVersion() KafkaVersion {
	switch a.Version {
	case 1:
		return V2_0_0_0
	case 0:
		return V0_11_0_0
	default:
		return V2_0_0_0
	}
}
