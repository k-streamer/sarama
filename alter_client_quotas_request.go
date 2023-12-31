package sarama

// AlterClientQuotas Request (Version: 0) => [entries] validate_only
//   entries => [entity] [ops]
//     entity => entity_type entity_name
//       entity_type => STRING
//       entity_name => NULLABLE_STRING
//     ops => key value remove
//       key => STRING
//       value => FLOAT64
//       remove => BOOLEAN
//   validate_only => BOOLEAN

type AlterClientQuotasRequest struct {
	Version      int16
	Entries      []AlterClientQuotasEntry // The quota configuration entries to alter.
	ValidateOnly bool                     // Whether the alteration should be validated, but not performed.
}

type AlterClientQuotasEntry struct {
	Entity []QuotaEntityComponent // The quota entity to alter.
	Ops    []ClientQuotasOp       // An individual quota configuration entry to alter.
}

type ClientQuotasOp struct {
	Key    string  // The quota configuration key.
	Value  float64 // The value to set, otherwise ignored if the value is to be removed.
	Remove bool    // Whether the quota configuration value should be removed, otherwise set.
}

func (a *AlterClientQuotasRequest) Encode(pe packetEncoder) error {
	// Entries
	if err := pe.putArrayLength(len(a.Entries)); err != nil {
		return err
	}
	for _, e := range a.Entries {
		if err := e.Encode(pe); err != nil {
			return err
		}
	}

	// ValidateOnly
	pe.putBool(a.ValidateOnly)

	return nil
}

func (a *AlterClientQuotasRequest) Decode(pd packetDecoder, version int16) error {
	// Entries
	entryCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if entryCount > 0 {
		a.Entries = make([]AlterClientQuotasEntry, entryCount)
		for i := range a.Entries {
			e := AlterClientQuotasEntry{}
			if err = e.Decode(pd, version); err != nil {
				return err
			}
			a.Entries[i] = e
		}
	} else {
		a.Entries = []AlterClientQuotasEntry{}
	}

	// ValidateOnly
	validateOnly, err := pd.getBool()
	if err != nil {
		return err
	}
	a.ValidateOnly = validateOnly

	return nil
}

func (a *AlterClientQuotasEntry) Encode(pe packetEncoder) error {
	// Entity
	if err := pe.putArrayLength(len(a.Entity)); err != nil {
		return err
	}
	for _, component := range a.Entity {
		if err := component.Encode(pe); err != nil {
			return err
		}
	}

	// Ops
	if err := pe.putArrayLength(len(a.Ops)); err != nil {
		return err
	}
	for _, o := range a.Ops {
		if err := o.Encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (a *AlterClientQuotasEntry) Decode(pd packetDecoder, version int16) error {
	// Entity
	componentCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if componentCount > 0 {
		a.Entity = make([]QuotaEntityComponent, componentCount)
		for i := 0; i < componentCount; i++ {
			component := QuotaEntityComponent{}
			if err := component.Decode(pd, version); err != nil {
				return err
			}
			a.Entity[i] = component
		}
	} else {
		a.Entity = []QuotaEntityComponent{}
	}

	// Ops
	opCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if opCount > 0 {
		a.Ops = make([]ClientQuotasOp, opCount)
		for i := range a.Ops {
			c := ClientQuotasOp{}
			if err = c.Decode(pd, version); err != nil {
				return err
			}
			a.Ops[i] = c
		}
	} else {
		a.Ops = []ClientQuotasOp{}
	}

	return nil
}

func (c *ClientQuotasOp) Encode(pe packetEncoder) error {
	// Key
	if err := pe.putString(c.Key); err != nil {
		return err
	}

	// Value
	pe.putFloat64(c.Value)

	// Remove
	pe.putBool(c.Remove)

	return nil
}

func (c *ClientQuotasOp) Decode(pd packetDecoder, version int16) error {
	// Key
	key, err := pd.getString()
	if err != nil {
		return err
	}
	c.Key = key

	// Value
	value, err := pd.getFloat64()
	if err != nil {
		return err
	}
	c.Value = value

	// Remove
	remove, err := pd.getBool()
	if err != nil {
		return err
	}
	c.Remove = remove

	return nil
}

func (a *AlterClientQuotasRequest) APIKey() int16 {
	return 49
}

func (a *AlterClientQuotasRequest) APIVersion() int16 {
	return a.Version
}

func (a *AlterClientQuotasRequest) HeaderVersion() int16 {
	return 1
}

func (a *AlterClientQuotasRequest) IsValidVersion() bool {
	return a.Version == 0
}

func (a *AlterClientQuotasRequest) RequiredVersion() KafkaVersion {
	return V2_6_0_0
}
