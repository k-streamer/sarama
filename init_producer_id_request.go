package sarama

import "time"

type InitProducerIDRequest struct {
	Version            int16
	TransactionalID    *string
	TransactionTimeout time.Duration
	ProducerID         int64
	ProducerEpoch      int16
}

func (i *InitProducerIDRequest) Encode(pe packetEncoder) error {
	if i.Version < 2 {
		if err := pe.putNullableString(i.TransactionalID); err != nil {
			return err
		}
	} else {
		if err := pe.putNullableCompactString(i.TransactionalID); err != nil {
			return err
		}
	}
	pe.putInt32(int32(i.TransactionTimeout / time.Millisecond))
	if i.Version >= 3 {
		pe.putInt64(i.ProducerID)
		pe.putInt16(i.ProducerEpoch)
	}
	if i.Version >= 2 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

func (i *InitProducerIDRequest) Decode(pd packetDecoder, version int16) (err error) {
	i.Version = version
	if i.Version < 2 {
		if i.TransactionalID, err = pd.getNullableString(); err != nil {
			return err
		}
	} else {
		if i.TransactionalID, err = pd.getCompactNullableString(); err != nil {
			return err
		}
	}

	timeout, err := pd.getInt32()
	if err != nil {
		return err
	}
	i.TransactionTimeout = time.Duration(timeout) * time.Millisecond
	if i.Version >= 3 {
		if i.ProducerID, err = pd.getInt64(); err != nil {
			return err
		}

		if i.ProducerEpoch, err = pd.getInt16(); err != nil {
			return err
		}
	}

	if i.Version >= 2 {
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	return nil
}

func (i *InitProducerIDRequest) APIKey() int16 {
	return 22
}

func (i *InitProducerIDRequest) APIVersion() int16 {
	return i.Version
}

func (i *InitProducerIDRequest) HeaderVersion() int16 {
	if i.Version >= 2 {
		return 2
	}

	return 1
}

func (i *InitProducerIDRequest) IsValidVersion() bool {
	return i.Version >= 0 && i.Version <= 4
}

func (i *InitProducerIDRequest) RequiredVersion() KafkaVersion {
	switch i.Version {
	case 4:
		return V2_7_0_0
	case 3:
		return V2_5_0_0
	case 2:
		return V2_4_0_0
	case 1:
		return V2_0_0_0
	case 0:
		return V0_11_0_0
	default:
		return V2_7_0_0
	}
}
