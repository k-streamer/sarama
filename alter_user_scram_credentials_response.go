package sarama

import "time"

type AlterUserScramCredentialsResponse struct {
	Version int16

	ThrottleTime time.Duration

	Results []*AlterUserScramCredentialsResult
}

type AlterUserScramCredentialsResult struct {
	User string

	ErrorCode    KError
	ErrorMessage *string
}

func (r *AlterUserScramCredentialsResponse) Encode(pe packetEncoder) error {
	pe.putInt32(int32(r.ThrottleTime / time.Millisecond))
	pe.putCompactArrayLength(len(r.Results))

	for _, u := range r.Results {
		if err := pe.putCompactString(u.User); err != nil {
			return err
		}
		pe.putInt16(int16(u.ErrorCode))
		if err := pe.putNullableCompactString(u.ErrorMessage); err != nil {
			return err
		}
		pe.putEmptyTaggedFieldArray()
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (r *AlterUserScramCredentialsResponse) Decode(pd packetDecoder, version int16) error {
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	r.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	numResults, err := pd.getCompactArrayLength()
	if err != nil {
		return err
	}

	if numResults > 0 {
		r.Results = make([]*AlterUserScramCredentialsResult, numResults)
		for i := 0; i < numResults; i++ {
			r.Results[i] = &AlterUserScramCredentialsResult{}
			if r.Results[i].User, err = pd.getCompactString(); err != nil {
				return err
			}

			kerr, err := pd.getInt16()
			if err != nil {
				return err
			}

			r.Results[i].ErrorCode = KError(kerr)
			if r.Results[i].ErrorMessage, err = pd.getCompactNullableString(); err != nil {
				return err
			}
			if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
				return err
			}
		}
	}

	if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
		return err
	}
	return nil
}

func (r *AlterUserScramCredentialsResponse) APIKey() int16 {
	return 51
}

func (r *AlterUserScramCredentialsResponse) APIVersion() int16 {
	return r.Version
}

func (r *AlterUserScramCredentialsResponse) HeaderVersion() int16 {
	return 2
}

func (r *AlterUserScramCredentialsResponse) IsValidVersion() bool {
	return r.Version == 0
}

func (r *AlterUserScramCredentialsResponse) RequiredVersion() KafkaVersion {
	return V2_7_0_0
}

func (r *AlterUserScramCredentialsResponse) throttleTime() time.Duration {
	return r.ThrottleTime
}
