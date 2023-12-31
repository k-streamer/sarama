package sarama

// DescribeUserScramCredentialsRequest is a request to get list of SCRAM user names
type DescribeUserScramCredentialsRequest struct {
	// Version 0 is currently only supported
	Version int16

	// If this is an empty array, all users will be queried
	DescribeUsers []DescribeUserScramCredentialsRequestUser
}

// DescribeUserScramCredentialsRequestUser is a describe request about specific user name
type DescribeUserScramCredentialsRequestUser struct {
	Name string
}

func (r *DescribeUserScramCredentialsRequest) Encode(pe packetEncoder) error {
	pe.putCompactArrayLength(len(r.DescribeUsers))
	for _, d := range r.DescribeUsers {
		if err := pe.putCompactString(d.Name); err != nil {
			return err
		}
		pe.putEmptyTaggedFieldArray()
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (r *DescribeUserScramCredentialsRequest) Decode(pd packetDecoder, version int16) error {
	n, err := pd.getCompactArrayLength()
	if err != nil {
		return err
	}
	if n == -1 {
		n = 0
	}

	r.DescribeUsers = make([]DescribeUserScramCredentialsRequestUser, n)
	for i := 0; i < n; i++ {
		r.DescribeUsers[i] = DescribeUserScramCredentialsRequestUser{}
		if r.DescribeUsers[i].Name, err = pd.getCompactString(); err != nil {
			return err
		}
		if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
		return err
	}
	return nil
}

func (r *DescribeUserScramCredentialsRequest) APIKey() int16 {
	return 50
}

func (r *DescribeUserScramCredentialsRequest) APIVersion() int16 {
	return r.Version
}

func (r *DescribeUserScramCredentialsRequest) HeaderVersion() int16 {
	return 2
}

func (r *DescribeUserScramCredentialsRequest) IsValidVersion() bool {
	return r.Version == 0
}

func (r *DescribeUserScramCredentialsRequest) RequiredVersion() KafkaVersion {
	return V2_7_0_0
}
