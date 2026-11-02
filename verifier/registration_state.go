package verifier

// nextRegistrationSeq reserves the registration_seq of the next Register
// call through the role's evidence.RegistrationSeq: max(last + 1, now in Unix
// ms), durable in StateDir's registration.json before it is returned
// (CONTRACT §3a); without a StateDir it is kept in memory, still
// clock-floored.
func (r *Role) nextRegistrationSeq() (uint64, error) {
	return r.seq.Next(r.nowMillis())
}
