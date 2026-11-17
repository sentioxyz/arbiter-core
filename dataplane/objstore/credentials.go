package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
)

// redacted replaces the secret and the session token wherever Credentials
// is printed.
const redacted = "[redacted]"

// Credentials is one key of the bucket's S3-compatible API: an HMAC key pair,
// and for temporary credentials (Cloudflare R2's, D4 of the si-backup R2
// design) the session token that goes with it. String, GoString, LogValue
// and MarshalJSON print the access key id and never the secret or the token.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	// SessionToken is empty for a static key pair, whose statements render
	// exactly as they did before it existed. When set, s3() takes it as its
	// fourth argument and BACKUP and RESTORE address the bucket through a
	// temporary named collection (OpenEndpoint).
	SessionToken string
}

// String prints the access key id and [redacted] for the secret, and for the
// session token when there is one.
func (c Credentials) String() string {
	s := "objstore.Credentials{AccessKeyID: " + c.AccessKeyID + ", SecretAccessKey: " + redacted
	if c.SessionToken != "" {
		s += ", SessionToken: " + redacted
	}
	return s + "}"
}

// GoString is String, so %#v redacts too.
func (c Credentials) GoString() string { return c.String() }

// LogValue prints the access key id and [redacted] for the secret, and for
// the session token when there is one.
func (c Credentials) LogValue() slog.Value {
	attrs := []slog.Attr{slog.String("access_key_id", c.AccessKeyID), slog.String("secret_access_key", redacted)}
	if c.SessionToken != "" {
		attrs = append(attrs, slog.String("session_token", redacted))
	}
	return slog.GroupValue(attrs...)
}

// MarshalJSON prints the access key id and [redacted] for the secret, and for
// the session token when there is one, so a struct that holds Credentials
// never serializes either.
func (c Credentials) MarshalJSON() ([]byte, error) {
	token := ""
	if c.SessionToken != "" {
		token = redacted
	}
	return json.Marshal(struct {
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
		SessionToken    string `json:"session_token,omitempty"`
	}{c.AccessKeyID, redacted, token})
}

func (c Credentials) validate() error {
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return errors.New("objstore: credentials need both an access key id and a secret access key")
	}
	return nil
}

// redact returns err with every form of the secret and of the session token
// a statement carries (the raw value and its SQL-escaped form) replaced by
// [redacted]. A server exception stays a *clickhouse.Exception with its code
// and name, and a failed BACKUP or RESTORE a *backupStatusError with its
// status, so callers can still match them with errors.As; cancellation
// retains only its known context sentinel, and other errors become plain
// text. ClickHouse masks the secret argument itself, but an error that
// echoes a statement fragment would otherwise carry it.
func (c Credentials) redact(err error) error {
	if err == nil {
		return err
	}
	var forms []string
	for _, v := range []string{c.SecretAccessKey, c.SessionToken} {
		if v == "" {
			continue
		}
		forms = append(forms, v)
		if escaped := chparts.EscapeSQLString(v); escaped != v {
			forms = append(forms, escaped)
		}
	}
	if len(forms) == 0 {
		return err
	}
	scrub := func(s string) string {
		for _, f := range forms {
			s = strings.ReplaceAll(s, f, redacted)
		}
		return s
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, cause) {
			return redactedContextError{text: scrub(err.Error()), cause: cause}
		}
	}
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		out := redactException(*ex, scrub)
		return &out
	}
	var st *backupStatusError
	if errors.As(err, &st) {
		return &backupStatusError{Status: scrub(st.Status), ID: scrub(st.ID), Text: scrub(st.Text)}
	}
	return errors.New(scrub(err.Error()))
}

// redactedContextError preserves cancellation identity without retaining the
// original error, whose text or unwrap chain may contain the secret.
type redactedContextError struct {
	text  string
	cause error
}

func (e redactedContextError) Error() string { return e.text }
func (e redactedContextError) Unwrap() error { return e.cause }

func redactException(ex clickhouse.Exception, scrub func(string) string) clickhouse.Exception {
	out := clickhouse.Exception{Code: ex.Code, Name: ex.Name, Message: scrub(ex.Message), StackTrace: scrub(ex.StackTrace)}
	for _, nested := range ex.Nested {
		out.Nested = append(out.Nested, redactException(nested, scrub))
	}
	return out
}

// statementError wraps the redacted error of a statement that carried c.
func (c Credentials) statementError(format string, err error, args ...any) error {
	return fmt.Errorf(format+": %w", append(args, c.redact(err))...)
}
