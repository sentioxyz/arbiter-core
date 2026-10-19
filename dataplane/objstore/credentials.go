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

// redacted replaces the secret wherever Credentials is printed.
const redacted = "[redacted]"

// Credentials is one HMAC key pair of the bucket's S3-compatible API.
// String, GoString, LogValue and MarshalJSON print the access key id and
// never the secret.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// String prints the access key id and [redacted] for the secret.
func (c Credentials) String() string {
	return "objstore.Credentials{AccessKeyID: " + c.AccessKeyID + ", SecretAccessKey: " + redacted + "}"
}

// GoString is String, so %#v redacts too.
func (c Credentials) GoString() string { return c.String() }

// LogValue prints the access key id and [redacted] for the secret.
func (c Credentials) LogValue() slog.Value {
	return slog.GroupValue(slog.String("access_key_id", c.AccessKeyID), slog.String("secret_access_key", redacted))
}

// MarshalJSON prints the access key id and [redacted] for the secret, so a
// struct that holds Credentials never serializes the secret.
func (c Credentials) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
	}{c.AccessKeyID, redacted})
}

func (c Credentials) validate() error {
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return errors.New("objstore: credentials need both an access key id and a secret access key")
	}
	return nil
}

// redact returns err with every form of the secret a statement carries (the
// raw value and its SQL-escaped form) replaced by [redacted]. A server
// exception stays a *clickhouse.Exception with its code and name, so callers
// can still match the code with errors.As; cancellation retains only its
// known context sentinel, and other errors become plain text. ClickHouse
// masks the secret argument itself, but an error that
// echoes a statement fragment would otherwise carry it.
func (c Credentials) redact(err error) error {
	if err == nil || c.SecretAccessKey == "" {
		return err
	}
	forms := []string{c.SecretAccessKey}
	if escaped := chparts.EscapeSQLString(c.SecretAccessKey); escaped != c.SecretAccessKey {
		forms = append(forms, escaped)
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
