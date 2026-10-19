package objstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

const testSecret = "s3cr3t'value\\with-quote"

func TestCredentialsRedactTheSecret(t *testing.T) {
	// Given
	c := Credentials{AccessKeyID: "GOOG1EXAMPLE", SecretAccessKey: testSecret}
	holder := struct {
		Name        string
		Credentials Credentials
	}{"backup-0", c}
	var logged bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logged, nil))

	// When
	logger.Info("backup", "credentials", c, "holder", holder)
	asJSON, err := json.Marshal(holder)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	printed := []string{
		c.String(),
		fmt.Sprint(c), fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), fmt.Sprintf("%s", c),
		fmt.Sprintf("%v", holder), fmt.Sprintf("%+v", holder), fmt.Sprintf("%v", &holder),
		logged.String(), string(asJSON),
	}

	// Then
	for _, p := range printed {
		if strings.Contains(p, "s3cr3t") {
			t.Errorf("the secret was printed: %s", p)
		}
		if !strings.Contains(p, "GOOG1EXAMPLE") || !strings.Contains(p, redacted) {
			t.Errorf("want the access key id and %s in %s", redacted, p)
		}
	}
}

func TestStatementErrorsNeverCarryTheSecret(t *testing.T) {
	// Given
	c := Credentials{AccessKeyID: "GOOG1EXAMPLE", SecretAccessKey: testSecret}
	escaped := `s3cr3t\'value\\with-quote`
	server := &clickhouse.Exception{Code: 62, Name: "DB::Exception", Message: "Syntax error near '" + escaped + "'", StackTrace: "at " + testSecret}
	plain := errors.New("dial tcp: " + testSecret)

	// When
	gotServer := c.statementError("objstore: backup %s", server, "x")
	gotPlain := c.statementError("objstore: backup %s", plain, "x")

	// Then
	for _, err := range []error{gotServer, gotPlain} {
		if strings.Contains(err.Error(), "s3cr3t") {
			t.Errorf("the secret survived: %v", err)
		}
	}
	var ex *clickhouse.Exception
	if !errors.As(gotServer, &ex) || ex.Code != 62 || strings.Contains(ex.Message+ex.StackTrace, "s3cr3t") {
		t.Errorf("a server exception keeps its code and loses the secret: %#v", ex)
	}
	if c.redact(nil) != nil {
		t.Error("redact(nil) must be nil")
	}
}
