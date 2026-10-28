package serve

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// TestHandlerRefusesAMissingOrWrongToken: every route answers 401 without the
// exact bearer token, before routing; with it, the methods, routes and bodies
// are checked.
func TestHandlerRefusesAMissingOrWrongToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		routes := []struct{ method, path, body string }{
			{http.MethodGet, "/v1/serving", ""},
			{http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"0xs40"}`},
			{http.MethodPost, "/v1/serving/renew", `{"lease_id":"00000000000000000000000000000000"}`},
			{http.MethodPost, "/v1/serving/release", `{"lease_id":"00000000000000000000000000000000"}`},
			{http.MethodGet, "/v1/other", ""},
		}
		headers := []string{"", "Bearer ", "Bearer wrong-token", "Bearer " + testToken + "x", "Basic " + testToken, "bearer " + testToken, testToken}
		for _, r := range routes {
			for _, header := range headers {
				req := httptest.NewRequest(r.method, r.path, strings.NewReader(r.body))
				if header != "" {
					req.Header.Set("Authorization", header)
				}
				rec := httptest.NewRecorder()
				h.srv.Handler().ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized || strings.TrimSpace(rec.Body.String()) != `{"error":"member serving: missing or wrong bearer token"}` {
					t.Errorf("%s %s with Authorization %q: %d %s", r.method, r.path, header, rec.Code, rec.Body.String())
				}
			}
		}
		if d := h.database(); d.Lease != nil {
			t.Fatalf("an unauthenticated acquire granted %+v", d.Lease)
		}

		// With the token: the routes, methods and bodies.
		status, out := h.call(http.MethodGet, "/v1/serving", "")
		if status != http.StatusOK || out["node_id"] != "member-b" {
			t.Fatalf("GET /v1/serving: %d %v", status, out)
		}
		status, out = h.call(http.MethodPost, "/v1/serving", "")
		requireError(t, status, out, http.StatusMethodNotAllowed, "member serving: POST /v1/serving is not allowed; use GET")
		status, out = h.call(http.MethodGet, "/v1/serving/acquire", "")
		requireError(t, status, out, http.StatusMethodNotAllowed, "member serving: GET /v1/serving/acquire is not allowed; use POST")
		status, out = h.call(http.MethodGet, "/v1/other", "")
		requireError(t, status, out, http.StatusNotFound, "member serving: no route /v1/other")
		status, out = h.call(http.MethodPost, "/v1/serving/renew", `{`)
		requireError(t, status, out, http.StatusBadRequest, "member serving: unexpected EOF")
		status, out = h.call(http.MethodPost, "/v1/serving/renew", `{"lease_id":"a"} {"lease_id":"b"}`)
		requireError(t, status, out, http.StatusBadRequest, "member serving: the body holds more than one JSON value")
		status, out = h.call(http.MethodPost, "/v1/serving/release", `{}`)
		requireError(t, status, out, http.StatusBadRequest, "member serving: lease_id is required")
	})
}
