package install

import (
	"regexp"
	"strings"
	"testing"
)

// TestScratchNamesCarryTheNodeIDAndBootNonce: install scratch tables are
// <node_id>_<boot_nonce>_<n> (spec §8.5 step 3), every one starts with the
// node's and the boot's prefix, and a boot nonce is 8 fresh lowercase hex
// characters.
func TestScratchNamesCarryTheNodeIDAndBootNonce(t *testing.T) {
	if got := ScratchName("member-a", "0123abcd", 7); got != "member-a_0123abcd_7" {
		t.Fatalf("ScratchName = %q", got)
	}
	if got := ScratchPrefix("member-a", "0123abcd"); got != "member-a_0123abcd_" {
		t.Fatalf("ScratchPrefix = %q", got)
	}
	if got := NodePrefix("member-a"); got != "member-a_" {
		t.Fatalf("NodePrefix = %q", got)
	}
	name := ScratchName("member-c", "89abcdef", 12)
	if !strings.HasPrefix(name, NodePrefix("member-c")) || !strings.HasPrefix(name, ScratchPrefix("member-c", "89abcdef")) {
		t.Fatalf("%q lacks its node or boot prefix", name)
	}
	if strings.HasPrefix(ScratchName("member-ab", "89abcdef", 1), NodePrefix("member-a")) {
		t.Fatal("member-ab's scratch starts with member-a's prefix")
	}
	nonce := regexp.MustCompile(`^[0-9a-f]{8}$`)
	seen := map[string]bool{}
	for range 64 {
		n, err := NewBootNonce()
		if err != nil {
			t.Fatal(err)
		}
		if !nonce.MatchString(n) {
			t.Fatalf("boot nonce %q is not 8 lowercase hex characters", n)
		}
		seen[n] = true
	}
	if len(seen) < 60 {
		t.Fatalf("64 boot nonces gave only %d distinct values", len(seen))
	}
}
