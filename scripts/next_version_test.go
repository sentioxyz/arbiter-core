// Package scripts holds the guard tests for the shell helpers CI calls.
//
// The Cut Release workflow decides every released version by shelling out to
// next-version.sh, so these tests drive the script itself against real git
// repositories rather than restating its arithmetic in Go.
package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestNextVersion(t *testing.T) {
	script, err := filepath.Abs("next-version.sh")
	if err != nil {
		t.Fatalf("locate next-version.sh: %v", err)
	}

	type tag struct{ name, when string }

	tests := []struct {
		name string
		tags []tag
		env  []string
		want string
	}{
		{
			name: "first cut of a release line starts at .0.0",
			want: "v0.0.0",
			env:  []string{"RELEASE_TODAY_UTC=2026-07-27"},
		},
		{
			name: "a second cut on the same UTC day bumps patch",
			tags: []tag{{"v0.0.0", "2026-07-27T10:00:00Z"}},
			env:  []string{"RELEASE_TODAY_UTC=2026-07-27"},
			want: "v0.0.1",
		},
		{
			name: "a cut on a new UTC day bumps minor and resets patch",
			tags: []tag{{"v0.3.5", "2026-07-26T23:59:00Z"}},
			env:  []string{"RELEASE_TODAY_UTC=2026-07-27"},
			want: "v0.4.0",
		},
		{
			name: "the day boundary is exclusive at UTC midnight",
			tags: []tag{{"v0.3.5", "2026-07-27T00:00:00Z"}},
			env:  []string{"RELEASE_TODAY_UTC=2026-07-27"},
			want: "v0.3.6",
		},
		{
			name: "the day is measured in UTC, not the runner local zone",
			tags: []tag{{"v0.2.9", "2026-07-26T23:30:00Z"}},
			env:  []string{"RELEASE_TODAY_UTC=2026-07-27", "TZ=UTC-8"},
			want: "v0.3.0",
		},
		{
			name: "the previous cut is picked by version order",
			tags: []tag{
				{"v0.9.0", "2026-07-27T01:00:00Z"},
				{"v0.10.0", "2026-07-27T02:00:00Z"},
			},
			env:  []string{"RELEASE_TODAY_UTC=2026-07-27"},
			want: "v0.10.1",
		},
		{
			name: "non release tags cannot seed the next version",
			tags: []tag{
				{"v0.1.2", "2026-07-27T01:00:00Z"},
				{"v0.1.3-rc1", "2026-07-27T02:00:00Z"},
				{"v0.1.4-nightly", "2026-07-27T03:00:00Z"},
			},
			env:  []string{"RELEASE_TODAY_UTC=2026-07-27"},
			want: "v0.1.3",
		},
		{
			name: "a new release line ignores the old one",
			tags: []tag{{"v0.4.2", "2026-07-27T01:00:00Z"}},
			env:  []string{"RELEASE_TODAY_UTC=2026-07-27", "RELEASE_MAJOR=1"},
			want: "v1.0.0",
		},
		{
			name: "a lightweight tag falls back to the commit date",
			tags: []tag{{"v0.5.0", ""}},
			env:  []string{"RELEASE_TODAY_UTC=2026-01-01"},
			want: "v0.5.1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := initRepo(t)
			for _, tag := range tc.tags {
				writeTag(t, dir, tag.name, tag.when)
			}
			if got := runScript(t, dir, script, tc.env); got != tc.want {
				t.Errorf("next-version.sh = %q, want %q", got, tc.want)
			}
		})
	}
}

func initRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	runGit(t, dir, nil, "init", "-q", "-b", "main")
	runGit(t, dir, nil, "config", "user.name", "arbiter-core-test")
	runGit(t, dir, nil, "config", "user.email", "arbiter-core-test@example.com")
	runGit(t, dir, []string{
		"GIT_AUTHOR_DATE=2026-01-01T00:00:00Z",
		"GIT_COMMITTER_DATE=2026-01-01T00:00:00Z",
	}, "commit", "-q", "--allow-empty", "-m", "root")
	return dir
}

func writeTag(t *testing.T, dir, name, when string) {
	t.Helper()
	if when == "" {
		runGit(t, dir, nil, "tag", name)
		return
	}
	runGit(t, dir, []string{"GIT_COMMITTER_DATE=" + when}, "tag", "-a", name, "-m", name)
}

func runGit(t *testing.T, dir string, env []string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(isolatedEnv(), env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func runScript(t *testing.T, dir, script string, env []string) string {
	t.Helper()
	cmd := exec.Command("bash", script)
	cmd.Dir = dir
	cmd.Env = append(isolatedEnv(), env...)
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if exit, ok := err.(*exec.ExitError); ok {
			stderr = string(exit.Stderr)
		}
		t.Fatalf("run %s: %v\n%s", script, err, stderr)
	}
	return strings.TrimSpace(string(out))
}

func isolatedEnv() []string {
	return append(os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
	)
}
