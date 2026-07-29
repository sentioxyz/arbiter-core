package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpdateHousegate(t *testing.T) {
	script, err := filepath.Abs("update-housegate.sh")
	if err != nil {
		t.Fatalf("locate update-housegate.sh: %v", err)
	}

	const fullCommit = "4dd088f4fe17d7bf13ba2c2e2311d72d0b97cd54"
	tests := []struct {
		name            string
		input           string
		resolvedVersion string
		wantVersion     string
	}{
		{
			name:            "release tag",
			input:           "v0.7.1",
			resolvedVersion: "v0.7.1",
			wantVersion:     "0.7.1",
		},
		{
			name:            "commit SHA",
			input:           "4dd088f",
			resolvedVersion: "v0.7.2-0.20260729070302-4dd088f4fe17",
			wantVersion:     "0.7.2-0.20260729070302-4dd088f4fe17",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := initHousegateUpdateRepo(t)
			toolLog := filepath.Join(dir, "tools.log")
			fakeGo := writeFakeTool(t, dir, "fake-go", `
if [ "$1" = "list" ]; then
	printf "%s %s\n" "$FAKE_GO_VERSION" "$FAKE_GO_COMMIT"
fi
`)
			fakeBazel := writeFakeTool(t, dir, "fake-bazel", "")

			cmd := exec.Command("bash", script, tc.input)
			cmd.Dir = dir
			cmd.Env = append(isolatedEnv(),
				"GO_BIN="+fakeGo,
				"BAZEL_BIN="+fakeBazel,
				"FAKE_GO_VERSION="+tc.resolvedVersion,
				"FAKE_GO_COMMIT="+fullCommit,
				"FAKE_TOOL_LOG="+toolLog,
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("update-housegate.sh %s: %v\n%s", tc.input, err, out)
			}

			for _, name := range []string{"MODULE.bazel", "README.md"} {
				got := readFile(t, filepath.Join(dir, name))
				for _, want := range []string{
					`version = "` + tc.wantVersion + `"`,
					`commit = "` + fullCommit + `"`,
					`# Resolved Housegate ` + tc.resolvedVersion + `; source is pinned by the commit below.`,
				} {
					if !strings.Contains(got, want) {
						t.Errorf("%s does not contain %q:\n%s", name, want, got)
					}
				}
				if strings.Contains(got, "1.0.0") {
					t.Errorf("%s retained the stale Bzlmod version:\n%s", name, got)
				}
			}

			log := readFile(t, toolLog)
			for _, want := range []string{
				"go get github.com/housegate/housegate@" + tc.resolvedVersion,
				"go mod tidy",
				"bazel mod deps --lockfile_mode=update",
			} {
				if !strings.Contains(log, want) {
					t.Errorf("tool log does not contain %q:\n%s", want, log)
				}
			}
		})
	}
}

func TestUpdateHousegateRejectsInvalidInput(t *testing.T) {
	script, err := filepath.Abs("update-housegate.sh")
	if err != nil {
		t.Fatalf("locate update-housegate.sh: %v", err)
	}

	dir := initHousegateUpdateRepo(t)
	cmd := exec.Command("bash", script, "main")
	cmd.Dir = dir
	cmd.Env = isolatedEnv()
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("update-housegate.sh main succeeded:\n%s", out)
	}
	if !strings.Contains(string(out), "expected a Housegate vX.Y.Z tag or a 7-40 character commit SHA") {
		t.Fatalf("unexpected error:\n%s", out)
	}
}

func initHousegateUpdateRepo(t *testing.T) string {
	t.Helper()
	dir := initRepo(t)
	const pins = `bazel_dep(
    name = "housegate",
    version = "1.0.0",
)

git_override(
    module_name = "housegate",
    # Dereferenced annotated v0.7.0 release tag.
    commit = "bc99cda81be13b52fd7831752525de0a098f9018",
    remote = "https://github.com/housegate/housegate",
)
`
	writeFile(t, filepath.Join(dir, "MODULE.bazel"), "module(name = \"arbiter_core\")\n\n"+pins)
	writeFile(t, filepath.Join(dir, "README.md"), "Example:\n\n```starlark\n"+pins+"```\n")
	writeFile(t, filepath.Join(dir, "go.mod"), "module example.com/arbiter-core-test\n\ngo 1.26.3\n")
	return dir
}

func writeFakeTool(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	content := `#!/usr/bin/env bash
set -euo pipefail
printf "%s %s\n" "` + strings.TrimPrefix(name, "fake-") + `" "$*" >> "$FAKE_TOOL_LOG"
` + body
	writeFile(t, path, content)
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
	return path
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
