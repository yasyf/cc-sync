package orcabridge

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestImportRecoveryLaunch(t *testing.T) {
	advertised := strings.Replace(describeLocal, `"capabilities":["`+CapabilityWorkspace+`"]`, `"capabilities":["`+CapabilityWorkspace+`","`+CapabilityRecoveryLaunch+`"]`, 1)
	launch := map[string]RecoveryLaunch{"src-a": {AppendSystemPrompt: "moved from /src"}}
	tests := []struct {
		name     string
		describe string
		launch   map[string]RecoveryLaunch
		wantFile string
	}{
		{"advertised", advertised, launch, `{"src-a":{"appendSystemPrompt":"moved from /src"}}`},
		{"not advertised", describeLocal, launch, ""},
		{"nothing to send", advertised, nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeOrca(t)
			f.respond("recovery-describe", tt.describe, 0)
			if _, err := f.client.Import(t.Context(), ImportRequest{Descriptor: []byte(`{}`), Checkout: "/dst", CheckpointID: "cp-1", RecoveryLaunch: tt.launch}); err != nil {
				t.Fatal(err)
			}
			calls := f.calls()
			argv := calls[len(calls)-1].argv
			i := slices.Index(argv, "--recovery-launch-file")
			if tt.wantFile == "" {
				if i >= 0 {
					t.Fatalf("argv = %q, want no --recovery-launch-file", argv)
				}
				return
			}
			if i < 0 || argv[len(argv)-1] != "--json" {
				t.Fatalf("argv = %q, want --recovery-launch-file before --json", argv)
			}
			got, err := f.root.ReadFile(path.Join("calls", fmt.Sprintf("%04d", len(calls)-1), "launch"))
			if err != nil || string(got) != tt.wantFile {
				t.Errorf("launch file = %s, %v, want %s", got, err, tt.wantFile)
			}
			if _, err := os.Stat(argv[i+1]); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("launch file %s left behind: %v", argv[i+1], err)
			}
		})
	}
}

func TestImportArgsRecoveryLaunchCap(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"at cap", MaxAppendSystemPromptBytes, false},
		{"over cap", MaxAppendSystemPromptBytes + 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := importArgs(ImportRequest{Checkout: "/dst", CheckpointID: "cp-1", RecoveryLaunch: map[string]RecoveryLaunch{"a": {AppendSystemPrompt: strings.Repeat("x", tt.size)}}})
			if (err != nil) != tt.wantErr {
				t.Errorf("importArgs err = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

func TestOmitted(t *testing.T) {
	tests := []struct {
		name       string
		descriptor string
		want       []OmittedBinding
		wantCode   string
	}{
		{"listed", `{"version":1,"omittedBindings":[{"agent":"codex","key":"session_id","id":"c-1","reason":"agent-not-supported-v1"}]}`, []OmittedBinding{{Agent: "codex", Key: "session_id", ID: "c-1", Reason: "agent-not-supported-v1"}}, ""},
		{"empty", `{"version":1,"omittedBindings":[]}`, []OmittedBinding{}, ""},
		{"missing", `{"version":1}`, nil, CodeDescriptorInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Omitted([]byte(tt.descriptor))
			if tt.wantCode != "" {
				if r, ok := errors.AsType[*RefusedError](err); !ok || r.Code != tt.wantCode {
					t.Fatalf("Omitted err = %v, want %s", err, tt.wantCode)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Omitted = %+v, %v, want %+v", got, err, tt.want)
			}
		})
	}
}
