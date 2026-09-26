package cli_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/yasyf/cc-sync/internal/cli"
)

func TestParseTarget(t *testing.T) {
	tests := []struct {
		in      string
		want    cli.Target
		wantErr bool
	}{
		{in: "host-mbp/wt-7f3a", want: cli.ItemRef{SourceHostID: "host-mbp", WorkspaceID: "wt-7f3a"}},
		{in: "0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90", want: cli.SessionRef{ID: "0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90"}},
		{in: "0f3c", want: cli.SessionRef{ID: "0f3c"}},
		{in: "host-mbp:0f3c", want: cli.SessionRef{Source: "host-mbp", ID: "0f3c"}},
		{in: "host:x/wt", want: cli.ItemRef{SourceHostID: "host:x", WorkspaceID: "wt"}},
		{in: "", wantErr: true},
		{in: "/wt", wantErr: true},
		{in: "host/", wantErr: true},
		{in: "a/b/c", wantErr: true},
		{in: ":0f3c", wantErr: true},
		{in: "host:", wantErr: true},
		{in: "0F3C", wantErr: true},
		{in: "-0f3c", wantErr: true},
		{in: "zz", wantErr: true},
		{in: "0f3c9a2e-5b1d-4c8e-9a7f-2d6b8e1c4a90a", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := cli.ParseTarget(tt.in)
			if tt.wantErr {
				if cli.Classify(err) != cli.CodeUsage || got != nil {
					t.Fatalf("ParseTarget(%q) = %#v, %v; want nil and a usage error", tt.in, got, err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("ParseTarget(%q) = %#v, %v; want %#v", tt.in, got, err, tt.want)
			}
			if got.String() != tt.in {
				t.Errorf("String() = %q, want %q", got.String(), tt.in)
			}
		})
	}
}

func TestParseCheckpoint(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "latest", want: "cli.LatestCheckpoint latest"},
		{in: "c0ffee", want: "cli.CheckpointID c0ffee"},
		{in: "0", want: "cli.CheckpointID 0"},
		{in: "at:2026-09-26T19:00:00Z", want: "cli.CheckpointAt at:2026-09-26T19:00:00Z"},
		{in: "at:2026-09-26T12:00:00.25-07:00", want: "cli.CheckpointAt at:2026-09-26T19:00:00.25Z"},
		{in: "hourly:-0h", want: "cli.CheckpointHourly hourly:-0h"},
		{in: "hourly:-23h", want: "cli.CheckpointHourly hourly:-23h"},
		{in: "daily:2026-09-25", want: "cli.CheckpointDaily daily:2026-09-25"},
		{in: "", wantErr: true},
		{in: "LATEST", wantErr: true},
		{in: "C0FFEE", wantErr: true},
		{in: "at:2026-09-26", wantErr: true},
		{in: "at:", wantErr: true},
		{in: "hourly:3h", wantErr: true},
		{in: "hourly:-h", wantErr: true},
		{in: "hourly:-3", wantErr: true},
		{in: "hourly:-99999999999999999999h", wantErr: true},
		{in: "daily:2026-13-01", wantErr: true},
		{in: "daily:26-09-25", wantErr: true},
		{in: "weekly:1", wantErr: true},
		{in: "yesterday", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := cli.ParseCheckpoint(tt.in)
			if tt.wantErr {
				if cli.Classify(err) != cli.CodeUsage || got != nil {
					t.Fatalf("ParseCheckpoint(%q) = %#v, %v; want nil and a usage error", tt.in, got, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseCheckpoint(%q) error = %v", tt.in, err)
			}
			if s := fmt.Sprintf("%T %s", got, got); s != tt.want {
				t.Errorf("ParseCheckpoint(%q) = %s, want %s", tt.in, s, tt.want)
			}
		})
	}
}

func TestExitCodes(t *testing.T) {
	tests := []struct {
		code cli.Code
		want int
	}{
		{cli.CodeInternal, 1},
		{cli.CodeCancelled, 1},
		{cli.CodeUsage, 2},
		{cli.CodeNotFound, 3},
		{cli.CodeNotReady, 4},
		{cli.CodeLiveLocalCollision, 4},
		{cli.CodeOrcaNotLocal, 4},
		{cli.CodeCheckoutConflict, 4},
		{cli.CodeUnsupported, 4},
		{cli.CodeOrcaUnavailable, 5},
		{cli.CodeUnavailable, 5},
	}
	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			if got := tt.code.ExitCode(); got != tt.want {
				t.Errorf("ExitCode() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want cli.Code
	}{
		{"plain", errors.New("boom"), cli.CodeInternal},
		{"typed", cli.Errorf(cli.CodeNotReady, "not ready"), cli.CodeNotReady},
		{"wrapped typed", fmt.Errorf("pickup: %w", cli.Errorf(cli.CodeCheckoutConflict, "exists")), cli.CodeCheckoutConflict},
		{"canceled", context.Canceled, cli.CodeCancelled},
		{"wrapped canceled", fmt.Errorf("restore: %w", context.Canceled), cli.CodeCancelled},
		{"typed over canceled", cli.Errorf(cli.CodeOrcaUnavailable, "orca: %w", context.Canceled), cli.CodeOrcaUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cli.Classify(tt.err); got != tt.want {
				t.Errorf("Classify() = %q, want %q", got, tt.want)
			}
		})
	}
}
