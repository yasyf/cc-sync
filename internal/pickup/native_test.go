package pickup

import (
	"context"
	"testing"
	"time"
)

func TestNativeProbesOnce(t *testing.T) {
	probes := 0
	n := &Native{
		Run: func(_ context.Context, args ...string) ([]byte, error) {
			probes++
			if args[0] == "--version" {
				return []byte("2.1.90 (Claude Code)\n"), nil
			}
			return []byte("  -r, --resume [value]  Resume a conversation\n  --append-system-prompt <prompt>\n"), nil
		},
		Now: time.Now,
	}
	for range 2 {
		if _, err := n.Prepare(t.Context(), t.TempDir(), SessionTarget{}, DivergenceRefuse); err == nil {
			t.Fatal("Prepare of an empty replica succeeded")
		}
	}
	if probes != 2 || n.caps == nil || !n.caps.Resume || !n.caps.AppendSystemPrompt {
		t.Errorf("probes = %d caps = %+v, want one --version and one --help probe advertising resume and append-system-prompt", probes, n.caps)
	}
}
