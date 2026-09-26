package claudenative

import (
	"testing"
	"time"
)

func TestClassifyRecord(t *testing.T) {
	at := time.Date(2026, 9, 26, 19, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		line    string
		want    Activity
		wantAt  time.Time
		wantErr bool
	}{
		{
			name:   "typed human prompt",
			line:   `{"type":"user","origin":{"kind":"human"},"message":{"role":"user","content":"fix it"},"timestamp":"2026-09-26T19:00:00.000Z"}`,
			want:   ActivityHuman,
			wantAt: at,
		},
		{
			name:   "human with null tool result",
			line:   `{"type":"user","origin":{"kind":"human"},"toolUseResult":null,"timestamp":"2026-09-26T19:00:00Z"}`,
			want:   ActivityHuman,
			wantAt: at,
		},
		{
			name:   "human meta injection",
			line:   `{"type":"user","origin":{"kind":"human"},"isMeta":true,"timestamp":"2026-09-26T19:00:00Z"}`,
			want:   ActivityNone,
			wantAt: at,
		},
		{
			name:   "tool result object",
			line:   `{"type":"user","toolUseResult":{"stdout":"ok"},"message":{"content":[{"type":"tool_result"}]},"timestamp":"2026-09-26T19:00:00Z"}`,
			want:   ActivityAutonomous,
			wantAt: at,
		},
		{
			name:   "tool result string",
			line:   `{"type":"user","origin":{"kind":"human"},"toolUseResult":"Error: denied","timestamp":"2026-09-26T19:00:00Z"}`,
			want:   ActivityAutonomous,
			wantAt: at,
		},
		{
			name:   "task notification",
			line:   `{"type":"user","origin":{"kind":"task-notification"},"timestamp":"2026-09-26T19:00:00Z"}`,
			want:   ActivityAutonomous,
			wantAt: at,
		},
		{
			name:   "peer message",
			line:   `{"type":"user","isMeta":true,"origin":{"kind":"peer","from":"x","name":"y","body":"z","senderTaskId":"t"},"timestamp":"2026-09-26T19:00:00Z"}`,
			want:   ActivityAutonomous,
			wantAt: at,
		},
		{
			name:   "user without origin",
			line:   `{"type":"user","message":{"content":"legacy"},"timestamp":"2026-09-26T19:00:00Z"}`,
			want:   ActivityNone,
			wantAt: at,
		},
		{
			name:   "queue enqueue",
			line:   `{"type":"queue-operation","operation":"enqueue","content":"next","timestamp":"2026-09-26T19:00:00Z","sessionId":"s"}`,
			want:   ActivityNone,
			wantAt: at,
		},
		{
			name:   "queue dequeue",
			line:   `{"type":"queue-operation","operation":"dequeue","timestamp":"2026-09-26T19:00:00Z","sessionId":"s"}`,
			want:   ActivityNone,
			wantAt: at,
		},
		{name: "assistant", line: `{"type":"assistant","message":{"content":[]},"timestamp":"2026-09-26T19:00:00Z"}`, want: ActivityAutonomous, wantAt: at},
		{
			name:   "queued human prompt",
			line:   `{"type":"attachment","uuid":"q1","timestamp":"2026-09-26T19:00:00Z","attachment":{"type":"queued_command","prompt":"next","commandMode":"prompt","origin":{"kind":"human"}}}`,
			want:   ActivityHuman,
			wantAt: at,
		},
		{
			name:   "queued human meta prompt",
			line:   `{"type":"attachment","uuid":"q1","timestamp":"2026-09-26T19:00:00Z","attachment":{"type":"queued_command","prompt":"next","commandMode":"prompt","isMeta":true,"origin":{"kind":"human"}}}`,
			want:   ActivityNone,
			wantAt: at,
		},
		{
			name:   "queued task notification",
			line:   `{"type":"attachment","uuid":"q1","timestamp":"2026-09-26T19:00:00Z","attachment":{"type":"queued_command","prompt":"<task-notification/>","commandMode":"task-notification"}}`,
			want:   ActivityAutonomous,
			wantAt: at,
		},
		{
			name:   "queued peer message",
			line:   `{"type":"attachment","uuid":"q1","timestamp":"2026-09-26T19:00:00Z","attachment":{"type":"queued_command","prompt":"hi","commandMode":"prompt","isMeta":true,"origin":{"kind":"peer"}}}`,
			want:   ActivityAutonomous,
			wantAt: at,
		},
		{
			name:   "scheduled task fire",
			line:   `{"type":"system","subtype":"scheduled_task_fire","content":"fired","isMeta":true,"timestamp":"2026-09-26T19:00:00Z"}`,
			want:   ActivityAutonomous,
			wantAt: at,
		},
		{name: "last-prompt", line: `{"type":"last-prompt","lastPrompt":"p","leafUuid":"u","sessionId":"s"}`, want: ActivityNone},
		{name: "ai-title", line: `{"type":"ai-title","aiTitle":"t","sessionId":"s"}`, want: ActivityNone},
		{name: "custom-title", line: `{"type":"custom-title","customTitle":"t","sessionId":"s"}`, want: ActivityNone},
		{name: "relocated", line: `{"type":"relocated","relocatedCwd":"/x","sessionId":"s"}`, want: ActivityNone},
		{name: "mode", line: `{"type":"mode","mode":"normal","sessionId":"s"}`, want: ActivityNone},
		{name: "agent-name", line: `{"type":"agent-name","agentName":"a","sessionId":"s"}`, want: ActivityNone},
		{name: "worktree-state", line: `{"type":"worktree-state","worktreeSession":null,"sessionId":"s"}`, want: ActivityNone},
		{name: "file-history-snapshot", line: `{"type":"file-history-snapshot","messageId":"m","snapshot":{}}`, want: ActivityNone},
		{name: "cost-state", line: `{"type":"cost-state","sessionId":"s"}`, want: ActivityNone},
		{name: "permission-mode", line: `{"type":"permission-mode","permissionMode":"default","sessionId":"s"}`, want: ActivityNone},
		{name: "invalid json", line: `{"type":"user"`, wantErr: true},
		{name: "not an object", line: `["user"]`, wantErr: true},
		{name: "bad timestamp", line: `{"type":"assistant","timestamp":"yesterday"}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotAt, err := ClassifyRecord([]byte(tt.line))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ClassifyRecord() = %v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ClassifyRecord() error = %v", err)
			}
			if got != tt.want || !gotAt.Equal(tt.wantAt) {
				t.Errorf("ClassifyRecord() = %v, %v, want %v, %v", got, gotAt, tt.want, tt.wantAt)
			}
		})
	}
}

func TestClassifyMetadataRecords(t *testing.T) {
	at := time.Date(2026, 9, 26, 19, 0, 0, 0, time.UTC)
	check := func(t *testing.T, line string) {
		t.Helper()
		got, gotAt, err := ClassifyRecord([]byte(line))
		if err != nil {
			t.Fatalf("ClassifyRecord() error = %v", err)
		}
		if got != ActivityNone || !gotAt.Equal(at) {
			t.Errorf("ClassifyRecord() = %v, %v, want %v, %v", got, gotAt, ActivityNone, at)
		}
	}
	for _, subtype := range []string{"turn_duration", "stop_hook_summary", "compact_boundary", "away_summary", "local_command", "informational"} {
		t.Run("system "+subtype, func(t *testing.T) {
			check(t, `{"type":"system","subtype":"`+subtype+`","isMeta":false,"timestamp":"2026-09-26T19:00:00Z"}`)
		})
	}
	for _, kind := range []string{
		"prompt_snapshot", "environment", "session_context", "skill_listing", "deferred_tools_delta", "deferred_tools_record",
		"agent_listing_delta", "credential_org", "plan_mode", "async_hook_response", "hook_success", "task_reminder", "edited_text_file",
	} {
		t.Run("attachment "+kind, func(t *testing.T) {
			check(t, `{"type":"attachment","uuid":"m1","timestamp":"2026-09-26T19:00:00Z","attachment":{"type":"`+kind+`"}}`)
		})
	}
}
