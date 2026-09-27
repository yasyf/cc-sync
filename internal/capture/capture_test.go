package capture

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/claudenative"
	"github.com/yasyf/cc-sync/internal/codesnap"
	"github.com/yasyf/cc-sync/internal/inventory"
	"github.com/yasyf/cc-sync/internal/orcabridge"
	"github.com/yasyf/cc-sync/internal/scheduler"
	"github.com/yasyf/cc-sync/internal/sessionarchive"
	"github.com/yasyf/reposync/worktree"
	"github.com/yasyf/synckit/artifact"
)

type harness struct {
	store     *fakeStore
	code      *fakeCode
	stamper   *fakeStamper
	orca      *fakeOrca
	catalog   *fakeCatalog
	publisher *fakePublisher
	sessions  []claudenative.Session
	now       time.Time
	job       *Job
	meta      string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir := t.TempDir()
	h := &harness{
		store:     newFakeStore(),
		code:      &fakeCode{files: map[string]string{"a.txt": "one"}},
		stamper:   &fakeStamper{stamp: "code-1"},
		orca:      &fakeOrca{descriptor: descriptor("inst-1")},
		catalog:   &fakeCatalog{},
		publisher: &fakePublisher{},
		now:       t0,
		meta:      "meta-1",
	}
	config := dir + "/claude"
	h.sessions = []claudenative.Session{nativeSession(t, config, sidA, t0.Add(-time.Minute)), nativeSession(t, config, sidB, t0.Add(-2*time.Hour))}
	wt := worktree.Worktree{
		ID: wtID, Origin: "https://example.com/r.git", Relpath: "r", Trunk: "main",
		Root: "/src/r", GitDir: "/src/r/.git", CommonDir: "/src/r/.git", Kind: worktree.KindGit, Branch: "feat", Head: oid('1'), Incarnation: 7,
	}
	h.job = New(Config{
		Self:      self,
		Layout:    claudenative.Layout{ConfigDir: config, TmpRoot: "/private/tmp/claude-501", UID: 501},
		Home:      "/Users/src",
		Store:     h.store,
		Code:      h.code,
		Stamper:   h.stamper,
		Orca:      h.orca,
		Catalog:   h.catalog,
		Publisher: h.publisher,
		Targets:   targetsOf(wt, h),
		CodeIndex: dir + "/codesnap",
		StateDir:  dir + "/capture",
		Now:       func() time.Time { return h.now },
	})
	return h
}

func targetsOf(wt worktree.Worktree, h *harness) Targets {
	return targetFunc(func(id string) (inventory.Target, bool) {
		if id != wt.ID {
			return inventory.Target{}, false
		}
		return inventory.Target{Worktree: wt, Sessions: h.sessions}, true
	})
}

type targetFunc func(string) (inventory.Target, bool)

func (f targetFunc) Target(id string) (inventory.Target, bool) { return f(id) }

func (h *harness) capture(t *testing.T) scheduler.Result {
	t.Helper()
	res, err := h.job.Capture(context.Background(), scheduler.Unit{WorktreeID: wtID, RepoKey: "/src/r/.git", MetaStamp: h.meta})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	return res
}

func (h *harness) touchSession(t *testing.T, uuid string) {
	t.Helper()
	h.now = h.now.Add(time.Minute)
	appendTranscript(t, h.sessions[0], transcriptRecord(uuid, h.now))
	h.sessions[0].LastActivity = h.now
	h.meta += "+" + uuid
}

func TestCaptureUnchangedWritesNothing(t *testing.T) {
	h := newHarness(t)
	first := h.capture(t)
	if first.Outcome != scheduler.OutcomeCaptured || first.Checkpoint == "" {
		t.Fatalf("first capture = %+v, want captured", first)
	}
	puts, codeCalls, orcaCalls, kicks := h.store.putCount(), h.code.calls, h.orca.calls, h.publisher.kicks
	second := h.capture(t)
	if second != (scheduler.Result{Outcome: scheduler.OutcomeUnchanged, Checkpoint: first.Checkpoint}) {
		t.Fatalf("second capture = %+v, want unchanged %s", second, first.Checkpoint)
	}
	if h.store.putCount() != puts || h.code.calls != codeCalls || h.orca.calls != orcaCalls || h.publisher.kicks != kicks || len(h.catalog.records) != 1 {
		t.Fatalf("unchanged capture wrote: puts %d→%d, code %d→%d, orca %d→%d, kicks %d→%d, records %d",
			puts, h.store.putCount(), codeCalls, h.code.calls, orcaCalls, h.orca.calls, kicks, h.publisher.kicks, len(h.catalog.records))
	}
}

func TestCaptureRecordsCheckpoint(t *testing.T) {
	h := newHarness(t)
	res := h.capture(t)
	rec := h.catalog.last(t)
	wantRepo := catalog.Repo{Origin: "https://example.com/r.git", RelPath: "r", Branch: "feat", SourcePath: "/src/r"}
	if rec.wt.ID != wtID || rec.wt.Repo != wantRepo {
		t.Fatalf("worktree = %+v, want id %s repo %+v", rec.wt, wtID, wantRepo)
	}
	if want := (catalog.Orca{Kind: "worktree", Name: "Feature", InstanceID: "inst-1", Freshness: t0}); rec.wt.Orca == nil || *rec.wt.Orca != want {
		t.Fatalf("orca = %+v, want %+v", rec.wt.Orca, want)
	}
	cp := rec.cp
	if cp.ID != res.Checkpoint || !cp.CapturedAt.Equal(t0) || !cp.SourceActivityAt.Equal(t0.Add(-time.Minute)) || cp.Deferred != "" {
		t.Fatalf("checkpoint = %+v, result %+v", cp, res)
	}
	if !cp.Completeness.Complete || len(cp.Completeness.Missing) != 0 || !cp.Code.Complete || cp.Code.Untracked != 1 {
		t.Fatalf("completeness %+v code %+v, want complete with one untracked file", cp.Completeness, cp.Code)
	}
	gotSessions := []string{cp.Sessions[0].ID + "/" + cp.Sessions[0].Activity, cp.Sessions[1].ID + "/" + cp.Sessions[1].Activity}
	if want := []string{sidA + "/human", sidB + "/idle"}; !slices.Equal(gotSessions, want) {
		t.Fatalf("sessions = %v, want %v", gotSessions, want)
	}
	deps := h.store.deps(t, cp.Root)
	wantMedia := []string{codesnap.MediaCode, orcabridge.MediaDescriptor, sessionarchive.MediaSession, sessionarchive.MediaSession}
	gotMedia := make([]string, len(deps))
	for i, d := range deps {
		gotMedia[i] = h.store.media(t, d)
	}
	if h.store.media(t, cp.Root) != MediaCheckpoint || !slices.Equal(gotMedia, wantMedia) {
		t.Fatalf("root %s deps media = %v, want %v", h.store.media(t, cp.Root), gotMedia, wantMedia)
	}
	var m sessionarchive.Manifest
	if err := json.Unmarshal(h.store.blob(t, h.store.deps(t, deps[2])[0]), &m); err != nil {
		t.Fatal(err)
	}
	if m.Home != "/Users/src" || m.TmpRoot != "/private/tmp/claude-501" || m.UID != 501 {
		t.Fatalf("session manifest home %q tmp root %q uid %d, want the source layout", m.Home, m.TmpRoot, m.UID)
	}
	if h.publisher.kicks != 1 {
		t.Fatalf("publisher kicks = %d, want 1", h.publisher.kicks)
	}
}

func TestCaptureChecksReferencesIntoUndiscoveredSidecars(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		present []string
		missing []string
	}{
		{
			name:    "tool result under an absent session dir",
			line:    `{"type":"user","toolUseResult":"Full output saved to: {{SESSION_DIR}}/tool-results/gone.txt"}`,
			missing: []string{"session/tool-results/gone.txt"},
		},
		{
			name:    "backup under an absent file history",
			line:    `{"type":"file-history-snapshot","snapshot":{"trackedFileBackups":{"/a.go":{"backupFileName":"abc@v1"}}}}`,
			missing: []string{"file-history/abc@v1"},
		},
		{
			name:    "tool result the inventory had not seen",
			line:    `{"type":"user","toolUseResult":"Full output saved to: {{SESSION_DIR}}/tool-results/late.txt"}`,
			present: []string{"tool-results/late.txt"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			s := h.sessions[0]
			sessionDir := strings.TrimSuffix(s.TranscriptPath, ".jsonl")
			appendTranscript(t, s, strings.ReplaceAll(tt.line, "{{SESSION_DIR}}", sessionDir)+"\n")
			for _, rel := range tt.present {
				path := filepath.Join(sessionDir, rel)
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("spilled"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			h.capture(t)
			got := h.catalog.last(t).cp.Completeness
			want := catalog.Completeness{Complete: len(tt.missing) == 0, Missing: named("session:"+sidA+":missing:", tt.missing)}
			if got.Complete != want.Complete || !slices.Equal(got.Missing, want.Missing) {
				t.Fatalf("completeness = %+v, want %+v", got, want)
			}
		})
	}
}

func TestCaptureCodeOnlyEditViaStamper(t *testing.T) {
	h := newHarness(t)
	first := h.capture(t)
	firstCode := h.store.deps(t, h.catalog.last(t).cp.Root)[0]
	h.now = h.now.Add(2 * time.Minute)
	h.stamper.stamp = "code-2"
	h.code.files["b.txt"] = "untracked"
	second := h.capture(t)
	if second.Outcome != scheduler.OutcomeCaptured || second.Checkpoint == first.Checkpoint {
		t.Fatalf("code-only edit = %+v, want a new checkpoint after %s", second, first.Checkpoint)
	}
	cp := h.catalog.last(t).cp
	if code := h.store.deps(t, cp.Root)[0]; code == firstCode || cp.Code.Untracked != 2 {
		t.Fatalf("code dep %v (first %v) untracked %d, want a new code root with 2 files", code, firstCode, cp.Code.Untracked)
	}
}

func TestCaptureDeferredKeepsLastCompleteCode(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		files   map[string]string
		outcome scheduler.Outcome
		state   string
		missing []string
	}{
		{
			name:    "mid-operation",
			err:     &worktree.DeferredError{Reason: "rebase"},
			outcome: scheduler.OutcomeDeferred,
			state:   "deferred:rebase",
		},
		{
			name:    "partial",
			err:     &worktree.PartialError{Remaining: []string{"big/a.bin", "big/b.bin"}, Reason: worktree.PartialNewBytes},
			outcome: scheduler.OutcomePartial,
			state:   "deferred:partial-max-new-bytes",
			missing: []string{"code:big/a.bin", "code:big/b.bin"},
		},
		{
			name:    "missing lfs",
			err:     &worktree.MissingLFSError{Objects: []worktree.LFSObjectRef{{Path: "assets/x.bin", OID: "ab12", Size: 3}}},
			outcome: scheduler.OutcomeMissingLFS,
			state:   "deferred:missing-lfs",
			missing: []string{"lfs:assets/x.bin@ab12"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.capture(t)
			first := h.catalog.last(t).cp
			firstCode := h.store.deps(t, first.Root)[0]
			h.code.err = tt.err
			h.stamper.stamp = "code-2"
			h.touchSession(t, "u-next")
			res := h.capture(t)
			cp := h.catalog.last(t).cp
			if res != (scheduler.Result{Outcome: tt.outcome, Checkpoint: cp.ID, Reason: tt.state}) || cp.ID == first.ID {
				t.Fatalf("result = %+v, want %s/%s with a new checkpoint", res, tt.outcome, tt.state)
			}
			if deps := h.store.deps(t, cp.Root); deps[0] != firstCode {
				t.Fatalf("code dependency = %v, want the last complete %v", deps[0], firstCode)
			}
			if cp.Deferred != tt.state || cp.Completeness.Complete || !slices.Equal(cp.Completeness.Missing, tt.missing) {
				t.Fatalf("checkpoint deferred %q completeness %+v, want %q missing %v", cp.Deferred, cp.Completeness, tt.state, tt.missing)
			}
			if !cp.Code.CapturedAt.Equal(first.Code.CapturedAt) || cp.Code.Digest != first.Code.Digest {
				t.Fatalf("code summary = %+v, want the last complete %+v", cp.Code, first.Code)
			}
			if !cp.Mixed() || first.Mixed() {
				t.Fatalf("mixed = %t over first %t, want only the new checkpoint mixed", cp.Mixed(), first.Mixed())
			}
			snap := catalog.Snapshot{Self: self}
			if r := snap.ReadinessOf(self, cp); r.Ready || r.Deferred != tt.state {
				t.Fatalf("mixed readiness = %+v, want not ready, deferred %q", r, tt.state)
			}
			if !snap.ReadinessOf(self, first).Ready {
				t.Fatal("the earlier complete checkpoint is not ready")
			}
			mixed, complete := cp, first
			mixed.ExpiresAt, complete.ExpiresAt = t0.Add(24*time.Hour), t0.Add(24*time.Hour)
			kept := catalog.Retain([]catalog.Checkpoint{mixed, complete}, t0.Add(time.Minute))
			if len(kept) != 2 || kept[1].ID != first.ID {
				t.Fatalf("retained %+v, want the mixed checkpoint and the last complete %s", kept, first.ID)
			}
			again := h.capture(t)
			if again != (scheduler.Result{Outcome: tt.outcome, Checkpoint: cp.ID, Reason: tt.state}) || len(h.catalog.records) != 2 {
				t.Fatalf("retry = %+v with %d records, want the same deferred checkpoint", again, len(h.catalog.records))
			}
		})
	}
}

func TestCaptureOmittedWithoutPriorCode(t *testing.T) {
	h := newHarness(t)
	h.code.err = &worktree.DeferredError{Reason: "merge"}
	res := h.capture(t)
	cp := h.catalog.last(t).cp
	if res.Outcome != scheduler.OutcomeDeferred || cp.Deferred != "deferred:merge" || cp.Code != (worktree.Summary{}) {
		t.Fatalf("result %+v checkpoint deferred %q code %+v, want deferred with no code", res, cp.Deferred, cp.Code)
	}
	for _, d := range h.store.deps(t, cp.Root) {
		if m := h.store.media(t, d); m == codesnap.MediaCode {
			t.Fatalf("root depends on code %v with no complete capture", d)
		}
	}
}

func TestCaptureBusyRequeuesWithoutCheckpoint(t *testing.T) {
	h := newHarness(t)
	h.code.err = worktree.ErrBusy
	h.code.files = map[string]string{}
	puts := h.store.putCount()
	res := h.capture(t)
	if res != (scheduler.Result{Outcome: scheduler.OutcomeBusy, Reason: "busy"}) {
		t.Fatalf("busy = %+v", res)
	}
	if h.store.putCount() != puts || len(h.catalog.records) != 0 || h.orca.calls != 0 || h.publisher.kicks != 0 {
		t.Fatalf("busy capture wrote: puts %d→%d records %d orca %d kicks %d", puts, h.store.putCount(), len(h.catalog.records), h.orca.calls, h.publisher.kicks)
	}
	h.code.err = nil
	if res := h.capture(t); res.Outcome != scheduler.OutcomeCaptured || h.code.calls != 2 {
		t.Fatalf("retry = %+v after %d code calls, want captured", res, h.code.calls)
	}
}

func TestCapturePinsPartialProgress(t *testing.T) {
	h := newHarness(t)
	h.code.err = &worktree.PartialError{Remaining: []string{"c.txt"}, Reason: worktree.PartialEntries}
	h.capture(t)
	pinned := h.store.pins[owner]
	if len(pinned) != 1 {
		t.Fatalf("pins after partial = %v, want one root", pinned)
	}
	put, err := newFakeStore().Put(context.Background(), strings.NewReader("one"), string(worktree.MediaFile))
	if err != nil {
		t.Fatal(err)
	}
	if !h.store.reaches(t, pinned[0], put.Digest) || h.store.media(t, pinned[0]) != MediaPartial {
		t.Fatalf("pinned root %v does not hold the partial capture's content %v", pinned[0], put.Digest)
	}
	h.code.files["c.txt"] = "second"
	h.stamper.stamp = "code-2"
	h.capture(t)
	if got := h.store.pins[owner]; len(got) != 2 || got[0] != pinned[0] {
		t.Fatalf("pins after second partial = %v, want %v plus one", got, pinned)
	}
	h.code.err = nil
	h.stamper.stamp = "code-3"
	if res := h.capture(t); res.Outcome != scheduler.OutcomeCaptured {
		t.Fatalf("complete capture = %+v", res)
	}
	if got, ok := h.store.pins[owner]; ok {
		t.Fatalf("pins after complete snapshot = %v, want none", got)
	}
}

func TestCapturePartialPinsExpire(t *testing.T) {
	tests := []struct {
		name  string
		after time.Duration
		kept  bool
	}{
		{"fresh", PartialPinTTL - time.Second, true},
		{"expired", PartialPinTTL, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.code.err = &worktree.PartialError{Remaining: []string{"c.txt"}, Reason: worktree.PartialEntries}
			h.capture(t)
			h.now = h.now.Add(tt.after)
			if err := h.job.ExpirePins(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, ok := h.store.pins[owner]; ok != tt.kept {
				t.Fatalf("pinned after %s = %v, want %v", tt.after, ok, tt.kept)
			}
		})
	}
}

func TestCaptureOrcaUnavailableKeepsLastDescriptor(t *testing.T) {
	tests := []struct {
		name string
		err  error
		keep bool
	}{
		{"unavailable", &orcabridge.UnavailableError{Reason: orcabridge.ReasonNotRunning}, true},
		{"not an orca worktree", &orcabridge.RefusedError{Code: orcabridge.CodeSelectorNotFound}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.capture(t)
			firstDesc := h.store.deps(t, h.catalog.last(t).cp.Root)[1]
			h.orca.err = tt.err
			h.touchSession(t, "u-orca")
			if res := h.capture(t); res.Outcome != scheduler.OutcomeCaptured {
				t.Fatalf("capture = %+v", res)
			}
			rec := h.catalog.last(t)
			hasDesc := h.store.deps(t, rec.cp.Root)[1] == firstDesc
			if hasDesc != tt.keep || (rec.wt.Orca != nil) != tt.keep {
				t.Fatalf("descriptor kept %v orca %+v, want kept %v", hasDesc, rec.wt.Orca, tt.keep)
			}
			if tt.keep && !rec.wt.Orca.Freshness.Equal(t0) {
				t.Fatalf("orca freshness = %s, want the last export %s", rec.wt.Orca.Freshness, t0)
			}
		})
	}
}

func TestCaptureRecordsOmittedBindings(t *testing.T) {
	h := newHarness(t)
	h.orca.descriptor = descriptorOmitting("inst-1", `[{"agent":"codex","key":"session_id","id":"c-1","reason":"agent-not-supported-v1"}]`)
	h.capture(t)
	want := []catalog.OmittedBinding{{Agent: "codex", Key: "session_id", ID: "c-1", Reason: "agent-not-supported-v1"}}
	if got := h.catalog.last(t).cp.Omitted; !slices.Equal(got, want) {
		t.Fatalf("omitted = %+v, want %+v", got, want)
	}
	h.orca.descriptor = descriptor("inst-1")
	h.touchSession(t, "u-omit")
	h.capture(t)
	if got := h.catalog.last(t).cp.Omitted; got != nil {
		t.Fatalf("omitted after the binding left = %+v, want none", got)
	}
}

func TestCaptureRefusesDescriptorWithoutOmittedBindings(t *testing.T) {
	h := newHarness(t)
	h.orca.descriptor = `{"version":1,"workspace":{"worktreeId":"w1","instanceId":"inst-1","path":"/src/r","branch":"feat"}}`
	_, err := h.job.Capture(context.Background(), scheduler.Unit{WorktreeID: wtID, MetaStamp: h.meta})
	if r, ok := errors.AsType[*orcabridge.RefusedError](err); !ok || r.Code != orcabridge.CodeDescriptorInvalid {
		t.Fatalf("Capture error = %v, want %s", err, orcabridge.CodeDescriptorInvalid)
	}
	if len(h.catalog.records) != 0 {
		t.Fatalf("invalid descriptor recorded %d checkpoints", len(h.catalog.records))
	}
}

func TestCaptureRefusesRootOverClosureBound(t *testing.T) {
	h := newHarness(t)
	h.job.cfg.Bound = artifact.ClosureBound{MaxObjects: 3, MaxDepth: 32, MaxBytes: 1 << 30}
	_, err := h.job.Capture(context.Background(), scheduler.Unit{WorktreeID: wtID, MetaStamp: h.meta})
	var bound *artifact.ClosureError
	if !errors.As(err, &bound) || bound.Bound != artifact.BoundObjects {
		t.Fatalf("Capture error = %v, want a closure bound error", err)
	}
	if len(h.catalog.records) != 0 || h.publisher.kicks != 0 {
		t.Fatalf("over-bound root published: %d records, %d kicks", len(h.catalog.records), h.publisher.kicks)
	}
}

func TestCaptureUnknownWorktree(t *testing.T) {
	h := newHarness(t)
	_, err := h.job.Capture(context.Background(), scheduler.Unit{WorktreeID: "ffffffffffffffffffffffffffffffff"})
	if !errors.Is(err, ErrUnknownWorktree) {
		t.Fatalf("Capture error = %v, want ErrUnknownWorktree", err)
	}
}

func TestCodeStamp(t *testing.T) {
	h := newHarness(t)
	got, err := h.job.CodeStamp(context.Background(), scheduler.Unit{WorktreeID: wtID})
	if err != nil || got != "code-1" {
		t.Fatalf("CodeStamp = %q, %v; want code-1", got, err)
	}
}
