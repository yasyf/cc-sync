package capture

import (
	"cmp"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
)

const orcaKindWorktree = "worktree"

func summarize(descriptor []byte, exportedAt time.Time) (catalog.Orca, error) {
	var d struct {
		Workspace struct {
			InstanceID string `json:"instanceId"`
			Path       string `json:"path"`
			Branch     string `json:"branch"`
			Meta       struct {
				DisplayName string `json:"displayName"`
			} `json:"meta"`
		} `json:"workspace"`
	}
	if err := json.Unmarshal(descriptor, &d); err != nil {
		return catalog.Orca{}, fmt.Errorf("decode orca descriptor: %w", err)
	}
	w := d.Workspace
	if w.InstanceID == "" {
		return catalog.Orca{}, fmt.Errorf("orca descriptor of %s lacks a workspace instance id", w.Path)
	}
	return catalog.Orca{
		Kind:       orcaKindWorktree,
		Name:       cmp.Or(w.Meta.DisplayName, w.Branch, filepath.Base(w.Path)),
		InstanceID: w.InstanceID,
		Freshness:  exportedAt,
	}, nil
}
