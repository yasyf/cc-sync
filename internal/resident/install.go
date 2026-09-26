package resident

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/yasyf/cc-sync/internal/catalog"
	"github.com/yasyf/cc-sync/internal/config"
	"github.com/yasyf/cc-sync/internal/consumer"
	"github.com/yasyf/synckit/codec"
	"github.com/yasyf/synckit/manifest"
)

// Synckitd is the synckit daemon binary install and uninstall drive.
const Synckitd = "synckitd"

// HelperCommand is the hidden cc-sync subcommand the helper LaunchAgent runs.
const HelperCommand = "helper-serve"

// Runner runs one external command to completion.
type Runner func(ctx context.Context, name string, args ...string) error

// ExecRunner runs the command with exec, folding its output into the error.
func ExecRunner(ctx context.Context, name string, args ...string) error {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput() //nolint:gosec // G204: callers pass the synckitd binary and a fixed verb argv, never a shell string.
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Manifest is the consumer manifest cc-sync registers with synckitd.
func Manifest() manifest.Manifest {
	return manifest.Manifest{
		Name:    consumer.ServiceID,
		Binary:  consumer.ServiceID,
		Brew:    "yasyf/tap/cc-sync",
		Watch:   manifest.WatchSpec{Debounce: codec.Duration(2 * time.Second)},
		Service: manifest.ServiceSpec{Kind: "resident", SchemaFingerprint: consumer.Fingerprint},
		Helper:  &manifest.HelperSpec{Command: HelperCommand, SessionType: manifest.SessionTypeAqua},
	}
}

// Ensure creates the layout and the first stamp.
func Ensure(layout config.Layout, self string) error {
	if err := layout.Ensure(); err != nil {
		return err
	}
	return catalog.NewPublisher(catalog.New(layout.CatalogPath, self, time.Now), layout.StampDir).Ensure()
}

// Install runs Ensure, registers the manifest through a temp file with
// `synckitd register`, and converges the helper LaunchAgent with
// `synckitd install`.
func Install(ctx context.Context, run Runner, layout config.Layout, self string) (err error) {
	if err := Ensure(layout, self); err != nil {
		return err
	}
	data, err := json.MarshalIndent(Manifest(), "", "  ")
	if err != nil {
		return fmt.Errorf("resident: encode manifest: %w", err)
	}
	tmp, err := os.CreateTemp("", "cc-sync-manifest-*.json")
	if err != nil {
		return fmt.Errorf("resident: manifest temp file: %w", err)
	}
	defer func() { err = errors.Join(err, os.Remove(tmp.Name())) }()
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		return errors.Join(fmt.Errorf("resident: write manifest: %w", err), tmp.Close())
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("resident: write manifest: %w", err)
	}
	if err := run(ctx, Synckitd, "register", tmp.Name()); err != nil {
		return err
	}
	return run(ctx, Synckitd, "install")
}

// Uninstall unregisters the manifest, lets `synckitd install` sweep the
// helper LaunchAgent, and then removes each purge path.
func Uninstall(ctx context.Context, run Runner, purge []string) error {
	if err := run(ctx, Synckitd, "unregister", consumer.ServiceID); err != nil {
		return err
	}
	if err := run(ctx, Synckitd, "install"); err != nil {
		return err
	}
	for _, path := range purge {
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("resident: purge %s: %w", path, err)
		}
	}
	return nil
}
