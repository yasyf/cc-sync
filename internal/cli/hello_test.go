package cli_test

import (
	"bytes"
	"testing"

	"github.com/yasyf/cc-sync/internal/cli"
)

func TestHello(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "hello greets", args: []string{"hello"}, want: "Hello from cc-sync!\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			root := cli.NewRootCmd()
			root.SetOut(&out)
			root.SetErr(&out)
			root.SetArgs(tt.args)
			if err := root.Execute(); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if got := out.String(); got != tt.want {
				t.Errorf("output = %q, want %q", got, tt.want)
			}
		})
	}
}
