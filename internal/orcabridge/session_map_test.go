package orcabridge

import (
	"reflect"
	"testing"
)

func TestImportArgsSessionMap(t *testing.T) {
	args, err := importArgs(ImportRequest{
		Checkout: "/dst", CheckpointID: "cp-1", Resume: []string{"local-b"},
		SessionIDMap: []SessionMapping{{From: "src-a", To: "local-a"}, {From: "src-b", To: "local-b"}},
	})
	if err != nil {
		t.Fatalf("importArgs: %v", err)
	}
	want := []string{
		"recovery", "import", "--descriptor", "-", "--checkout", "/dst", "--checkpoint", "cp-1",
		"--resume", "local-b", "--session-map", "src-a=local-a", "--session-map", "src-b=local-b",
	}
	if !reflect.DeepEqual(args[:len(want)], want) {
		t.Errorf("args = %q\nwant prefix %q", args, want)
	}
}
