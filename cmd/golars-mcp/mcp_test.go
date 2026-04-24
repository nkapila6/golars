package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestInitialize exercises the base protocol handshake and verifies
// the server self-describes.
func TestInitialize(t *testing.T) {
	in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n")
	var out bytes.Buffer
	if err := serve(bufio.NewReader(in), &out); err != nil {
		t.Fatal(err)
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v - raw: %s", err, out.String())
	}
	if resp.Error != nil {
		t.Fatalf("error: %v", resp.Error)
	}
	m, _ := resp.Result.(map[string]any)
	info, _ := m["serverInfo"].(map[string]any)
	if info["name"] != "golars-mcp" {
		t.Fatalf("serverInfo.name = %v, want golars-mcp", info["name"])
	}
}

// TestToolsList checks that every registered tool is discoverable.
func TestToolsList(t *testing.T) {
	req := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}` + "\n"
	var out bytes.Buffer
	if err := serve(bufio.NewReader(strings.NewReader(req)), &out); err != nil {
		t.Fatal(err)
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	m, _ := resp.Result.(map[string]any)
	items, _ := m["tools"].([]any)
	if len(items) < 5 {
		t.Fatalf("tools: got %d items, want ≥ 5", len(items))
	}
}

// TestUnknownMethod surfaces a sensible error.
func TestUnknownMethod(t *testing.T) {
	req := `{"jsonrpc":"2.0","id":1,"method":"no/such/thing"}` + "\n"
	var out bytes.Buffer
	if err := serve(bufio.NewReader(strings.NewReader(req)), &out); err != nil {
		t.Fatal(err)
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil {
		t.Fatal("expected error, got success")
	}
	if resp.Error.Code != -32601 {
		t.Fatalf("error code: got %d want -32601", resp.Error.Code)
	}
}
