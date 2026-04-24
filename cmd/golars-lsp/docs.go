package main

import (
	"encoding/json"
	"strings"
	"sync"
)

// docStore is an in-memory registry of open documents keyed by LSP
// URI. Content is kept verbatim (full text sync) so we don't have to
// deal with incremental-edit bookkeeping for this tiny language.
type docStore struct {
	mu sync.RWMutex
	m  map[string]*document
}

type document struct {
	uri     string
	content string
	lines   []string // split once, used by every feature
	version int32
}

func newDocStore() *docStore {
	return &docStore{m: make(map[string]*document)}
}

func (s *docStore) set(uri, content string, version int32) *document {
	doc := &document{
		uri:     uri,
		content: content,
		lines:   strings.Split(content, "\n"),
		version: version,
	}
	s.mu.Lock()
	s.m[uri] = doc
	s.mu.Unlock()
	return doc
}

func (s *docStore) get(uri string) *document {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.m[uri]
}

func (s *docStore) drop(uri string) {
	s.mu.Lock()
	delete(s.m, uri)
	s.mu.Unlock()
}

// --------------------------------------------------------------------
// didOpen / didChange / didClose handlers
// --------------------------------------------------------------------

type textDocumentItem struct {
	URI        string `json:"uri"`
	LanguageID string `json:"languageId"`
	Version    int32  `json:"version"`
	Text       string `json:"text"`
}

type didOpenParams struct {
	TextDocument textDocumentItem `json:"textDocument"`
}

func (s *server) handleDidOpen(msg *rawMessage) {
	var p didOpenParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		s.logf("didOpen unmarshal: %v", err)
		return
	}
	doc := s.docs.set(p.TextDocument.URI, p.TextDocument.Text, p.TextDocument.Version)
	s.publishDiagnostics(doc)
}

type versionedDocumentID struct {
	URI     string `json:"uri"`
	Version int32  `json:"version"`
}

type contentChange struct {
	Text string `json:"text"` // full-sync: the entire new document
}

type didChangeParams struct {
	TextDocument   versionedDocumentID `json:"textDocument"`
	ContentChanges []contentChange     `json:"contentChanges"`
}

func (s *server) handleDidChange(msg *rawMessage) {
	var p didChangeParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		s.logf("didChange unmarshal: %v", err)
		return
	}
	if len(p.ContentChanges) == 0 {
		return
	}
	// Full-sync: last change holds the full new text.
	doc := s.docs.set(p.TextDocument.URI, p.ContentChanges[len(p.ContentChanges)-1].Text, p.TextDocument.Version)
	s.publishDiagnostics(doc)
}

type textDocumentIdentifier struct {
	URI string `json:"uri"`
}

type didCloseParams struct {
	TextDocument textDocumentIdentifier `json:"textDocument"`
}

func (s *server) handleDidClose(msg *rawMessage) {
	var p didCloseParams
	if err := json.Unmarshal(msg.Params, &p); err != nil {
		return
	}
	s.docs.drop(p.TextDocument.URI)
	// Clear diagnostics on close so stale markers don't linger.
	s.notify("textDocument/publishDiagnostics", struct {
		URI         string `json:"uri"`
		Diagnostics []any  `json:"diagnostics"`
	}{p.TextDocument.URI, []any{}})
}
