package centrifuge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

const (
	emulationLogTestToken   = "secret-connect-token"
	emulationLogTestSession = "secret-session-id"
)

// emulationLogRecorder keeps every log entry a node writes.
type emulationLogRecorder struct {
	mu      sync.Mutex
	entries []LogEntry
}

func (r *emulationLogRecorder) handle(entry LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, entry)
}

func (r *emulationLogRecorder) snapshot() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]LogEntry(nil), r.entries...)
}

// postEmulation starts a node at level, posts body to its emulation endpoint and returns the status and the log.
func postEmulation(t *testing.T, level LogLevel, contentType string, body []byte) (int, []LogEntry) {
	t.Helper()
	recorder := &emulationLogRecorder{}
	n, err := New(Config{LogLevel: level, LogHandler: recorder.handle})
	require.NoError(t, err)
	require.NoError(t, n.Run())
	t.Cleanup(func() { _ = n.Shutdown(context.Background()) })
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/emulation", bytes.NewBuffer(body))
	require.NoError(t, err)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode, recorder.snapshot()
}

// requireNoSecret fails when any log entry carries the connect token or the session id of the request.
func requireNoSecret(t *testing.T, entries []LogEntry) {
	t.Helper()
	for _, entry := range entries {
		text := entry.Message + " " + fmt.Sprint(entry.Fields)
		require.NotContains(t, text, emulationLogTestToken)
		require.NotContains(t, text, emulationLogTestSession)
	}
}

func emulationRequestWithToken(t *testing.T) []byte {
	t.Helper()
	command := &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{Token: emulationLogTestToken}}
	data, err := json.Marshal(command)
	require.NoError(t, err)
	body, err := json.Marshal(&protocol.EmulationRequest{Node: "unknown", Session: emulationLogTestSession, Data: data})
	require.NoError(t, err)
	return body
}

func TestEmulationUnknownNodeLogsMetadataAtDebug(t *testing.T) {
	t.Parallel()
	status, entries := postEmulation(t, LogLevelDebug, "", emulationRequestWithToken(t))
	require.Equal(t, http.StatusNotFound, status)
	requireNoSecret(t, entries)

	var found bool
	for _, entry := range entries {
		if entry.Message == "emulation request for unknown node" {
			found = true
			require.Equal(t, LogLevelDebug, entry.Level)
			require.Equal(t, "unknown", entry.Fields["node"])
			require.Equal(t, "json", entry.Fields["protocol"])
			require.NotZero(t, entry.Fields["size"])
		}
		require.NotEqual(t, LogLevelError, entry.Level, "a client-caused miss is not a server error: %s", entry.Message)
	}
	require.True(t, found)
}

func TestEmulationUnknownNodeLogsNothingAtInfo(t *testing.T) {
	t.Parallel()
	status, entries := postEmulation(t, LogLevelInfo, "", emulationRequestWithToken(t))
	require.Equal(t, http.StatusNotFound, status)
	for _, entry := range entries {
		require.False(t, strings.Contains(entry.Message, "emulation"), "unexpected emulation log at info: %s", entry.Message)
	}
}

func TestEmulationBadRequestLogsNoBody(t *testing.T) {
	t.Parallel()
	body := []byte(`{"node":"unknown","session":"` + emulationLogTestSession + `","data":"` + emulationLogTestToken)
	status, entries := postEmulation(t, LogLevelDebug, "", body)
	require.Equal(t, http.StatusBadRequest, status)
	requireNoSecret(t, entries)

	var found bool
	for _, entry := range entries {
		if entry.Message == "can't unmarshal emulation request" {
			found = true
			require.Equal(t, LogLevelDebug, entry.Level)
			require.Equal(t, len(body), entry.Fields["size"])
		}
	}
	require.True(t, found)
}

func TestEmulationBadProtobufLogsNoBody(t *testing.T) {
	t.Parallel()
	// 0x0f is field 1 with wire type 7, which no protobuf message accepts.
	body := []byte("\x0f" + emulationLogTestToken + emulationLogTestSession)
	status, entries := postEmulation(t, LogLevelDebug, "application/octet-stream", body)
	require.Equal(t, http.StatusBadRequest, status)
	requireNoSecret(t, entries)
}
