package rpcregistry

import (
	"crypto/sha256"
	"fmt"
	"os"
	"time"
)

// fileSource identifies the content of a registry file: its size and modification time, and the
// sha256 hash of its content. The poller compares the stat first, and the hash to tell a change of
// content from a touch.
type fileSource struct {
	size    int64
	modTime time.Time
	hash    [sha256.Size]byte
}

// fileSnapshot is the content of a registry file read at once, with the source of that content.
type fileSnapshot struct {
	fileSource
	data []byte
}

// snapshotFile stats and reads the registry file at path. The stat is taken before the read, so a
// change made during the read is seen by the next check.
func snapshotFile(path string) (*fileSnapshot, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("rpc registry file %s: %w", path, err)
	}
	data, err := os.ReadFile(path) //nolint:gosec // Path of the registry file set by the operator in the config.
	if err != nil {
		return nil, fmt.Errorf("rpc registry file %s: %w", path, err)
	}
	return &fileSnapshot{
		fileSource: fileSource{
			size:    info.Size(),
			modTime: info.ModTime(),
			hash:    sha256.Sum256(data),
		},
		data: data,
	}, nil
}
