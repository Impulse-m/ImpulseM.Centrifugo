package rpcregistry

import (
	"crypto/sha256"
	"fmt"
	"io"
	"math"
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
// change made during the read is seen by the next check. When limit is positive, the read stops at
// limit bytes and a file which is over it is refused, so a file which grows after the stat is not read
// whole. Zero reads the file whole.
func snapshotFile(path string, limit int64) (*fileSnapshot, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("rpc registry file %s: %w", path, err)
	}
	file, err := os.Open(path) //nolint:gosec // Path of the registry file set by the operator in the config.
	if err != nil {
		return nil, fmt.Errorf("rpc registry file %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()
	var reader io.Reader = file
	if limit > 0 {
		// limit+1 would overflow at the largest limit, so the reader stops one byte above the largest limit below it.
		reader = io.LimitReader(file, min(limit, math.MaxInt64-1)+1)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("rpc registry file %s: %w", path, err)
	}
	if limit > 0 && int64(len(data)) > limit {
		return nil, fmt.Errorf("rpc registry file %s is over the poll limit of %d bytes, keeping the current registry", path, limit)
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
