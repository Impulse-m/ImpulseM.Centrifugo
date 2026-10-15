package accessrevoke

import (
	"strconv"
	"testing"

	"github.com/centrifugal/centrifuge"
)

// identifiedRecord is an Identified ext state for the benchmark: the record of a connection which holds its own identity.
type identifiedRecord struct {
	userID   string
	identity centrifuge.TokenIdentity
}

// AccessIdentity implements Identified.
func (r *identifiedRecord) AccessIdentity() (string, centrifuge.TokenIdentity) {
	return r.userID, r.identity
}

// BenchmarkSweepConnections sweeps a hub of 10,000 real connections, each with a record. Apart from the snapshot of the
// hub, a sweep allocates nothing: the identity is read from the record without a lock or a closure.
func BenchmarkSweepConnections(b *testing.B) {
	const connections = 10000
	node := newRevokeTestNode(b)
	for i := 0; i < connections; i++ {
		connectTestClient(b, node, "user-"+strconv.Itoa(i)+"|"+testUUID+"|0|100")
	}
	for _, client := range node.Hub().Connections() {
		client.SetExtState(&identifiedRecord{userID: client.UserID(), identity: client.TokenIdentity()})
	}
	store := NewStore(0)
	queue := NewDisconnectQueue(connections, 100)
	options := CheckOptions{Block: true, Session: true, UserTokens: true}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rejected := SweepConnections(node.Hub(), store, queue, options); rejected != 0 {
			b.Fatalf("rejected %d connections of an empty store", rejected)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*connections), "ns/conn")
}
