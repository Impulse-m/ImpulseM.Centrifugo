package redisshard

// ShardIndex returns the index of the shard of key among numShards shards. The same key maps to the same
// index for the same number of shards, and growing numShards by one moves only about 1/numShards of the keys.
func ShardIndex(key string, numShards int) int {
	if numShards <= 1 {
		return 0
	}
	return int(jumpConsistentHash(fnv1a64(key), int32(numShards)))
}


// jumpConsistentHash maps key to one of numBuckets buckets (Lamping and Veach). Growing numBuckets by one
// moves only about 1/numBuckets of the keys, and only to the new bucket.
func jumpConsistentHash(key uint64, numBuckets int32) int32 {
	var b int64 = -1
	var j int64
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int32(b)
}


// fnv1a64 returns FNV-1a hash of s. It does not allocate.
func fnv1a64(s string) uint64 {
	const offset64 uint64 = 14695981039346656037
	const prime64 uint64 = 1099511628211
	hash := offset64
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}
	return hash
}
