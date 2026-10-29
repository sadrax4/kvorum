package client

import "sync"

// endpointRing is the ordered set of known endpoints together with the
// preferred one. The preferred endpoint starts as the first configured one
// and moves to the last known leader as hints are followed.
type endpointRing struct {
	mu        sync.Mutex
	addrs     []string
	preferred int
}

func newEndpointRing(addrs []string) *endpointRing {
	return &endpointRing{addrs: addrs}
}

// current returns the preferred endpoint.
func (r *endpointRing) current() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addrs[r.preferred]
}

// prefer records addr as the preferred endpoint, adding it when unknown.
func (r *endpointRing) prefer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.preferred = r.indexOfLocked(addr)
}

// next returns the endpoint after addr in ring order, so that repeated calls
// cycle through every known endpoint.
func (r *endpointRing) next(addr string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.indexOfLocked(addr)
	return r.addrs[(i+1)%len(r.addrs)]
}

// all returns a snapshot of the known endpoints.
func (r *endpointRing) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.addrs))
	copy(out, r.addrs)
	return out
}

func (r *endpointRing) indexOfLocked(addr string) int {
	for i, a := range r.addrs {
		if a == addr {
			return i
		}
	}
	r.addrs = append(r.addrs, addr)
	return len(r.addrs) - 1
}
