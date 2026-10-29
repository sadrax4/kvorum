package client

// ReadOption customises Get and Scan.
type ReadOption interface {
	applyRead(*readOptions)
}

// WriteOption customises Put and Delete.
type WriteOption interface {
	applyWrite(*writeOptions)
}

// WatchOption customises Watch.
type WatchOption interface {
	applyWatch(*watchOptions)
}

type readOptions struct {
	stale  bool
	limit  int
	prefix bool
}

type writeOptions struct {
	prevKV bool
}

type watchOptions struct {
	prefix   bool
	rangeEnd []byte
}

type readOptionFunc func(*readOptions)

func (f readOptionFunc) applyRead(o *readOptions) { f(o) }

type writeOptionFunc func(*writeOptions)

func (f writeOptionFunc) applyWrite(o *writeOptions) { f(o) }

type watchOptionFunc func(*watchOptions)

func (f watchOptionFunc) applyWatch(o *watchOptions) { f(o) }

// WithStale relaxes a read to READ_CONSISTENCY_STALE: the serving node answers
// from its local applied state without coordinating with the leader. Reads
// are linearizable by default.
func WithStale() ReadOption {
	return readOptionFunc(func(o *readOptions) { o.stale = true })
}

// WithLimit caps the number of keys a Scan returns. Values of zero or less
// mean no client-side limit (the server still applies its own maximum).
func WithLimit(n int) ReadOption {
	return readOptionFunc(func(o *readOptions) {
		if n < 0 {
			n = 0
		}
		o.limit = n
	})
}

// PrefixOption is the option returned by WithPrefix. It implements both
// ReadOption and WatchOption.
type PrefixOption struct{}

func (PrefixOption) applyRead(o *readOptions)   { o.prefix = true }
func (PrefixOption) applyWatch(o *watchOptions) { o.prefix = true }

// WithPrefix makes Scan or Watch cover every key that starts with the given
// start key: the end of the range becomes PrefixEnd(start). For Scan it
// overrides the explicit end argument.
func WithPrefix() PrefixOption {
	return PrefixOption{}
}

// WithRange makes Watch cover the half-open range [key, end). WithPrefix
// takes precedence when both are given.
func WithRange(end []byte) WatchOption {
	return watchOptionFunc(func(o *watchOptions) { o.rangeEnd = end })
}

// WithPrevKV asks the server to return the previous value of the key in the
// response of a Put or Delete.
func WithPrevKV() WriteOption {
	return writeOptionFunc(func(o *writeOptions) { o.prevKV = true })
}

func applyReadOptions(opts []ReadOption) readOptions {
	var o readOptions
	for _, opt := range opts {
		if opt != nil {
			opt.applyRead(&o)
		}
	}
	return o
}

func applyWriteOptions(opts []WriteOption) writeOptions {
	var o writeOptions
	for _, opt := range opts {
		if opt != nil {
			opt.applyWrite(&o)
		}
	}
	return o
}

func applyWatchOptions(opts []WatchOption) watchOptions {
	var o watchOptions
	for _, opt := range opts {
		if opt != nil {
			opt.applyWatch(&o)
		}
	}
	return o
}
