package transport

import "errors"

var (
	// ErrInvalidConfig is returned by [New] when the [Config] is malformed.
	ErrInvalidConfig = errors.New("transport: invalid config")

	// ErrInvalidMessage is returned by [FromProto] when a wire message cannot
	// be decoded into a valid [raft.Message].
	ErrInvalidMessage = errors.New("transport: invalid message")

	// ErrAlreadyStarted is returned by [Transport.Start] when the transport is
	// already serving.
	ErrAlreadyStarted = errors.New("transport: already started")

	// ErrStopped is returned when an operation is attempted on a transport
	// that has been stopped.
	ErrStopped = errors.New("transport: stopped")
)
