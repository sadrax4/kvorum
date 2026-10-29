package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sadrax4/kvorum/api/raftpb"
	"github.com/sadrax4/kvorum/raft"
)

func TestStepRejectsBadEnvelopes(t *testing.T) {
	t.Parallel()
	b := startNode(t, 2, nil)
	b.handler.stepErr = errors.New("state machine refused")
	client := rawClient(t, b.tr.Address())

	tests := []struct {
		name string
		msg  *raftpb.Message
		code codes.Code
	}{
		{name: "wrong to", msg: ToProto(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 99}), code: codes.InvalidArgument},
		{name: "missing message", msg: nil, code: codes.InvalidArgument},
		{name: "zero from", msg: ToProto(raft.Message{Type: raft.MsgHeartbeat, From: 0, To: 2}), code: codes.InvalidArgument},
		{name: "bad type", msg: &raftpb.Message{Type: 200, From: 1, To: 2}, code: codes.InvalidArgument},
		{name: "snapshot via step", msg: &raftpb.Message{Type: uint32(raft.MsgSnap), From: 1, To: 2, SnapshotMeta: &raftpb.SnapshotMeta{Index: 1}}, code: codes.InvalidArgument},
		{name: "handler error", msg: ToProto(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2}), code: codes.Internal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := client.Step(ctx, &raftpb.StepRequest{Message: tc.msg})
			if got := status.Code(err); got != tc.code {
				t.Fatalf("Step code = %v (%v), want %v", got, err, tc.code)
			}
		})
	}
}

func TestStepCountsOnlyDecodedMessages(t *testing.T) {
	t.Parallel()
	b := startNode(t, 2, nil)
	client := rawClient(t, b.tr.Address())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Step(ctx, &raftpb.StepRequest{Message: ToProto(raft.Message{Type: raft.MsgVote, From: 1, To: 99})}); err == nil {
		t.Fatal("misaddressed message accepted")
	}
	if b.metrics.received.Load() != 0 || len(b.handler.messages()) != 0 {
		t.Fatal("rejected message was counted or delivered")
	}
	if _, err := client.Step(ctx, &raftpb.StepRequest{Message: ToProto(raft.Message{Type: raft.MsgVote, From: 1, To: 2})}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if b.metrics.received.Load() != 1 || len(b.handler.messages()) != 1 {
		t.Fatal("accepted message was not counted and delivered")
	}
}

// installSnapshot sends chunks and returns the final status code.
func installSnapshot(t *testing.T, client raftpb.RaftServiceClient, chunks ...*raftpb.InstallSnapshotRequest) codes.Code {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.InstallSnapshot(ctx)
	if err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	for _, c := range chunks {
		if err := stream.Send(c); err != nil {
			break
		}
	}
	_, err = stream.CloseAndRecv()
	return status.Code(err)
}

func headerChunk(m raft.Message) *raftpb.InstallSnapshotRequest {
	return &raftpb.InstallSnapshotRequest{Payload: &raftpb.InstallSnapshotRequest_Header{Header: ToProto(m)}}
}

func dataChunk(b []byte) *raftpb.InstallSnapshotRequest {
	return &raftpb.InstallSnapshotRequest{Payload: &raftpb.InstallSnapshotRequest_Data{Data: b}}
}

func TestInstallSnapshotProtocolErrors(t *testing.T) {
	t.Parallel()
	b := startNode(t, 2, func(c *Config) { c.MaxSnapshotSize = 16 })
	client := rawClient(t, b.tr.Address())
	good := snapshotMessage(nil)
	misaddressed := snapshotMessage(nil)
	misaddressed.To = 3
	notSnap := raft.Message{Type: raft.MsgApp, From: 1, To: 2}

	tests := []struct {
		name   string
		chunks []*raftpb.InstallSnapshotRequest
		code   codes.Code
	}{
		{name: "empty stream", chunks: nil, code: codes.InvalidArgument},
		{name: "data first", chunks: []*raftpb.InstallSnapshotRequest{dataChunk([]byte("x"))}, code: codes.InvalidArgument},
		{name: "empty chunk first", chunks: []*raftpb.InstallSnapshotRequest{{}}, code: codes.InvalidArgument},
		{name: "wrong to", chunks: []*raftpb.InstallSnapshotRequest{headerChunk(misaddressed)}, code: codes.InvalidArgument},
		{name: "header not a snapshot", chunks: []*raftpb.InstallSnapshotRequest{headerChunk(notSnap)}, code: codes.InvalidArgument},
		{name: "second header", chunks: []*raftpb.InstallSnapshotRequest{headerChunk(good), headerChunk(good)}, code: codes.InvalidArgument},
		{name: "too large", chunks: []*raftpb.InstallSnapshotRequest{headerChunk(good), dataChunk(make([]byte, 10)), dataChunk(make([]byte, 7))}, code: codes.ResourceExhausted},
		{name: "at limit", chunks: []*raftpb.InstallSnapshotRequest{headerChunk(good), dataChunk(make([]byte, 16))}, code: codes.OK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := installSnapshot(t, client, tc.chunks...); got != tc.code {
				t.Fatalf("code = %v, want %v", got, tc.code)
			}
		})
	}
}

func TestInstallSnapshotHandlerErrorIsInternal(t *testing.T) {
	t.Parallel()
	b := startNode(t, 2, nil)
	b.handler.stepErr = errors.New("restore failed")
	client := rawClient(t, b.tr.Address())
	if got := installSnapshot(t, client, headerChunk(snapshotMessage(nil)), dataChunk([]byte("abc"))); got != codes.Internal {
		t.Fatalf("code = %v, want Internal", got)
	}
	if b.metrics.received.Load() != 1 {
		t.Fatalf("received = %d, want 1", b.metrics.received.Load())
	}
}

func TestHandlerContextCancelledOnStop(t *testing.T) {
	t.Parallel()
	b := startNode(t, 2, nil)
	b.handler.block = make(chan struct{})
	b.handler.entered = make(chan struct{}, 1)
	client := rawClient(t, b.tr.Address())

	errc := make(chan error, 1)
	go func() {
		_, err := client.Step(context.Background(), &raftpb.StepRequest{
			Message: ToProto(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2}),
		})
		errc <- err
	}()
	select {
	case <-b.handler.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never entered")
	}
	start := time.Now()
	b.tr.Stop()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Stop took %v with a blocked handler", elapsed)
	}
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("blocked call succeeded after Stop")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client call did not return after Stop")
	}
}
