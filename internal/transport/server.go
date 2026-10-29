package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/sadrax4/kvorum/api/raftpb"
	"github.com/sadrax4/kvorum/raft"
)

const (
	// serverKeepaliveTime is the idle period after which the server pings a
	// client.
	serverKeepaliveTime = 30 * time.Second
	// serverKeepaliveTimeout is how long the server waits for a ping reply
	// before closing the connection.
	serverKeepaliveTimeout = 10 * time.Second
	// keepaliveMinTime is the shortest client ping interval the server
	// tolerates; faster pingers are disconnected.
	keepaliveMinTime = 5 * time.Second
)

// raftServer implements raftpb.RaftServiceServer on behalf of a Transport.
type raftServer struct {
	raftpb.UnimplementedRaftServiceServer
	t *Transport
}

func newGRPCServer(cfg Config) *grpc.Server {
	return grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.MaxMessageSize),
		grpc.MaxSendMsgSize(cfg.MaxMessageSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    serverKeepaliveTime,
			Timeout: serverKeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             keepaliveMinTime,
			PermitWithoutStream: true,
		}),
	)
}

// Step implements raftpb.RaftServiceServer.
func (s *raftServer) Step(ctx context.Context, req *raftpb.StepRequest) (*raftpb.StepResponse, error) {
	pm := req.GetMessage()
	if err := s.checkEnvelope(pm); err != nil {
		return nil, err
	}
	if pm.GetType() == uint32(raft.MsgSnap) {
		return nil, status.Error(codes.InvalidArgument, "MsgSnap must use InstallSnapshot")
	}
	m, err := FromProto(pm)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.t.metrics.MessageReceived(m.Type, proto.Size(req))
	if err := s.deliver(ctx, m); err != nil {
		return nil, err
	}
	return &raftpb.StepResponse{}, nil
}

// InstallSnapshot implements raftpb.RaftServiceServer. It expects a header
// chunk followed by zero or more data chunks, reassembles the payload within
// MaxSnapshotSize, and delivers the resulting MsgSnap to the handler.
func (s *raftServer) InstallSnapshot(stream raftpb.RaftService_InstallSnapshotServer) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "empty snapshot stream")
		}
		return err
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first snapshot chunk must be the header")
	}
	if err := s.checkEnvelope(header); err != nil {
		return err
	}
	if header.GetType() != uint32(raft.MsgSnap) {
		return status.Errorf(codes.InvalidArgument, "snapshot header has type %s", raft.MessageType(header.GetType()))
	}
	m, err := FromProto(header)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	data, err := s.receiveData(stream)
	if err != nil {
		return err
	}
	m.Snapshot.Data = data
	s.t.metrics.MessageReceived(raft.MsgSnap, proto.Size(first)+len(data))
	if err := s.deliver(stream.Context(), m); err != nil {
		return err
	}
	return stream.SendAndClose(&raftpb.InstallSnapshotResponse{})
}

// receiveData reads data chunks until the client closes the stream.
func (s *raftServer) receiveData(stream raftpb.RaftService_InstallSnapshotServer) ([]byte, error) {
	limit := s.t.cfg.MaxSnapshotSize
	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return buf.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
		payload, ok := chunk.GetPayload().(*raftpb.InstallSnapshotRequest_Data)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "snapshot stream carries a second header")
		}
		if int64(buf.Len())+int64(len(payload.Data)) > limit {
			return nil, status.Errorf(codes.ResourceExhausted, "snapshot exceeds %d bytes", limit)
		}
		buf.Write(payload.Data)
	}
}

// checkEnvelope validates the addressing of an inbound message.
func (s *raftServer) checkEnvelope(pm *raftpb.Message) error {
	if pm == nil {
		return status.Error(codes.InvalidArgument, "missing message")
	}
	if to := raft.NodeID(pm.GetTo()); to != s.t.cfg.LocalID {
		return status.Errorf(codes.InvalidArgument, "message addressed to %s, this node is %s", to, s.t.cfg.LocalID)
	}
	if pm.GetFrom() == uint64(raft.None) {
		return status.Error(codes.InvalidArgument, "message has no sender")
	}
	return nil
}

// deliver hands m to the handler under a context that is cancelled by either
// the request or the transport stopping, and maps handler errors to
// codes.Internal.
func (s *raftServer) deliver(ctx context.Context, m raft.Message) error {
	ctx, cancel := s.t.requestContext(ctx)
	defer cancel()
	if err := s.t.handler.Step(ctx, m); err != nil {
		s.t.logger.Warn("handler rejected message",
			slog.String("type", m.Type.String()),
			slog.Uint64("from", uint64(m.From)),
			slog.Any("error", err))
		return status.Errorf(codes.Internal, "step %s: %v", m.Type, err)
	}
	return nil
}
