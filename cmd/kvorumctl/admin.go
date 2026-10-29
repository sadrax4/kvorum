package main

import (
	"context"
	"io"

	"github.com/sadrax4/kvorum/client"
)

func cmdStatus(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	pos, err := parseInterleaved(newFlagSet("status"), args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos); err != nil {
		return err
	}
	st, err := api.Status(ctx)
	if err != nil {
		return err
	}
	if g.json {
		return printJSON(out, struct {
			Header        headerJSON   `json:"header"`
			ID            uint64       `json:"id"`
			State         string       `json:"state"`
			Term          uint64       `json:"term"`
			Leader        uint64       `json:"leader"`
			CommitIndex   uint64       `json:"commit_index"`
			AppliedIndex  uint64       `json:"applied_index"`
			LastIndex     uint64       `json:"last_index"`
			SnapshotIndex uint64       `json:"snapshot_index"`
			Members       []memberJSON `json:"members"`
			Version       string       `json:"version"`
		}{
			Header: headerView(st.Header), ID: st.ID, State: st.State, Term: st.Term, Leader: st.Leader,
			CommitIndex: st.CommitIndex, AppliedIndex: st.AppliedIndex, LastIndex: st.LastIndex,
			SnapshotIndex: st.SnapshotIndex, Members: memberViews(st.Members), Version: st.Version,
		})
	}
	err = printFields(out, [][2]string{
		{"id", u64(st.ID)},
		{"state", st.State},
		{"term", u64(st.Term)},
		{"leader", u64(st.Leader)},
		{"commit_index", u64(st.CommitIndex)},
		{"applied_index", u64(st.AppliedIndex)},
		{"last_index", u64(st.LastIndex)},
		{"snapshot_index", u64(st.SnapshotIndex)},
		{"version", st.Version},
	})
	if err != nil {
		return err
	}
	if len(st.Members) > 0 {
		if err := writef(out, "\n"); err != nil {
			return err
		}
		return printMembers(out, st.Members, st.Leader)
	}
	return nil
}

func cmdMembers(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	pos, err := parseInterleaved(newFlagSet("members"), args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos); err != nil {
		return err
	}
	members, leader, err := api.Members(ctx)
	if err != nil {
		return err
	}
	return printMembership(out, g.json, members, leader)
}

func printMembership(out io.Writer, asJSON bool, members []client.Member, leader uint64) error {
	if asJSON {
		return printJSON(out, struct {
			Members []memberJSON `json:"members"`
			Leader  uint64       `json:"leader"`
		}{Members: memberViews(members), Leader: leader})
	}
	return printMembers(out, members, leader)
}

func cmdMember(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	if len(args) == 0 {
		return usageErrorf("missing member subcommand: add, remove or promote")
	}
	switch args[0] {
	case "add":
		return cmdMemberAdd(ctx, api, g, args[1:], out)
	case "remove":
		return cmdMemberRemove(ctx, api, g, args[1:], out)
	case "promote":
		return cmdMemberPromote(ctx, api, g, args[1:], out)
	case "-h", "--help", "help":
		return errHelpRequested
	default:
		return usageErrorf("unknown member subcommand %q", args[0])
	}
}

func cmdMemberAdd(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	fs := newFlagSet("member add")
	learner := fs.Bool("learner", false, "add as learner")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) < 2 {
		return usageErrorf("member add needs <id> and <raft-addr>")
	}
	if len(pos) > 3 {
		return usageErrorf("unexpected argument %q", pos[3])
	}
	id, err := parseID(pos[0])
	if err != nil {
		return err
	}
	m := client.Member{ID: id, RaftAddress: pos[1], Learner: *learner}
	if len(pos) == 3 {
		m.ClientAddress = pos[2]
	}
	members, err := api.AddMember(ctx, m)
	if err != nil {
		return err
	}
	return printMembership(out, g.json, members, 0)
}

func cmdMemberRemove(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	pos, err := parseInterleaved(newFlagSet("member remove"), args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos, "id"); err != nil {
		return err
	}
	id, err := parseID(pos[0])
	if err != nil {
		return err
	}
	members, err := api.RemoveMember(ctx, id)
	if err != nil {
		return err
	}
	return printMembership(out, g.json, members, 0)
}

func cmdMemberPromote(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	pos, err := parseInterleaved(newFlagSet("member promote"), args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos, "id"); err != nil {
		return err
	}
	id, err := parseID(pos[0])
	if err != nil {
		return err
	}
	members, err := api.PromoteMember(ctx, id)
	if err != nil {
		return err
	}
	return printMembership(out, g.json, members, 0)
}

func cmdTransferLeader(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	pos, err := parseInterleaved(newFlagSet("transfer-leader"), args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos, "id"); err != nil {
		return err
	}
	id, err := parseID(pos[0])
	if err != nil {
		return err
	}
	if err := api.TransferLeadership(ctx, id); err != nil {
		return err
	}
	if g.json {
		return printJSON(out, struct {
			Target uint64 `json:"target"`
		}{Target: id})
	}
	return writef(out, "leadership transfer to %d requested\n", id)
}

func cmdSnapshot(ctx context.Context, api kvorumAPI, g globals, args []string, out io.Writer) error {
	pos, err := parseInterleaved(newFlagSet("snapshot"), args)
	if err != nil {
		return err
	}
	if err := wantArgs(pos); err != nil {
		return err
	}
	index, err := api.Snapshot(ctx)
	if err != nil {
		return err
	}
	if g.json {
		return printJSON(out, struct {
			SnapshotIndex uint64 `json:"snapshot_index"`
		}{SnapshotIndex: index})
	}
	return printFields(out, [][2]string{{"snapshot_index", u64(index)}})
}
