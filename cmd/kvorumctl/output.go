package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"unicode"
	"unicode/utf8"

	"github.com/sadrax4/kvorum/client"
)

// printJSON writes v as indented JSON followed by a newline.
func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	return nil
}

// printFields writes name/value pairs as two aligned columns.
func printFields(w io.Writer, fields [][2]string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, f := range fields {
		if _, err := fmt.Fprintf(tw, "%s\t%s\n", f[0], f[1]); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// printTable writes a header row followed by rows, columns aligned.
func printTable(w io.Writer, header []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if err := writeRow(tw, header); err != nil {
		return err
	}
	for _, r := range rows {
		if err := writeRow(tw, r); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeRow(w io.Writer, cells []string) error {
	_, err := fmt.Fprintln(w, strings.Join(cells, "\t"))
	return err
}

// writef is fmt.Fprintf returning only the error.
func writef(w io.Writer, format string, args ...any) error {
	_, err := fmt.Fprintf(w, format, args...)
	return err
}

// display renders bytes for humans: as-is when they are printable text,
// quoted with escapes otherwise.
func display(b []byte) string {
	if utf8.Valid(b) && isPrintable(b) {
		return string(b)
	}
	return strconv.Quote(string(b))
}

func isPrintable(b []byte) bool {
	for _, r := range string(b) {
		if r == '\t' || r == ' ' {
			continue
		}
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func u64(v uint64) string { return strconv.FormatUint(v, 10) }

// JSON views. Keys and values are rendered as strings rather than base64 so
// the output stays readable; invalid UTF-8 is replaced by the encoder.

type headerJSON struct {
	MemberID uint64 `json:"member_id"`
	Term     uint64 `json:"term"`
	Revision uint64 `json:"revision"`
}

type kvJSON struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	Version        uint64 `json:"version"`
	CreateRevision uint64 `json:"create_revision"`
	ModRevision    uint64 `json:"mod_revision"`
}

type memberJSON struct {
	ID            uint64 `json:"id"`
	RaftAddress   string `json:"raft_address"`
	ClientAddress string `json:"client_address"`
	Learner       bool   `json:"learner"`
}

func headerView(h client.Header) headerJSON {
	return headerJSON{MemberID: h.MemberID, Term: h.Term, Revision: h.Revision}
}

func kvView(kv *client.KeyValue) *kvJSON {
	if kv == nil {
		return nil
	}
	return &kvJSON{
		Key:            string(kv.Key),
		Value:          string(kv.Value),
		Version:        kv.Version,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
	}
}

func kvFields(prefix string, kv *client.KeyValue) [][2]string {
	if kv == nil {
		return nil
	}
	return [][2]string{
		{prefix + "key", display(kv.Key)},
		{prefix + "value", display(kv.Value)},
		{prefix + "version", u64(kv.Version)},
		{prefix + "create_revision", u64(kv.CreateRevision)},
		{prefix + "mod_revision", u64(kv.ModRevision)},
	}
}

func memberViews(ms []client.Member) []memberJSON {
	out := make([]memberJSON, 0, len(ms))
	for _, m := range ms {
		out = append(out, memberJSON{ID: m.ID, RaftAddress: m.RaftAddress, ClientAddress: m.ClientAddress, Learner: m.Learner})
	}
	return out
}

// printMembers renders the membership table; leaderID marks the leader.
func printMembers(w io.Writer, members []client.Member, leaderID uint64) error {
	rows := make([][]string, 0, len(members))
	for _, m := range members {
		role := "voter"
		if m.Learner {
			role = "learner"
		}
		leader := ""
		if m.ID == leaderID && leaderID != 0 {
			leader = "*"
		}
		rows = append(rows, []string{u64(m.ID), m.RaftAddress, m.ClientAddress, role, leader})
	}
	return printTable(w, []string{"ID", "RAFT_ADDRESS", "CLIENT_ADDRESS", "ROLE", "LEADER"}, rows)
}
