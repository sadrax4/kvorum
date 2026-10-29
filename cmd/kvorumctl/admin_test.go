package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStatusCommand(t *testing.T) {
	t.Parallel()
	res := testRun(t, newFakeAPI(), "status")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	want := "id              1\nstate           leader\nterm            3\nleader          1\ncommit_index    10\napplied_index   10\nlast_index      11\nsnapshot_index  5\nversion         v1.2.3\n\n" +
		"ID  RAFT_ADDRESS   CLIENT_ADDRESS  ROLE     LEADER\n1   10.0.0.1:2380  10.0.0.1:2379   voter    *\n2   10.0.0.2:2380  10.0.0.2:2379   learner  \n"
	if res.stdout != want {
		t.Fatalf("stdout:\n%q\nwant:\n%q", res.stdout, want)
	}

	res = testRun(t, newFakeAPI(), "--json", "status")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	var got struct {
		State   string       `json:"state"`
		Leader  uint64       `json:"leader"`
		Members []memberJSON `json:"members"`
		Header  headerJSON   `json:"header"`
	}
	if err := json.Unmarshal([]byte(res.stdout), &got); err != nil {
		t.Fatalf("invalid json %q: %v", res.stdout, err)
	}
	if got.State != "leader" || got.Leader != 1 || len(got.Members) != 2 || !got.Members[1].Learner || got.Header.Term != 3 {
		t.Fatalf("unexpected json %+v", got)
	}
}

func TestMembersCommand(t *testing.T) {
	t.Parallel()
	res := testRun(t, newFakeAPI(), "members")
	if res.code != exitOK {
		t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
	}
	lines := strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "ID  RAFT_ADDRESS") {
		t.Fatalf("unexpected table %q", res.stdout)
	}
	if !strings.HasSuffix(lines[1], "voter    *") || !strings.Contains(lines[2], "learner") {
		t.Fatalf("leader marker or role missing: %q", res.stdout)
	}

	res = testRun(t, newFakeAPI(), "--json", "members")
	var got struct {
		Members []memberJSON `json:"members"`
		Leader  uint64       `json:"leader"`
	}
	if err := json.Unmarshal([]byte(res.stdout), &got); err != nil || got.Leader != 1 || len(got.Members) != 2 {
		t.Fatalf("unexpected json %q (%v)", res.stdout, err)
	}
}

func TestMemberSubcommands(t *testing.T) {
	t.Parallel()
	t.Run("add learner with client addr", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		res := testRun(t, api, "member", "add", "3", "10.0.0.3:2380", "10.0.0.3:2379", "--learner")
		if res.code != exitOK {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		m := api.lastMember
		if m.ID != 3 || m.RaftAddress != "10.0.0.3:2380" || m.ClientAddress != "10.0.0.3:2379" || !m.Learner {
			t.Fatalf("AddMember called with %+v", m)
		}
		if !strings.Contains(res.stdout, "3   10.0.0.3:2380  10.0.0.3:2379   learner") {
			t.Fatalf("unexpected stdout %q", res.stdout)
		}
	})
	t.Run("add voter without client addr", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		res := testRun(t, api, "member", "add", "4", "10.0.0.4:2380")
		if res.code != exitOK {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
		if api.lastMember.Learner || api.lastMember.ClientAddress != "" || api.lastMember.ID != 4 {
			t.Fatalf("AddMember called with %+v", api.lastMember)
		}
	})
	t.Run("add too many args", func(t *testing.T) {
		t.Parallel()
		res := testRun(t, newFakeAPI(), "member", "add", "4", "a", "b", "c")
		if res.code != exitUsage || !strings.Contains(res.stderr, `unexpected argument "c"`) {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
	})
	t.Run("remove", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		res := testRun(t, api, "--json", "member", "remove", "2")
		if res.code != exitOK || api.lastID != 2 {
			t.Fatalf("code=%d id=%d stderr=%q", res.code, api.lastID, res.stderr)
		}
		var got struct {
			Members []memberJSON `json:"members"`
		}
		if err := json.Unmarshal([]byte(res.stdout), &got); err != nil || len(got.Members) != 1 {
			t.Fatalf("unexpected json %q (%v)", res.stdout, err)
		}
	})
	t.Run("promote", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		res := testRun(t, api, "member", "promote", "2")
		if res.code != exitOK || api.lastID != 2 {
			t.Fatalf("code=%d id=%d stderr=%q", res.code, api.lastID, res.stderr)
		}
		if strings.Contains(res.stdout, "learner") {
			t.Fatalf("member 2 should be a voter after promotion: %q", res.stdout)
		}
	})
	t.Run("help", func(t *testing.T) {
		t.Parallel()
		res := testRun(t, newFakeAPI(), "member", "-h")
		if res.code != exitOK || !strings.Contains(res.stdout, "--learner") {
			t.Fatalf("code=%d stdout=%q", res.code, res.stdout)
		}
	})
	t.Run("error", func(t *testing.T) {
		t.Parallel()
		api := newFakeAPI()
		api.err = errBoom
		res := testRun(t, api, "member", "promote", "2")
		if res.code != exitFailure || !strings.Contains(res.stderr, "boom") {
			t.Fatalf("code=%d stderr=%q", res.code, res.stderr)
		}
	})
}

func TestTransferLeaderAndSnapshot(t *testing.T) {
	t.Parallel()
	api := newFakeAPI()
	res := testRun(t, api, "transfer-leader", "2")
	if res.code != exitOK || api.lastID != 2 || res.stdout != "leadership transfer to 2 requested\n" {
		t.Fatalf("code=%d id=%d stdout=%q stderr=%q", res.code, api.lastID, res.stdout, res.stderr)
	}
	res = testRun(t, api, "--json", "transfer-leader", "2")
	if res.code != exitOK || !strings.Contains(res.stdout, `"target": 2`) {
		t.Fatalf("code=%d stdout=%q", res.code, res.stdout)
	}

	res = testRun(t, api, "snapshot")
	if res.code != exitOK || res.stdout != "snapshot_index  42\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q", res.code, res.stdout, res.stderr)
	}
	res = testRun(t, api, "--json", "snapshot")
	var got struct {
		SnapshotIndex uint64 `json:"snapshot_index"`
	}
	if err := json.Unmarshal([]byte(res.stdout), &got); err != nil || got.SnapshotIndex != 42 {
		t.Fatalf("unexpected json %q (%v)", res.stdout, err)
	}
}

func TestDisplay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   []byte
		want string
	}{
		{in: []byte("plain"), want: "plain"},
		{in: []byte("with space\tand tab"), want: "with space\tand tab"},
		{in: []byte("héllo"), want: "héllo"},
		{in: []byte{}, want: ""},
		{in: []byte("line\nbreak"), want: `"line\nbreak"`},
		{in: []byte{0xff, 0x00}, want: `"\xff\x00"`},
	}
	for _, tc := range tests {
		if got := display(tc.in); got != tc.want {
			t.Errorf("display(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
