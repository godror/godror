package sid_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/godror/godror/sid"
)

func TestListOptionsRoundTrip(t *testing.T) {
	const x = `(DESCRIPTION=(LOAD_BALANCE=on)(SOURCE_ROUTE=yes)(ADDRESS=(PROTOCOL=tcp)(HOST=host1)(PORT=1521)))`
	s, err := sid.ParseConnDescription(x)
	if err != nil {
		t.Fatal(err)
	}
	var d sid.Description
	if err = d.Parse([]sid.Statement{s}); err != nil {
		t.Fatal(err)
	}
	if want := (sid.ListOptions{LoadBalance: true, SourceRoute: true}); d.Options != want {
		t.Errorf("parsed options: got %+v, wanted %+v", d.Options, want)
	}

	want := sid.Description{
		Options: sid.ListOptions{Failover: true, LoadBalance: true, SourceRoute: true},
		Addresses: []sid.Address{
			{Protocol: "tcp", Host: "host1", Port: 1521},
			{Protocol: "tcp", Host: "host2", Port: 1522, BufSizes: sid.BufSizes{RecvBufSize: 1024}},
		},
	}
	var buf strings.Builder
	want.Print(&buf, "", "")
	printed := buf.String()
	t.Log(printed)
	for _, k := range []string{"(FAILOVER=on)", "(LOAD_BALANCE=on)", "(SOURCE_ROUTE=on)"} {
		if !strings.Contains(printed, k) {
			t.Errorf("printed %q lacks %q", printed, k)
		}
	}
	if s, err = sid.ParseConnDescription(printed); err != nil {
		t.Fatal(err)
	}
	var got sid.Description
	if err = got.Parse([]sid.Statement{s}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip: got %+v, wanted %+v", got, want)
	}
}

func TestBufSizesIsZero(t *testing.T) {
	for _, tc := range []struct {
		bs   sid.BufSizes
		want bool
	}{
		{sid.BufSizes{}, true},
		{sid.BufSizes{RecvBufSize: 1}, false},
		{sid.BufSizes{SendBufSize: 1}, false},
		{sid.BufSizes{RecvBufSize: 1, SendBufSize: 1}, false},
	} {
		if got := tc.bs.IsZero(); got != tc.want {
			t.Errorf("%+v.IsZero()=%t, wanted %t", tc.bs, got, tc.want)
		}
	}
	var buf strings.Builder
	sid.Address{}.Print(&buf, "", "")
	if buf.Len() != 0 {
		t.Errorf("empty Address printed %q", buf.String())
	}
}
