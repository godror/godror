package sid_test

import (
	"strings"
	"testing"
	"unicode"

	"gopkg.in/goracle.v2/sid"
)

func TestParseDescription(t *testing.T) {
	const x = `(DESCRIPTION=
   (SOURCE_ROUTE=yes)
   (ADDRESS=(PROTOCOL=tcp)(HOST=host1)(PORT=1630))
   (ADDRESS_LIST=
     (FAILOVER=on)
     (LOAD_BALANCE=off)
     (ADDRESS=(PROTOCOL=tcp)(HOST=host2a)(PORT=1630))
     (ADDRESS=(PROTOCOL=tcp)(HOST=host2b)(PORT=1630)))
   (ADDRESS=(PROTOCOL=tcp)(HOST=host3)(PORT=1521))
   (CONNECT_DATA=(SERVICE_NAME=Sales.us.example.com)))`
	s, err := sid.ParseConnDescription(x)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", s)
	t.Log(s)
	strip := func(s string) string {
		return strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return -1
			}
			return r
		}, s)
	}
	if want, got := strip(x), strip(s.String()); want != got {
		t.Errorf("got %q, wanted %q", got, want)
	}

	var d sid.Description
	if err = d.Parse([]sid.Statement{s}); err != nil {
		t.Error(err)
	}
	var buf strings.Builder
	d.Print(&buf, "", "")
	t.Log(buf.String())
}
