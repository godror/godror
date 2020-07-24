package sid_test

import (
	"strings"
	"testing"
	"unicode"

	"github.com/godror/godror/sid"
)

func TestParseDescription(t *testing.T) {
	const x = `(DESCRIPTION=
   (ENABLE=broken)
   (RECV_BUF_SIZE=32)
   (SDU=42)
   (SEND_BUF_SIZE=1000)
   (TYPE_OF_SERVICE=oracle11_database)
   (LOAD_BALANCE=on)
   (FAILOVER=on)
   (SOURCE_ROUTE=yes)
   (ADDRESS=(PROTOCOL=tcp)(HOST=host1)(PORT=1630))
   (ADDRESS_LIST=
     (SOURCE_ROUTE=yes)
	 (ADDRESS=(PROTOCOL=tcp)(HOST=host5)(PORT=1630)(RECV_BUF_SIZE=11784))
	 (ADDRESS=(PROTOCOL=tcp)(HOST=host6)(PORT=1521)))
   (ADDRESS_LIST=
     (FAILOVER=on)
     (LOAD_BALANCE=off)
	 (SOURCE_ROUTE=yes)
     (ADDRESS=(PROTOCOL=tcp)(HOST=host2a)(PORT=1630))
     (ADDRESS=(PROTOCOL=tcp)(HOST=host2b)(PORT=1630)))
   (ADDRESS=(PROTOCOL=tcp)(HOST=host3)(PORT=1521))
   (HTTP_PROXY=myproxy)(HTTPS_PROXY_PORT=3333)
   (COMPRESSION=on)(COMPRESSION_LEVELS=2)
   (CONNECT_DATA=(SERVICE_NAME=Sales.us.example.com)(SERVER=dedicated)
     (COLOCATION_TAG=abc)
     (SECURITY=(SSL_SERVER_CERT_DN=cn=sales,cn=OracleContext"))
   )
   (CONNECT_TIMEOUT=10ms)(RETRY_COUNT=3)(RETRY_DELAY=2)(TRANSPORT_CONNECT_TIMEOUT=300 ms)
 )`
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
