//go:build timestamppb
// +build timestamppb

package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTimestamppb(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("Timestamppb"), 30*time.Second)
	defer cancel()
	const qry = `SELECT SYSDATE, :1, (SYSDATE-:2)*24*3600 FROM DUAL`
	it1, it2 := timestamppb.Now(), timestamppb.New(time.Now().Add(-10*time.Minute))
	var ot1, ot2 sql.NullTime
	var on3 sql.NullInt64
	if err := testDb.QueryRowContext(ctx, qry, it1, it2).Scan(&ot1, &ot2, &on3); err != nil {
		t.Fatalf("%s: %v", qry, err)
	}
	t.Logf("SYSDATE=%v 1=%v 2=%v", ot1, ot2, on3)
	if !(ot2.Valid && !ot2.Time.IsZero()) {
		t.Errorf("ot2=%v", ot2)
	}
}
