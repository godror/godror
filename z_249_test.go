package godror_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

func TestOpenCloseCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("OpenCloseCrash"), 10*time.Second)
	defer cancel()

	godror.SlowdownInitTZ(true)
	c1, e1 := sql.Open("godror", testConStr)
	t.Logf("e1: %v", e1)
	if c1 != nil {
		defer c1.Close()
	}
	c2, e2 := sql.Open("godror", testConStr)
	t.Logf("e2: %v", e2)
	if c2 != nil {
		defer c2.Close()
	}

	join := make(chan struct{})
	waitForSch := make(chan struct{})
	var token struct{}
	go func() {
		defer c1.Close()
		waitForSch <- token
		e1 := c1.PingContext(ctx)
		t.Logf("conn1 done: %v", e1)
		join <- token
	}()
	go func() {
		defer c2.Close()
		<-waitForSch
		e2 := c2.PingContext(ctx)
		t.Logf("conn2 done: %v", e2)
		join <- token
	}()
	<-join
	<-join
}
