// MIT License
//
// Copyright (c) 2022-2026 GoAkt Team
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package actor_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/goakt/v3/actor"
	gerrors "github.com/tochemey/goakt/v3/errors"
	"github.com/tochemey/goakt/v3/log"
)

// replyEcho answers every StringValue with itself; a value ending in "slow"
// is answered after 3 ms.
type replyEcho struct{}

func (*replyEcho) PreStart(*actor.Context) error { return nil }
func (*replyEcho) PostStop(*actor.Context) error { return nil }
func (*replyEcho) Receive(ctx *actor.ReceiveContext) {
	if m, ok := ctx.Message().(*wrapperspb.StringValue); ok {
		if strings.HasSuffix(m.GetValue(), "slow") {
			time.Sleep(3 * time.Millisecond)
		}
		ctx.Response(m)
	}
}

// askStorm runs 16 goroutines x 400 Asks, each goroutine to its own echo
// actor, and returns the timeouts, the replies that did not match their
// request, and the other errors.
func askStorm(t *testing.T, ask func(ctx context.Context, sys actor.ActorSystem, to *actor.PID, msg string) (any, error),
	timeout func(i int) (string, bool)) (timeouts, wrong, others int64) {
	t.Helper()
	ctx := context.Background()
	sys, err := actor.NewActorSystem("askstorm", actor.WithLogger(log.DiscardLogger))
	if err != nil {
		t.Fatal(err)
	}
	if err := sys.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sys.Stop(ctx) }()
	const goroutines, asks = 16, 400
	pids := make([]*actor.PID, goroutines)
	for g := range pids {
		if pids[g], err = sys.Spawn(ctx, fmt.Sprintf("echo-%d", g), &replyEcho{}); err != nil {
			t.Fatal(err)
		}
	}
	var to, wr, ot atomic.Int64
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range asks {
				suffix, late := timeout(i)
				want := fmt.Sprintf("%d-%d%s", g, i, suffix)
				resp, err := ask(ctx, sys, pids[g], want)
				switch {
				case errors.Is(err, gerrors.ErrRequestTimeout):
					if !late {
						to.Add(1)
					}
				case err != nil:
					ot.Add(1)
				case resp.(*wrapperspb.StringValue).GetValue() != want:
					wr.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return to.Load(), wr.Load(), ot.Load()
}

// TestAskReplyNotDropped: an Ask used to write responseClosed on its
// ReceiveContext after the receiver had returned that context to the pool,
// so a concurrent Ask that reused it lost its reply and timed out. The
// echo actors answer at once: every timeout is a dropped reply.
func TestAskReplyNotDropped(t *testing.T) {
	asks := map[string]func(context.Context, actor.ActorSystem, *actor.PID, string) (any, error){
		"actor.Ask": func(ctx context.Context, _ actor.ActorSystem, to *actor.PID, msg string) (any, error) {
			return actor.Ask(ctx, to, wrapperspb.String(msg), 500*time.Millisecond)
		},
		"PID.Ask": func(ctx context.Context, sys actor.ActorSystem, to *actor.PID, msg string) (any, error) {
			return sys.NoSender().Ask(ctx, to, wrapperspb.String(msg), 500*time.Millisecond)
		},
	}
	for name, ask := range asks {
		t.Run(name, func(t *testing.T) {
			timeouts, wrong, others := askStorm(t, ask, func(int) (string, bool) { return "", false })
			if timeouts+wrong+others > 0 {
				t.Fatalf("%d dropped replies, %d wrong replies, %d other errors of 6400 Asks", timeouts, wrong, others)
			}
		})
	}
}

// TestLateReplyNotMisdelivered: a reply that arrives after its Ask timed out
// must never be delivered to another Ask (per-request reply channels).
// One request in 8 is answered after the 2 ms timeout.
func TestLateReplyNotMisdelivered(t *testing.T) {
	ask := func(ctx context.Context, _ actor.ActorSystem, to *actor.PID, msg string) (any, error) {
		return actor.Ask(ctx, to, wrapperspb.String(msg), 2*time.Millisecond)
	}
	_, wrong, _ := askStorm(t, ask, func(i int) (string, bool) {
		if i%8 == 0 {
			return "-slow", true
		}
		return "", true // under load a prompt reply may also miss 2 ms; only misdelivery fails
	})
	if wrong > 0 {
		t.Fatalf("%d late replies delivered to another Ask", wrong)
	}
}
