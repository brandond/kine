package server

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/peer"
)

var watchID int64

// explicit interface check
var _ etcdserverpb.WatchServer = (*KVServerBridge)(nil)

func (s *KVServerBridge) Watch(ws etcdserverpb.Watch_WatchServer) error {
	w := watcher{
		server:  ws,
		backend: s.limited.backend,
		watches: map[int64]func(){},
	}
	defer w.Close()

	if p, ok := peer.FromContext(ws.Context()); ok {
		w.peer = *p
	}

	logrus.Debugf("WATCH SERVER CREATE, peer=%s", w.peer.Addr)

	for {
		msg, err := ws.Recv()
		if err != nil {
			return err
		}

		if cr := msg.GetCreateRequest(); cr != nil {
			w.Start(ws.Context(), cr)
		}
		if cr := msg.GetCancelRequest(); cr != nil {
			logrus.Debugf("WATCH CANCEL REQ id=%d", cr.WatchId)
			w.Cancel(cr.WatchId, 0, 0, nil)
		}
	}
}

type watcher struct {
	sync.Mutex

	wg      sync.WaitGroup
	backend Backend
	peer    peer.Peer
	server  etcdserverpb.Watch_WatchServer
	watches map[int64]func()
}

func (w *watcher) Start(ctx context.Context, r *etcdserverpb.WatchCreateRequest) {
	w.Lock()
	defer w.Unlock()
	w.wg.Add(1)

	ctx, cancel := context.WithCancel(ctx)

	id := atomic.AddInt64(&watchID, 1)
	w.watches[id] = cancel

	key := string(r.Key)

	logrus.Debugf("WATCH START id=%d, count=%d, key=%s, end=%v, revision=%d, filters=%v, fragment=%v, prev=%v, peer=%s",
		id, len(w.watches), key, string(r.RangeEnd), r.StartRevision, r.Filters, r.Fragment, r.PrevKv, w.peer.Addr)

	go func() {
		defer w.wg.Done()
		if err := w.server.Send(&etcdserverpb.WatchResponse{
			Header:  &etcdserverpb.ResponseHeader{},
			Created: true,
			WatchId: id,
		}); err != nil {
			w.Cancel(id, 0, 0, err)
			return
		}

		wr := w.backend.Watch(ctx, key, r.StartRevision)

		if wr.CompactRevision != 0 {
			w.Cancel(id, wr.CurrentRevision, wr.CompactRevision, ErrCompacted)
			return
		}

		outer := true
		for outer {
			// Block on initial read from channel
			reads := 1
			events := <-wr.Events

			// Collect additional queued events from the channel
			inner := true
			for inner {
				select {
				case e, ok := <-wr.Events:
					reads++
					events = append(events, e...)
					if !ok {
						// channel was closed, break out of both loops
						inner = false
						outer = false
					}
				default:
					inner = false
				}
			}

			// Send collected events in a single response
			if len(events) > 0 {
				if logrus.IsLevelEnabled(logrus.DebugLevel) {
					for _, event := range events {
						logrus.Debugf("WATCH READ id=%d, key=%s, revision=%d", id, event.KV.Key, event.KV.ModRevision)
					}
				}

				wr := &etcdserverpb.WatchResponse{
					Header:  txnHeader(events[len(events)-1].KV.ModRevision),
					WatchId: id,
					Events:  toEvents(events...),
				}
				logrus.Debugf("WATCH SEND id=%d, events=%d, size=%d reads=%d", id, len(wr.Events), wr.Size(), reads)
				if err := w.server.Send(wr); err != nil {
					w.Cancel(id, 0, 0, err)
				}
			}
		}

		w.Cancel(id, 0, 0, nil)
		logrus.Debugf("WATCH CLOSE id=%d, key=%s", id, key)
	}()
}

func toEvents(events ...*Event) []*mvccpb.Event {
	ret := make([]*mvccpb.Event, 0, len(events))
	for _, e := range events {
		ret = append(ret, toEvent(e))
	}
	return ret
}

func toEvent(event *Event) *mvccpb.Event {
	e := &mvccpb.Event{
		Kv:     toKV(event.KV),
		PrevKv: toKV(event.PrevKV),
	}
	if event.Delete {
		e.Type = mvccpb.DELETE
	} else {
		e.Type = mvccpb.PUT
	}

	return e
}

func (w *watcher) Cancel(watchID, revision, compactRev int64, err error) {
	var reason string
	if err != nil {
		reason = err.Error()
	}
	logrus.Debugf("WATCH CANCEL id=%d, reason=%s, compactRev=%d", watchID, reason, compactRev)

	serr := w.server.Send(&etcdserverpb.WatchResponse{
		Header:          &etcdserverpb.ResponseHeader{Revision: revision},
		Canceled:        err != nil,
		CancelReason:    reason,
		WatchId:         watchID,
		CompactRevision: compactRev,
	})
	if serr != nil && err != nil && !clientv3.IsConnCanceled(serr) {
		logrus.Errorf("WATCH Failed to send cancel response for watchID %d: %v", watchID, serr)
	}
	w.Lock()
	if cancel, ok := w.watches[watchID]; ok {
		cancel()
		delete(w.watches, watchID)
	}
	w.Unlock()
}

func (w *watcher) Close() {
	logrus.Debugf("WATCH SERVER CLOSE peer=%s", w.peer.Addr)
	w.Lock()
	for _, v := range w.watches {
		v()
	}
	w.Unlock()
	w.wg.Wait()
}
