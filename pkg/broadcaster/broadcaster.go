package broadcaster

import (
	"context"
	"net"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/peer"
)

var (
	// default peer for internal subscriptions that do not have a GRPC Peer
	defaultPeer = peer.Peer{Addr: &net.UnixAddr{Name: "internal", Net: "unix"}}
)

type ConnectFunc func() (chan interface{}, error)

type Broadcaster struct {
	sync.Mutex
	running bool
	subs    map[chan interface{}]peer.Peer
}

func (b *Broadcaster) Subscribe(ctx context.Context, connect ConnectFunc) (<-chan interface{}, error) {
	b.Lock()
	defer b.Unlock()

	if !b.running {
		if err := b.start(connect); err != nil {
			return nil, err
		}
	}

	sub := make(chan interface{}, 100)
	if b.subs == nil {
		b.subs = map[chan interface{}]peer.Peer{}
	}

	p := defaultPeer
	if cp, ok := peer.FromContext(ctx); ok {
		p = *cp
	}

	logrus.Debugf("BROADCASTER SUBSCRIBE %s", p.Addr)
	b.subs[sub] = p
	go func() {
		<-ctx.Done()
		b.unsub(sub, true)
	}()

	return sub, nil
}

func (b *Broadcaster) unsub(sub chan interface{}, lock bool) {
	if lock {
		b.Lock()
	}
	if peer, ok := b.subs[sub]; ok {
		logrus.Debugf("BROADCASTER UNSUBSCRIBE %s", peer.Addr)
		close(sub)
		delete(b.subs, sub)
	}
	if lock {
		b.Unlock()
	}
}

func (b *Broadcaster) start(connect ConnectFunc) error {
	logrus.Debug("BROADCASTER starting")
	c, err := connect()
	if err != nil {
		return err
	}

	go b.stream(c)
	b.running = true
	return nil
}

func (b *Broadcaster) stream(input chan interface{}) {
	for item := range input {
		b.Lock()
		for sub, peer := range b.subs {
			select {
			case sub <- item:
				if l := len(sub); l > 0 {
					logrus.Debugf("BROADCASTER STREAM QUEUE %s: %d", peer.Addr, l)
				}
			default:
				logrus.Warnf("BROADCASTER STREAM DROP %s: slow subscriber", peer.Addr)
				go b.unsub(sub, true)
			}
		}
		b.Unlock()
	}

	logrus.Debug("BROADCASTER stopped")
	b.Lock()
	for sub := range b.subs {
		b.unsub(sub, false)
	}
	b.running = false
	b.Unlock()
}
