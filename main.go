package main

import (
	"flag"
	"fmt"
	"github.com/binance-chain/go-sdk/client/rpc"
	ctypes "github.com/binance-chain/go-sdk/common/types"
	"github.com/juju/ratelimit"
	"github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/types"
	"math/rand"
	"runtime/debug"
	"time"
)

var (
	nodeAddr           string
	seed               int64
	rate               int64
	testClientInstance *rpc.HTTP
)

func init() {
	flag.StringVar(&nodeAddr, "addr", "tcp://127.0.0.1:26657", "set addr path")
	flag.Int64Var(&seed, "seed", 100, "seed")
	flag.Int64Var(&rate, "rate", 5000, "rate")
}

func defaultClient() *rpc.HTTP {
	testClientInstance = rpc.NewRPCClient(nodeAddr, ctypes.TestNetwork)
	return testClientInstance
}

func main() {
	flag.Parse()
	c := defaultClient()
	r := rand.New(rand.NewSource(seed))
	bucket := ratelimit.NewBucketWithRate(float64(rate), rate)
	p := NewPool(300, 10000, 10)
	i := 0
	for {
		bucket.Wait(1)
		fmt.Println(i)
		i++
		p.Schedule(func() {
			txbyte := make([]byte, 400)
			_, err := r.Read(txbyte)
			if err != nil {
				fmt.Println(err)
			}
			_, err = c.BroadcastTxAsync(types.Tx(txbyte))
			if err != nil {
				fmt.Println(err)
			}
		})
	}
}

// ErrScheduleTimeout returned by Pool to indicate that there no free
// goroutines during some period of time.
var ErrScheduleTimeout = fmt.Errorf("schedule error: timed out")

// Pool contains logic of goroutine reuse.
type Pool struct {
	common.BaseService
	sem  chan struct{}
	work chan func()
}

// NewPool creates new goroutine pool with given size. It also creates a work
// queue of given size. Finally, it spawns given amount of goroutines
// immediately.
func NewPool(size, queue, spawn int) *Pool {
	if spawn <= 0 && queue > 0 {
		panic("dead queue configuration detected")
	}
	if spawn > size {
		panic("spawn > workers")
	}
	p := &Pool{
		sem:  make(chan struct{}, size),
		work: make(chan func(), queue),
	}
	p.BaseService = *common.NewBaseService(nil, "routine-pool", p)

	for i := 0; i < spawn; i++ {
		p.sem <- struct{}{}
		go p.worker(func() {})
	}

	return p
}

// Schedule schedules task to be executed over pool's workers.
func (p *Pool) Schedule(task func()) {
	p.schedule(task, nil)
}

// ScheduleTimeout schedules task to be executed over pool's workers.
// It returns ErrScheduleTimeout when no free workers met during given timeout.
func (p *Pool) ScheduleTimeout(timeout time.Duration, task func()) error {
	return p.schedule(task, time.After(timeout))
}

func (p *Pool) schedule(task func(), timeout <-chan time.Time) error {
	select {
	case <-timeout:
		return ErrScheduleTimeout
	case p.work <- task:
		return nil
	case p.sem <- struct{}{}:
		go p.worker(task)
		return nil
	}
}

func (p *Pool) worker(task func()) {
	defer func() {
		if r := recover(); r != nil {
			p.Logger.Error("failed to process task", "err", r, "stack", string(debug.Stack()))
		}
		<-p.sem
	}()

	task()

	for task := range p.work {
		task()
	}
}
