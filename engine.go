package falcon

import (
	"context"
	"encoding/json"
	"log"
	"sync"
)

type maxWorkers int
type getWorkerChan chan *Worker
type shutdown struct{}

type Engine struct {
	MaxWorkers int     `json:"max_workers"`
	Config     *Config `json:"-"`

	messagesProcessed int64
	workers           *State[int, *Worker]

	msgch    chan any
	workch   chan any
	ctx      context.Context
	cancel   context.CancelFunc
	shutdown bool

	// blah
	mu sync.RWMutex
}

func NewEngine() *Engine {
	ctx, cancel := context.WithCancel(context.Background())

	e := &Engine{
		Config:  DefaultConfig,
		workers: NewState[int, *Worker](),
		msgch:   make(chan any),
		workch:  make(chan any),
		ctx:     ctx,
		cancel:  cancel,
	}

	go e.loop()

	return e
}

func (e *Engine) String() string {
	output := struct {
		MessagesProcessed int64                `json:"messages_processed"`
		Workers           *State[int, *Worker] `json:"workers"`
	}{
		MessagesProcessed: e.messagesProcessed,
		Workers:           e.workers,
	}
	b, err := json.Marshal(output)
	if err != nil {
		log.Println(err)
	}
	return string(b)
}

func (e *Engine) WithConfig(cfg *Config) *Engine {
	e.Config = cfg
	return e
}

func (e *Engine) WithMaxWorkers(max int) *Engine {
	e.MaxWorkers = max
	return e
}

func (e *Engine) SetMaxWorkers(max int) {
	e.Receive(maxWorkers(max))
}

func (e *Engine) Start() *Engine {
	// set default config
	if e.Config == nil {
		e.Config = &Config{}
	}

	// start the pool
	go func() {
		// TODO: this should keep a minimum number of workers ready to work
		for i := 0; i < e.MaxWorkers; i++ {
			w := NewWorker(e, i, e.Config)
			e.Receive(w)
		}
	}()

	return e
}

func (e *Engine) Close() error {
	e.Receive(shutdown{})
	return nil
}

func (e *Engine) Queue(msg any) {
	e.workch <- msg
	e.messagesProcessed++
}

func (e *Engine) Receive(msg any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.shutdown {
		return
	}
	e.msgch <- msg
}

func (e *Engine) GetWorkers() <-chan *Worker {
	out := make(chan *Worker)

	go e.Receive(getWorkerChan(out))

	return out
}

func (e *Engine) GetProcessedCount() int {
	return int(e.messagesProcessed)
}

func (e *Engine) loop() {
	for msg := range e.msgch {
		// fmt.Println("------:", reflect.TypeOf(msg))
		e.handleMessage(msg)
	}
}

func (e *Engine) handleMessage(message any) {
	switch msg := message.(type) {
	case *Worker:
		e.addWorker(msg)
	case maxWorkers:
		e.updateMaxWorkers(int(msg))
	case getWorkerChan:
		e.handleGetWorkers(msg)

	case shutdown:
		// cancel context for all procs
		e.cancel()

		// shut down all channels
		e.mu.Lock()
		e.shutdown = true
		close(e.msgch)
		close(e.workch)
		e.mu.Unlock()
	}
}

func (e *Engine) handleGetWorkers(out getWorkerChan) {
	e.workers.ForEach(func(k int, w *Worker) {
		out <- w
	})
	close(out)
}

func (e *Engine) updateMaxWorkers(max int) {
	e.MaxWorkers = max
}

func (e *Engine) addWorker(w *Worker) {
	if e.workers.Len() >= e.MaxWorkers {
		return
	}

	w.ctx = e.ctx
	w.queue = e.workch
	w.parent = e

	e.workers.Set(w.Id, w)

	// fmt.Println("adding worker: ", w.Id)
	go w.work()
}
