package falcon

import (
	"context"
	"log"
)

type Config struct {
	Job       func(*Worker) error
	OnSuccess func(*Worker)
	OnError   func(error, *Worker)
}

type Worker struct {
	Id     int    `json:"id"`
	Status string `json:"status"`

	*Config `json:"-"`

	state  *State[string, any]
	parent *Engine

	queue chan any
	ctx   context.Context
}

var DefaultConfig = &Config{
	Job: func(w *Worker) error {
		log.Println("No Job Configured")
		return nil
	},
	OnSuccess: func(w *Worker) { log.Println("No OnSuccess Configured") },
	OnError:   func(err error, w *Worker) { log.Println("No OnError Configured") },
}

func NewWorker(e *Engine, id int, cfg ...*Config) *Worker {
	w := &Worker{
		Id:     id,
		Status: "waiting",
		Config: DefaultConfig,

		state: NewState[string, any](),
	}
	if len(cfg) == 1 {
		w.Config = cfg[0]
	}
	return w
}

func (w *Worker) GetState(state string) (any, bool) {
	return w.state.Get(state)
}

func (w *Worker) Close() error {
	// fmt.Println("closing worker:", w.Id)
	w.Status = "closed"
	return nil
}

func (w *Worker) work() {
	for {
		select {
		case <-w.ctx.Done():
			// fmt.Println("context done; stop looking for work")
			w.Status = "closing"
			w.Close()
			return
		case msg := <-w.queue:
			if msg == nil {
				// nothing to do
				continue
			}

			// make a done channel to keep
			// track of the worker status
			done := make(chan bool)

			go func(w *Worker) {
				defer func() {
					w.Status = "waiting"
					done <- true
				}()

				w.Status = "processing"
				w.state.Set("message", msg)

				if w.Config == nil {
					// fmt.Println("no configured jobs")
					return
				}

				if err := w.Job(w); err != nil {
					// fmt.Println("on error:", err)
					w.OnError(err, w)
					return
				}

				// fmt.Println("on success:")
				w.OnSuccess(w)
			}(w)

			// wait for the job to finish
			<-done
		}
	}
}
