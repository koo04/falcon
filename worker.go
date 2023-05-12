package falcon

import (
	"context"
)

type Config struct {
	Job func(*Worker) error
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

func NewWorker(e *Engine, id int, cfg ...*Config) *Worker {
	w := &Worker{
		Id:     id,
		Status: "waiting",

		state: NewState[string, any](),
	}
	if len(cfg) == 1 {
		w.Config = cfg[0]
	}

	return w
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

			go func() {
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
				}

				// fmt.Println("on success:")
			}()

			// wait for the job to finish
			<-done
		}
	}
}
