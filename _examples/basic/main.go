package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/midgarco/falcon"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// done channel to know when the error is handled
	done := make(chan bool)

	e := falcon.NewEngine().WithConfig(&falcon.Config{
		Before: func(w *falcon.Worker) error {
			logger.With("id", w.GetId()).Info("got work")
			return nil
		},
		Job: func(w *falcon.Worker) error {
			// set the worker specific logger
			wlogger := logger.With("id", w.GetId())

			wlogger.Info("doing work")
			message, ok := w.GetState(string(falcon.WorkerDefaultStateMessage))
			if !ok {
				return errors.New("no message state")
			}

			if message.(string) == "error" {
				return errors.New("example error")
			}

			wlogger.Info(message.(string))
			return nil
		},
		OnSuccess: func(w *falcon.Worker) {
			logger.Info("successfully finished job")
		},
		OnError: func(err error, w *falcon.Worker) {
			logger.With("error", err).Info("ran into an error")
			close(done)
		},
		OnComplete: func(w *falcon.Worker) {
			logger.With("id", w.GetId()).Info("worker completed job")
		},
	}).WithMaxWorkers(3).Start()
	defer e.Close()

	i := 1
	for {
		e.Queue("hello world: " + fmt.Sprint(i))

		i++
		if i > 3 {
			break
		}
	}

	e.Queue("error")

	// wait for the error to occour
	<-done
}
