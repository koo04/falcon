package falcon

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
)

type Event struct {
	ProcessId string `json:"process_id"`
}

func TestEngine(t *testing.T) {
	engine := NewEngine().WithMaxWorkers(5)
	defer engine.Close()

	for i := 0; i < 10; i++ {
		w := NewWorker(engine, i)
		go engine.Receive(w)
	}

	workers1 := engine.GetWorkers()
	for w := range workers1 {
		log.Println("1-worker: ", w.Id, w.GetStatus())
	}

	engine.SetMaxWorkers(10)
	log.Println("set workers to 10")

	for i := 10; i < 20; i++ {
		w := NewWorker(engine, i)
		go engine.Receive(w)
	}

	workers2 := engine.GetWorkers()
	for w := range workers2 {
		log.Println("2-worker: ", w.Id, w.GetStatus())
	}
}

func TestEngineQueue(t *testing.T) {
	engine := NewEngine().WithMaxWorkers(5).Start()
	defer engine.Close()

	workers := engine.GetWorkers()
	for w := range workers {
		log.Println("q-worker: ", w.Id, w.GetStatus())
	}

	ev := &Event{ProcessId: "001"}
	engine.Queue(ev)

	ev2 := &Event{ProcessId: "002"}
	engine.Queue(ev2)
}

func TestEngineConfig(t *testing.T) {
	wg := sync.WaitGroup{}

	engine := NewEngine().WithMaxWorkers(5).WithConfig(&Config{
		Before: func(w *Worker) error {
			workers := w.Parent().GetWorkers()
			for pw := range workers {
				if w.Id == pw.Id {
					continue
				}

				state, ok := w.GetState("message")
				if !ok {
					return errors.New("no message state")
				}
				event := state.(*Event)

				log.Println("before worker:", w.Id, "compare:", pw.Id, "process:", event.ProcessId)
			}
			return nil
		},
		Job: func(w *Worker) error {
			st, ok := w.GetState("message")
			if !ok {
				return errors.New("couldn't find state")
			}
			log.Println("job code from worker:", w.Id, "event:", st)
			return nil
		},
		OnSuccess: func(w *Worker) {
			wg.Done()

			st, _ := w.GetState("message")
			log.Println("success from worker:", w.Id, "event:", st)
		},
	}).Start()
	defer engine.Close()

	for i := 0; i < 5; i++ {
		wg.Add(1)
		ev := &Event{ProcessId: fmt.Sprintf("%03d", i)}
		engine.Queue(ev)
		log.Println(engine)
	}

	log.Println("waiting for all jobs to finish")
	log.Println(engine)
	wg.Wait()
}

func BenchmarkEngineQueue(b *testing.B) {
	engine := NewEngine().WithMaxWorkers(5).Start()
	defer engine.Close()

	b.ResetTimer()
	b.Run("queue message", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ev := &Event{ProcessId: fmt.Sprintf("%d", i)}
			engine.Queue(ev)
		}
	})
}
