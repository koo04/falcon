package falcon

import (
	"fmt"
	"log"
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
		log.Println("1-worker: ", w.Id, w.Status)
	}

	engine.SetMaxWorkers(10)
	log.Println("set workers to 10")

	for i := 10; i < 20; i++ {
		w := NewWorker(engine, i)
		go engine.Receive(w)
	}

	workers2 := engine.GetWorkers()
	for w := range workers2 {
		log.Println("2-worker: ", w.Id, w.Status)
	}
}

func TestEngineQueue(t *testing.T) {
	engine := NewEngine().WithMaxWorkers(5).Start()
	defer engine.Close()

	workers := engine.GetWorkers()
	for w := range workers {
		log.Println("q-worker: ", w.Id, w.Status)
	}

	ev := &Event{ProcessId: "001"}
	engine.Queue(ev)

	ev2 := &Event{ProcessId: "002"}
	engine.Queue(ev2)

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
