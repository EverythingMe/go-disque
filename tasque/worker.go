package tasque

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/EverythingMe/go-disque/disque"
	"github.com/garyburd/redigo/redis"
)

// A worker the runner for a worker process that runs goroutines as worker threads.
// You register TaskHandlers to handle tasks in the worker
type Worker struct {
	pool          *disque.Pool
	numGoroutines int
	workchan      chan *Task
	handlers      map[string]TaskHandler
	stopch        chan bool
	channels      []string
	mutx          sync.RWMutex
}

// Create a new worker that runs numGoroutines concurrently, connecting to disque addrs
func NewWorker(numGoroutines int, addrs ...string) *Worker {

	pool := disque.NewPool(disque.DialFunc(func(addr string) (redis.Conn, error) {
		return redis.Dial("tcp", addr)
	}), addrs...)

	pool.RefreshNodes()
	pool.RunRefreshLoop()

	return &Worker{
		pool:          pool,
		numGoroutines: numGoroutines,
		workchan:      make(chan *Task),
		handlers:      map[string]TaskHandler{},
		stopch:        make(chan bool),
		channels:      []string{},
	}
}

// Register a task handler in the worker. This shoudl
func (w *Worker) Handle(h TaskHandler) {

	w.mutx.Lock()
	defer w.mutx.Unlock()

	w.handlers[h.Id()] = h

	w.channels = append(w.channels, qname(h.Id()))

}

const getTimeout = time.Second

func (w *Worker) getHandler(t *Task) (TaskHandler, bool) {
	w.mutx.RLock()

	h, found := w.handlers[t.Name]
	return h, found
}

// runHandler safely wraps running a single task in a handler
func (w *Worker) runHandler(h TaskHandler, t *Task) (err error) {

	defer func() {
		e := recover()
		if e != nil {
			err, _ = e.(error)
			log.Println("tasque: PANIC handling task: %s %s", t.Name, e)
		}

	}()
	err = h.Handle(t)
	return
}

// a single worker loop
func (w *Worker) handlerLoop() {

	for task := range w.workchan {
		handler, found := w.getHandler(task)
		if !found {
			log.Printf("tasque: ERROR no handler for task %s", task.Name)
		}

		// call a safe runner func
		err := w.runHandler(handler, task)
		if err != nil {
			log.Println("tasque: Error handling task %s: %s", task.Name, err)
		} else {
			client, err := w.pool.Get()
			if err != nil {
				log.Println("Error getting client: %s", err)
			} else {
				client.Ack(task.JobId())
				client.Close()
			}

		}
	}
}

// Stop stops the worker from processing new tasks
func (w *Worker) Stop() {
	w.stopch <- true
	close(w.workchan)
}

// Run starts the worker and makes it request jobs
func (w *Worker) Run() {

	// TODO: make this dynamic
	for i := 0; i < w.numGoroutines; i++ {
		log.Println("Starting handler routine ", i+1)
		go w.handlerLoop()
	}

	for {
		client, err := w.pool.Get()
		defer client.Close()
		if err != nil {
			log.Println("tasque: could not get client")
			select {
			case <-w.stopch:
				return
			case <-time.After(100 * time.Millisecond):
			}

		}

		for {
			select {
			case <-w.stopch:
				return
			default:
			}

			job, err := client.Get(getTimeout, w.channels...)

			if err == nil {

				if job.Data != nil {

					task := new(Task)
					if err := json.Unmarshal(job.Data, task); err != nil {
						log.Printf("tasque: Could not unmarshal message data: %s", err)
						continue
					}
					task.jobId = job.Id()

					select {
					case w.workchan <- task:
					case <-w.stopch:
						return
					}
				}

			} else {
				log.Println("Error receiving: %s", err)
				client.Close()
				break
			}
		}
	}

}
