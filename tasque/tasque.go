// Package tasque implements a simple task processing framework on top of disque.
// The idea is that you create "task handlers" that process tasks, similar to how http handlers process requests.
// Each task handler must have a unique name or id that is used to enqueue tasks in it, like a n http handler
// is routed to a URL.
//
// You create a Worker that is a server for executing task handlers, and register task handlers in it.
//
// Then,for enqueueing tasks, you create a client and enqueue them by name, optionally giving tasks properties.
package tasque

import (
	"encoding/json"
	"time"
)

// A task represents the name and parameters of a task we want to execute on a worker.
// You can use it to pass parameters to the job executor
type Task struct {
	Name        string
	Params      map[string]interface{}
	jobId       string
	EnqueueTime time.Time
	ttl         time.Duration
	retry       time.Duration
}

// JobId The task's jobId, applicable only to tasks received from the server
func (t Task) JobId() string {
	return t.jobId
}

func (t Task) marshal() ([]byte, error) {
	return json.Marshal(t)
}

// Create a new task with a given id
func NewTask(id string) *Task {
	return &Task{
		Name:   id,
		Params: make(map[string]interface{}),
	}
}

// Set a property in the task
func (t *Task) Set(k string, v interface{}) *Task {
	t.Params[k] = v
	return t
}

// Set the task TTL - if it will not succeed after this time, disque will give up on it
func (t *Task) SetTTL(ttl time.Duration) *Task {
	t.ttl = ttl
	return t
}

// Set the retry timeout. This must be greater than 1. If the worker does not ACK the task in this timeout,
// disque will try to re-queue it
func (t *Task) SetRetry(d time.Duration) *Task {
	t.retry = d
	return t
}

// Execute the task on the client
func (t *Task) Do(c *Client) error {
	return c.Do(t)
}

// Delay executes the task, delayed for d duration
func (t *Task) Delay(c *Client, d time.Duration) error {
	return c.Delay(t, d)
}

// TaskHandler is the interface that handlers must provide
type TaskHandler interface {
	Handle(*Task) error
	Id() string
}

// FuncTaskHandler wraps a func as a complete handler
type FuncTaskHandler struct {
	f  func(*Task) error
	id string
}

// Handle calls the underlying func to handle the task
func (fh FuncTaskHandler) Handle(t *Task) error {
	return fh.f(t)
}

// Id returns the id of the handler
func (fh FuncTaskHandler) Id() string {
	return fh.id
}

// FuncHandler takes a func and its id and converts them into a FuncTaskHandler
func FuncHandler(f func(*Task) error, id string) FuncTaskHandler {
	return FuncTaskHandler{
		f:  f,
		id: id,
	}
}
