package tasque

import (
	"fmt"
	"time"

	"github.com/EverythingMe/go-disque/disque"
	"github.com/garyburd/redigo/redis"
)

// Client is used to enqueue tasks
type Client struct {
	pool              *disque.Pool
	enqueueTimeout    time.Duration
	replicationFactor int
}

// Create a new client for the given disque addrs. enqueueTimeout is the amount of time after which
// we fail
func NewClient(enqueueTimeout time.Duration, addrs ...string) *Client {

	pool := disque.NewPool(disque.DialFunc(func(addr string) (redis.Conn, error) {
		return redis.DialTimeout("tcp", addr, enqueueTimeout, enqueueTimeout, enqueueTimeout)
	}), addrs...)

	pool.RefreshNodes()
	pool.RunRefreshLoop()

	return &Client{
		pool:              pool,
		enqueueTimeout:    enqueueTimeout,
		replicationFactor: 0, //TODO
	}
}

func qname(tname string) string {
	return fmt.Sprintf("__tasque__%s", tname)
}

// Do puts the task in the queue for immediate execution
func (c *Client) Do(t *Task) (string, error) {
	return c.Delay(t, 0)
}

// Delay puts the task in the queue for execution after the delay period of time
func (c *Client) Delay(t *Task, delay time.Duration) (string, error) {

	client, err := c.pool.Get()

	if err != nil {
		return "", err
	}
	defer client.Close()

	b, err := t.marshal()
	if err != nil {
		return "", fmt.Errorf("Could not marshal task: %s", err)
	}

	ar := disque.AddRequest{
		Job: disque.Job{
			Queue: qname(t.Name),
			Data:  b,
		},
		Timeout:   c.enqueueTimeout,
		Replicate: c.replicationFactor,
		Delay:     delay,
	}

	id, err := client.Add(ar)
	t.jobId = id
	
	return id, err
}
