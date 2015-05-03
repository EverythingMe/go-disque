// Package disqchan is an abstraction of Disque messages over Go channels.
//
// The idea is that you create Chan objects on multiple machines. Each of them has to have the same
// name across machines, and can be used to send and/or receive messages.
//
// Messages can be any object. Messages are serialized as JSON, so the usual rules of JSON marshalling apply
package disqchan

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/EverythingMe/go-disque/disque"
	"github.com/garyburd/redigo/redis"
)

// Chan represents a cross machine "channel" over disque
type Chan struct {
	sendch chan interface{}
	rcvch  chan interface{}
	pool   *disque.Pool
	name   string
	qname  string
	async  bool
	stopch chan bool
	mutx   sync.Mutex
}

// Name returns the name of the chan
func (c Chan) Name() string {
	return c.name
}

func dial(addr string) (redis.Conn, error) {

	return redis.Dial("tcp", addr)
}

var (
	// The timeout on GETJOB requests
	GetTimeout = time.Second
)

// NewChan creates a new disque channel objec with a given name, over a disque cluster with the given addrs.
// If async is true, messages are sent using disque async replication (see disque docs for details)
func NewChan(name string, async bool, addrs ...string) *Chan {
	ret := &Chan{
		sendch: nil,
		rcvch:  nil,
		pool:   disque.NewPool(disque.DialFunc(dial), addrs...),
		name:   name,
		qname:  fmt.Sprintf("__qchan__%s", name),
		async:  async,
		stopch: make(chan bool),
	}
	fmt.Println(addrs)
	err := ret.pool.RefreshNodes()
	if err != nil {
		log.Println("Error refreshing nodes: %s", err)
	}

	ret.pool.RunRefreshLoop()
	return ret
}

// Stop stop the chan's internal send/receive loops
func (c *Chan) Stop() {
	c.mutx.Lock()
	defer c.mutx.Unlock()
	if c.stopch != nil {
		if c.sendch != nil {
			close(c.sendch)
		}

		c.stopch <- true
		close(c.stopch)
	}
}

// SendChan returns a channel to which objects can be sent
func (c *Chan) SendChan() chan<- interface{} {

	c.mutx.Lock()
	defer c.mutx.Unlock()

	if c.sendch == nil {
		c.sendch = make(chan interface{})
		go c.sendLoop()
	}
	return c.sendch
}

// RecvChan returns a channel from which received messages can be received.
//
// Before it is called, this Chan is not receiving from the queue
func (c *Chan) RecvChan() <-chan interface{} {
	c.mutx.Lock()
	defer c.mutx.Unlock()
	if c.rcvch == nil {
		c.rcvch = make(chan interface{})
		go c.receiveLoop()
	}
	return c.rcvch
}

type message struct {
	Val interface{}
}

func (c *Chan) receiveLoop() {

	for {
		client, err := c.pool.Get()

		if err != nil {
			log.Println("disqchan: could not get client:", err)
			select {
			case <-c.stopch:
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}

		}

		for {
			select {
			case <-c.stopch:
				return
			default:
			}

			job, err := client.Get(GetTimeout, c.qname)
			if err == nil {

				client.Ack(job.Id())

				if job.Data != nil {

					var msg message
					if err := json.Unmarshal(job.Data, &msg); err != nil {
						log.Printf("disqchan: Could not unmarshal message data: %s", err)
						continue
					}

					select {
					case c.rcvch <- msg.Val:
					default:
						// no listener - let's re-queue the job
						c.SendChan() <- msg.Val
						break
					}
				}

			} else {
				log.Println("Error receiving: %s", err)
				break
			}
		}
	}

}

func (c *Chan) sendLoop() {

	for {

		client, err := c.pool.Get()
		if err != nil {
			log.Println("disqchan: Cannot send message - could not get client", err)
			select {
			case <-c.stopch:
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}

		}
		select {

		case val := <-c.sendch:

			b, err := json.Marshal(message{Val: val})
			if err != nil {
				log.Printf("disqchan: Could not mashal value: %s", err)
				continue
			}

			ar := disque.AddRequest{
				Job: disque.Job{
					Queue: c.qname,
					Data:  b,
				},
				Timeout: time.Second,
				Retry:   time.Second,
				// we must ensure replication to ALL nodes
				Replicate: c.pool.Size(),
				Async:     c.async,
			}

			if _, err := client.Add(ar); err != nil {
				log.Printf("disqchan: Could not send message: %s", err)
			}

		case <-c.stopch:
			return

		}
		client.Close()
	}
}
