// Package disque implements a simlpe client for the Disque in-memory distributed queue [https://github.com/antirez/disque]
package disque

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Job describes a job that is about to be enqueued or is received from the client for proecessing
type Job struct {
	// The name of the queue this job is sent/received to/from
	Queue string
	// Job data - this can be anything
	Data []byte

	// the job id, available only for jobs received from the client, not enqueued
	id string
}

// Id returns the job id. the underlying id is only settable by the client, so there is only a getter for it
func (j Job) Id() string {
	return j.id
}

// AddRequest describes how you want a job to be enqueued
type AddRequest struct {
	// The job about to be added to the queue
	Job Job
	// If the add is not async, this is the maximal timeout for replicating the job to the minimal number of nodes
	Timeout time.Duration
	//  the number of nodes the job should be replicated to.
	Replicate int
	//  the duration that should elapse before the job is queued by any server
	Delay time.Duration
	// A duration after which, if no ACK is received, the job is put again into the queue for delivery
	Retry time.Duration
	// the max job life in seconds. After this time, the job is deleted even if it was not successfully delivered
	TTL time.Duration
	// if there are already Maxlen messages queued for the specified queue, adding returns an error
	Maxlen int
	// If set to true, the add command returns ASAP and replicates the job to other nodes in the background
	Async bool
}

// HelloResponse is returned from the Hello command and tells us the state of the cluster
type HelloResponse struct {
	NodeId string
	Nodes  nodeList
}

// Client is the interface that describes a disque client
type Client interface {

	// Add sents an ADDJOB command to disque, as specified by the AddRequest. Returns the job id or an error
	Add(AddRequest) (string, error)

	// Get gets one job from any of the given queues, or times out if timeout has elapsed without a job being available. Returns a job or an error
	Get(timeout time.Duration, queues ...string) (Job, error)

	// GetMulti gets <count> jobs from the given queues, or times out if timeout has elapsed without enough jobs being available. Returns a job or an error
	GetMulti(count int, timeout time.Duration, queues ...string) ([]Job, error)

	// Ack sends and ACKJOB command with the given job ids
	Ack(jobIds ...string) error

	// FastAck sends a FASTACK commadn with the given job ids. See the disque docs about the
	// difference between ACK and FASTACK
	FastAck(jobIds ...string) error

	// Qlen returns the length of a given queue
	Qlen(qname string) (int, error)

	// Hello is a handshake request with the server, returns a description of the cluster state
	Hello() (HelloResponse, error)

	// Close closes the underlying connection
	Close() error
}

// RedisClient implements a redigo based client
type RedisClient struct {
	conn redis.Conn
	node Node
}

//// Dial tries to connect to one of the addrs of the servers, and returns a new client if it succeeds
//func Dial(net string, timeout time.Duration, addrs ...string) (client Client, err error) {

//	var conn redis.Conn
//	for _, addr := range addrs {

//		if conn, err = redis.DialTimeout(net, addr, timeout, timeout, timeout); err != nil {
//			continue
//		}

//		client = &RedisClient{
//			conn:  conn,
//			net:   net,
//			addrs: addrs,
//		}

//		break

//	}

//	return
//}

// Close closes the underlying connection
func (c *RedisClient) Close() error {
	return c.conn.Close()
}

// Add sents an ADDJOB command to disque, as specified by the AddRequest. Returns the job id or an error
func (c *RedisClient) Add(r AddRequest) (string, error) {
	//ADDJOB queue_name job <ms-timeout> [REPLICATE <count>] [DELAY <sec>] [RETRY <sec>] [TTL <sec>] [MAXLEN <count>] [ASYNC]

	args := redis.Args{r.Job.Queue, r.Job.Data, int(r.Timeout / time.Millisecond)}

	if r.Replicate > 0 {
		args = args.Add("REPLICATE", r.Replicate)
	}

	if r.Delay > 0 {
		args = args.Add("DELAY", int64(r.Delay.Seconds()))
	}

	if r.Retry > 0 {
		args = args.Add("RETRY", int64(r.Retry.Seconds()))
	}

	if r.TTL > 0 {
		args = args.Add("TTL", int64(r.TTL.Seconds()))
	}

	if r.Maxlen > 0 {
		args = args.Add("MAXLEN", r.Maxlen)
	}

	if r.Async {
		args = args.Add("ASYNC")
	}

	id, err := redis.String(c.conn.Do("ADDJOB", args...))
	if err != nil {
		return "", errors.New("disque: could not add job: " + err.Error())
	}
	return id, nil
}

// Get gets one job from any of the given queues, or times out if timeout has elapsed without a job being available.
// Returns a job or an error
func (c *RedisClient) Get(timeout time.Duration, queues ...string) (Job, error) {

	ret, err := c.GetMulti(0, timeout, queues...)
	if err != nil {
		return Job{}, err
	}
	if ret == nil || len(ret) == 0 {
		return Job{}, errors.New("disque: no jobs returned")
	}
	return ret[0], nil

}

// GetMulti gets <count> jobs from the given queues, or times out if timeout has elapsed without
// enough jobs being available. Returns a list of jobs or an error
func (c *RedisClient) GetMulti(count int, timeout time.Duration, queues ...string) ([]Job, error) {

	if len(queues) == 0 {
		return nil, errors.New("disque: no queues specified")
	}
	if count < 0 {
		return nil, fmt.Errorf("disque: invalid count %d", count)
	}

	args := redis.Args{}
	if timeout > 0 {
		args.Add("TIMEOUT", timeout/time.Millisecond)
	}
	if count > 0 {
		args = args.Add("COUNT", count)
	}
	args = args.Add("FROM")
	args = args.AddFlat(queues)

	vals, err := redis.Values(c.conn.Do("GETJOB", args...))

	if err != nil {
		return nil, fmt.Errorf("disque: could not get jobs: %s", err)
	}

	ret := make([]Job, 0, len(vals))

	for _, v := range vals {
		if arr, ok := v.([]interface{}); ok {

			ret = append(ret, Job{
				Queue: string(arr[0].([]byte)),
				id:    string(arr[1].([]byte)),
				Data:  arr[2].([]byte),
			})

		}

	}

	return ret, nil

}

// Ack sends and ACKJOB command with the given job ids
func (c *RedisClient) Ack(jobIds ...string) error {

	args := make(redis.Args, 0, len(jobIds))
	args.AddFlat(jobIds)
	if _, err := c.conn.Do("ACKJOB", args...); err != nil {
		return fmt.Errorf("disque: error sending ACK: %s", err)
	}
	return nil
}

// FastAck sends a FASTACK commadn with the given job ids. See the disque docs about the
// difference between ACK and FASTACK
func (c *RedisClient) FastAck(jobIds ...string) error {
	args := make(redis.Args, 0, len(jobIds))
	args.AddFlat(jobIds)
	if _, err := c.conn.Do("FASTACK", args...); err != nil {
		return fmt.Errorf("disque: error sending ACK: %s", err)
	}
	return nil
}

// Qlen returns the length of a given queue
func (c *RedisClient) Qlen(qname string) (int, error) {

	return redis.Int(c.conn.Do("QLEN", qname))

}

const HelloVersionId = 1

// Hello is a handshake request with the server, returns a description of the cluster state
// TODO: implement this
func (c *RedisClient) Hello() (HelloResponse, error) {

	vals, err := redis.Values(c.conn.Do("HELLO"))
	ret := HelloResponse{}
	if err != nil {
		return ret, err
	}

	if len(vals) < 3 {
		return ret, fmt.Errorf("disque: invalid HELLO response: %v", vals)
	}

	versionId := vals[0].(int64)
	if versionId != HelloVersionId {
		return ret, fmt.Errorf("disque: unsupported HELLo version: %d, expected %d", versionId, HelloVersionId)
	}

	ret.NodeId = string(vals[1].([]byte))
	ret.Nodes = make(nodeList, 0, len(vals)-2)
	for _, v := range vals {

		if arr, ok := v.([]interface{}); ok {
			if len(arr) != 4 {
				log.Println("Invalid HELLO entry: %v", arr)
				continue
			}

			prio, err := strconv.ParseInt(string(arr[3].([]byte)), 32, 10)
			if err != nil {
				log.Println("Invalid priority: %v", arr[3])
				continue
			}

			addr := ""
			host := string(arr[1].([]byte))

			//in a single node, it doesn't know its IP, we just take it from the client itself
			if host == "" {
				addr = c.node.Addr
			} else {
				addr = fmt.Sprintf("%s:%s", host, string(arr[2].([]byte)))
			}

			add, err := net.ResolveTCPAddr("tcp", addr)
			if err != nil {
				log.Printf("Invalid address given: %s", addr)
				continue
			}
			node := Node{
				Id:       string(arr[0].([]byte)),
				Addr:     add.String(),
				Priority: int(prio),
			}

			ret.Nodes = append(ret.Nodes, node)
		}

	}

	return ret, nil
}
