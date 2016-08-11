package disque

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// NodeList is a list of nodes with search
type nodeList []Node

func (l nodeList) contains(n Node) bool {
	for _, node := range l {
		if n.Addr == node.Addr {
			return true
		}
	}

	return false
}

// Node describes a node in the cluster, received from Hello
type Node struct {
	Id       string
	Addr     string
	Priority int
}

// IsNull checks if a node is empty or not
func (n Node) IsNull() bool {
	return n.Addr == ""
}

// DialFunc is a redis dialer function that should be supplied to the pool
type DialFunc func(string) (redis.Conn, error)

// Pool is a client pool that keeps track of the available servers in the cluster, and retrieves
// clients to random nodes in the cluster. Pooled connections should be closed to automatically
// be returned to the pool
type Pool struct {
	mutx     sync.Mutex
	nodes    nodeList
	pools    map[string]*redis.Pool
	dialFunc DialFunc
	// for borrow tests
	numBorrowed int
}

func (p *Pool) Size() int {
	defer scopedLock(&p.mutx)()
	n := 0
	for _, node := range p.nodes {
		if node.Priority <= maxPriority {
			n++
		}
	}
	return n
}

// NewPool creates a new client pool, with a given redis dial function, and an initial list of ip:port addresses
// to try connecting to. You should call RefreshNodes after creating the pool to update the list of all
// nodes in the pool, and optionally call RunRefreshLoop to let the queue do this periodically in the background
func NewPool(f DialFunc, addrs ...string) *Pool {

	rand.Seed(time.Now().UnixNano())
	nodes := nodeList{}
	for _, addr := range addrs {
		nodes = append(nodes, Node{Addr: addr, Priority: 1})
	}

	return &Pool{
		mutx:     sync.Mutex{},
		nodes:    nodes,
		dialFunc: f,
		pools:    make(map[string]*redis.Pool),
	}
}

func scopedLock(m *sync.Mutex) func() {

	m.Lock()
	return func() {
		m.Unlock()
	}

}

//TMP - we only handle nodes with priority 1 right now
const maxPriority = 1

// selectNode select a valid node by random. Currently only nodes with priority 1 are selected
func (p *Pool) selectNode(selected nodeList) (Node, error) {

	defer scopedLock(&p.mutx)()

	if len(p.nodes) == 0 {
		return Node{}, errors.New("disque: no nodes in pool")
	}

	nodes := nodeList{}
	for _, node := range p.nodes {
		if node.Priority <= maxPriority && !selected.contains(node) {
			nodes = append(nodes, node)
		}
	}
	if len(nodes) == 0 {
		return Node{}, errors.New("disque: no nodes left to select from")
	}

	return nodes[rand.Intn(len(nodes))], nil

}

const (
	maxIdle              = 3
	refreshFrequency     = time.Minute
	testOnBorrowInterval = time.Second
)

// getPool returns a redis connection pool for a given address
func (p *Pool) getPool(addr string) *redis.Pool {

	defer scopedLock(&p.mutx)()

	pool, found := p.pools[addr]
	if !found {
		pool = redis.NewPool(func() (redis.Conn, error) {
			return p.dialFunc(addr)
		}, maxIdle)

		pool.TestOnBorrow = func(c redis.Conn, t time.Time) error {

			// for testing - count how many borrows we did
			p.numBorrowed++
			if time.Since(t) > testOnBorrowInterval {
				_, err := c.Do("PING")
				return err
			}
			return nil
		}

		p.pools[addr] = pool
	}

	return pool

}

// Close closes all pools
func (p *Pool) Close() error {
	defer scopedLock(&p.mutx)()

	var err error
	for _, pool := range p.pools {
		if e := pool.Close(); e != nil {
			err = e
		}
	}
	return err
}

// Get returns a client, or an error if we could not init one
func (p *Pool) Get() (Client, error) {

	selected := nodeList{}
	var node Node
	var err error
	// select node to connect to
	for {
		node, err = p.selectNode(selected)
		if err != nil {
			return nil, err
		}

		conn := p.getPool(node.Addr).Get()
		if conn.Err() != nil {
			selected = append(selected, node)
			continue
		}

		return &RedisClient{
			conn: conn,
			node: node,
		}, nil
	}

}

// UpdateNodes explicitly sets the nodes of the pool
func (p *Pool) UpdateNodes(nodes nodeList) {
	defer scopedLock(&p.mutx)()
	p.nodes = nodes
}

// RefreshNodes uses a HELLO call to refresh the node list in the cluster
func (p *Pool) RefreshNodes() error {
	client, err := p.Get()
	if err != nil {
		return err
	}
	defer client.Close()

	resp, err := client.Hello()
	if err != nil {
		return err
	}

	// update the node list based on the hello response
	p.UpdateNodes(resp.Nodes)
	return nil
}

// RunRefreshLoop starts a goroutine that periodically refreshes the node list using HELLO
func (p *Pool) RunRefreshLoop() {

	go func() {
		for range time.Tick(refreshFrequency) {

			err := p.RefreshNodes()
			if err != nil {
				log.Println("disque pool: could not select client for refreshing")

			}

		}
	}()
}
