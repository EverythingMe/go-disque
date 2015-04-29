package disque

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type NodeList []Node

func (l NodeList) contains(n Node) bool {
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

func (n Node) IsNull() bool {
	return n.Addr == ""
}

type DialFunc func(string) (redis.Conn, error)
type Pool struct {
	mutx     sync.Mutex
	nodes    NodeList
	pools    map[string]*redis.Pool
	dialFunc DialFunc
}

func NewPool(f DialFunc, addrs ...string) *Pool {

	rand.Seed(time.Now().UnixNano())
	nodes := NodeList{}
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

func (p *Pool) selectNode(selected NodeList) (Node, error) {

	defer scopedLock(&p.mutx)()

	if len(p.nodes) == 0 {
		return Node{}, errors.New("disque: no nodes in pool")
	}

	maxPriority := 1 //TMP - we only handle nodes with priority 1 right now
	nodes := NodeList{}
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
	maxIdle          = 3
	refreshFrequency = time.Minute
)

func (p *Pool) getPool(addr string) *redis.Pool {

	defer scopedLock(&p.mutx)()

	pool, found := p.pools[addr]
	if !found {
		pool = redis.NewPool(func() (redis.Conn, error) {
			return p.dialFunc(addr)
		}, maxIdle)

		p.pools[addr] = pool
	}

	return pool

}

func (p *Pool) Get() (Client, error) {

	selected := NodeList{}
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
func (p *Pool) Put(c Client) {
	c.(*RedisClient).conn.Close()
}

func (p *Pool) UpdateNodes(nodes NodeList) {
	defer scopedLock(&p.mutx)()
	p.nodes = nodes
}

func (p *Pool) RunRefreshLoop() {

	go func() {
		for range time.Tick(refreshFrequency) {

			client, err := p.Get()
			if err != nil {
				log.Println("disque pool: could not select client for refreshing")
				continue
			}

			resp, err := client.Hello()
			if err != nil {
				log.Println("disque pool: error getting hello: %s", err)
				continue
			}

			// update the node list based on the hello response
			p.UpdateNodes(resp.Nodes)

		}
	}()
}
