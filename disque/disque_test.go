package disque

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

const addr = "127.0.0.1:7711"

func dial(addr string) (redis.Conn, error) {
	timeout := time.Second
	return redis.DialTimeout("tcp", addr, timeout, timeout, timeout)
}
func TestAddJob(t *testing.T) {

	pool := NewPool(DialFunc(dial), addr)

	client, err := pool.Get()
	if err != nil || client == nil {
		panic("could not get client" + err.Error())
	}
	defer client.Close()

	ja := AddRequest{
		Job: Job{
			Queue: "test",
			Data:  []byte("foo"),
		},
		Timeout:   time.Millisecond * 100,
		Replicate: pool.Size(),
	}

	id, err := client.Add(ja)
	if err != nil {
		t.Fatal(err)
	}

	if id == "" {
		t.Errorf("Invalid id")
	}

	fmt.Println("id: ", id)
	//TODO: Add more edge case tests
}

func TestAddMulti(t *testing.T) {
	pool := NewPool(DialFunc(dial), addr)

	client, err := pool.Get()
	if err != nil || client == nil {
		panic("could not get client" + err.Error())
	}
	defer client.Close()

	ja := AddRequest{
		Job: Job{
			Queue: "test",
			Data:  []byte("foo"),
		},
	}

	ids, err := client.AddMulti([]AddRequest{ja})
	if err != nil {
		t.Fatal(err)
	}

	if len(ids) != 1 {
		t.Errorf("expected 1 id in response. got %d", len(ids))
	}

	if ids[0] == "" {
		t.Errorf("Invalid id")
	}
}

func ExampleClient() {

	pool := NewPool(DialFunc(dial), addr)
	client, err := pool.Get()
	if err != nil {
		panic(err)
	}
	defer client.Close()

	qname := "test1"

	// Create an "add" request with optional parameters.
	// TODO: create a builder-style API for this
	ja := AddRequest{
		Job: Job{
			Queue: qname,
			Data:  []byte("foo"),
		},
		Timeout:   time.Millisecond * 100,
		Replicate: pool.Size(),
	}

	// Add the job to the queue
	if _, err := client.Add(ja); err != nil {
		panic(err)
	}

	job, err := client.Get(time.Second, qname)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(job.Data))
	// Output:
	// foo
}

func TestPool(t *testing.T) {
	pool := NewPool(DialFunc(dial), addr)

	client, err := pool.Get()
	if err != nil || client == nil {
		panic("could not get client" + err.Error())
	}
	client.Close()

	client2, err := pool.Get()
	if err != nil || client2 == nil {
		panic("could not get client" + err.Error())
	}

	if pool.numBorrowed != 1 {
		t.Errorf("WE should have borrowed the connection, but didn't")
	}
	if client, err = pool.Get(); err != nil || client == nil {
		panic("could not get client" + err.Error())
	}
	if pool.numBorrowed != 1 {
		t.Errorf("WE didn't return a pooled connection and called GET again, this shouldn't have borrowed a connection")
	}

	client2.Close()

}
func TestHello(t *testing.T) {

	pool := NewPool(DialFunc(dial), addr)

	client, err := pool.Get()
	if err != nil || client == nil {
		panic("could not get client" + err.Error())
	}
	defer client.Close()

	ret, err := client.Hello()
	if err != nil {
		t.Fatal(err)
	}

	if ret.NodeId == "" {
		t.Errorf("No node id in hello response")
	}

	if len(ret.Nodes) == 0 {
		t.Errorf("No nodes returned from hello")
	}

	for _, node := range ret.Nodes {
		if node.Id == "" {
			t.Errorf("Node with empty id given")
		}

		if _, err := net.ResolveTCPAddr("tcp", node.Addr); err != nil {
			t.Errorf("Invalid address given: %s", node.Addr)
		}

		fmt.Println(node)

	}

	selected := nodeList{}
	node, err := pool.selectNode(selected)
	if err != nil {
		t.Fatal(err)
	}
	if node.Id != "" {
		t.Errorf("at this stage node ids should be empty. got %s", node.Id)
	}

	// test updating node pool
	pool.UpdateNodes(ret.Nodes)
	fmt.Println(pool.nodes)

	if node, err = pool.selectNode(selected); err != nil {
		t.Fatal(err)
	}

	if node.Id == "" {
		t.Errorf("at this stage node ids should NOT be empty. got %s", node.Id)
	}
	fmt.Println("selected: ", node)

}
func TestClient(t *testing.T) {
	pool := NewPool(DialFunc(dial), addr)

	client, err := pool.Get()
	if err != nil || client == nil {
		panic("could not get client" + err.Error())
	}
	defer client.Close()
	qname := "test1"
	ja := AddRequest{
		Job: Job{
			Queue: qname,
			Data:  []byte("foo"),
		},
		Timeout:   time.Millisecond * 100,
		Replicate: pool.Size(),
	}

	id, err := client.Add(ja)
	if err != nil {
		t.Fatal(err)
	}

	if l, err := client.Qlen(qname); err != nil {
		t.Error(err)
	} else if l != 1 {
		t.Errorf("Wrong queue len. expected 1, got %d", l)
	}

	job, err := client.Get(time.Second, qname)
	if err != nil {
		t.Error(err)
	}

	if l, err := client.Qlen(qname); err != nil {
		t.Error(err)
	} else if l != 0 {
		t.Errorf("Wrong queue len. expected 0, got %d", l)
	}

	if job.Id() != id {
		t.Errorf("Expected id %s, got %s", id, job.Id())
	}

	if string(job.Data) != string(ja.Job.Data) {
		t.Errorf("Wrong job data. got %s, expected %s", job.Data, ja.Job.Data)
	}

	if job.Queue != ja.Job.Queue {
		t.Errorf("Got wrong queue name, expected %s, got %s", ja.Job.Queue, job.Queue)
	}
	if err = client.Ack(job.Id()); err != nil {
		t.Errorf("Error ACKing: %s", err)
	}

}

func BenchmarkAdd(b *testing.B) {

	pool := NewPool(DialFunc(dial), addr)

	client, err := pool.Get()
	if err != nil || client == nil {
		panic("could not get client" + err.Error())
	}
	defer client.Close()

	ja := AddRequest{
		Job: Job{
			Queue: "bq",
			Data:  []byte("foo"),
		},
		Timeout:   time.Millisecond * 100,
		Replicate: pool.Size(),
		Async:     false,
	}

	for i := 0; i < b.N; i++ {

		_, err := client.Add(ja)
		if err != nil {
			b.Error(err)
		}
	}
}
