package disque

import (
	"fmt"
	"testing"
	"time"
)

const addr = "localhost:7711"

func TestAddJob(t *testing.T) {

	client, err := Dial("tcp", time.Second, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ja := AddRequest{
		Job: Job{
			Queue: "test",
			Data:  []byte("foo"),
		},
		Timeout: time.Millisecond * 100,
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

func ExampleClient() {

	client, err := Dial("tcp", time.Second, addr)
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
		Timeout: time.Millisecond * 100,
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
func TestClient(t *testing.T) {
	client, err := Dial("tcp", time.Second, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	qname := "test1"
	ja := AddRequest{
		Job: Job{
			Queue: qname,
			Data:  []byte("foo"),
		},
		Timeout: time.Millisecond * 100,
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
