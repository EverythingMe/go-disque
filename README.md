# go-disque

A simlpe Go client for the Disque in-memory distributed queue https://github.com/antirez/disque

## Example:

```go

func ExampleClient() {

	client, err := disque.Dial("tcp", time.Second, addr)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	qname := "test1"

	// Create an "add" request with optional parameters.
	// TODO: create a builder-style API for this
	ja := disque.AddRequest{
		Job: disque.Job{
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

```