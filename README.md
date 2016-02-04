# go-disque

A simple Go client for the Disque in-memory distributed queue https://github.com/antirez/disque

## Example:

```go

import (
	"fmt"
	"time"
	
	"github.com/garyburd/redigo/redis"
	"github.com/EverythingMe/go-disque/disque"
)

func dial(addr string) (redis.Conn, error) {
	return redis.Dial("tcp", addr)
}

func ExampleClient() {

	pool := disque.NewPool(disque.DialFunc(dial), "127.0.0.1:7711", "127.0.0.1:7712")

	client, err := pool.Get()
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


# Tasque
## Disque based remote task execution queue for Go

Taskque levereges Disque (https://github.com/antirez/disque) to create a simple and easy to use
distributed task execution queue.

The idea is simple - you creat TaskHandlers - callbacks that receive Tasks - which are simple execution
context objects. Then you run a Worker process that can handle multiple TaskHandlers by name. You can then 
enqueue tasks for the handlers from any machine in your cluster using a Client, and they get executed.

## Example - Creating a Worker


```go

import (
	"github.com/EverythingMe/go-disque/tasque"
	
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// Step 1: Define a handler that has a unique name
var Downloader = tasque.FuncHandler(func(t *tasque.Task) error {

	u := t.Params["url"].(string)
	res, err := http.Get(u)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	fp, err := os.Create(fmt.Sprintf("/tmp/%x", md5.Sum([]byte(u))))
	if err != nil {
		return err
	}
	defer fp.Close()

	if _, err := io.Copy(fp, res.Body); err != nil {
		return err
	}
	fmt.Printf("Downloaded %s successfully\n", u)

	return nil
	
}, "download")



// Step 2: Registering the handler and starting a Worker

func main() {
	
	// Worker with 10 concurrent goroutines. In real life scenarios set this to much higher values...
	worker := tasque.NewWorker(10, "127.0.0.1:7711")

	// register our downloader
	worker.Handle(Downloader)
	
	// Run the worker
	worker.Run()

}


```



# Example - Enqueuing a task

```go

func main() {
	
	client := tasque.NewClient(5*time.Second, "127.0.0.1:7711")

	task := tasque.NewTask(Downloader.Id()).Set("url", "http://google.com")
	err := client.Do(task)
	if err != nil {
		panic(err)
	}
}
```
