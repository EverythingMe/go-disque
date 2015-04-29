# DisqChan - distributed channels over Disque

This is an example project with a higher level abstraction over go-disque.

This library provides a disque-based networked channels, on which you can send and receive objects between machines.
Messages are encoded as JSON and deserialized when received.

## Example Usage:

### Sending messages:

```go

func ExampleSend() {

	sc := disqchan.NewChan("mychan", false, "127.0.0.1:7711")
	defer sc.Stop()

	ch := sc.SendChan()
	for i := 0; i < 100; i++ {
		ch <- fmt.Sprintf("Message %d", i)
	}

}

```

### Receiving Messages:

```go

func ExampleRecv() {

	c := NewChan("mychan", false, "127.0.0.1:7711")

	rch := c.RecvChan()

	i := 0
	for v := range rch {

		fmt.Println("Received: ", v)
		i++
		if i == 10 {
			fmt.Println("finished")
			break
		}
	}
}

```