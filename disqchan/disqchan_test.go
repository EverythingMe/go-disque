package disqchan

import (
	"fmt"
	"testing"
	"time"
)

func TestChan(t *testing.T) {

	// singular channel id
	name := fmt.Sprintf("testung%d", time.Now().UnixNano())
	N := 100

	sc := NewChan(name, false, "127.0.0.1:7711")
	defer sc.Stop()

	ch := sc.SendChan()
	for i := 0; i < N; i++ {
		ch <- fmt.Sprintf("Message %d", i)
	}

	n := 0
	waitch := make(chan bool)
	go func() {
		rc := NewChan(name, false, "127.0.0.1:7711")
		defer rc.Stop()
		rch := rc.RecvChan()

		for v := range rch {

			if _, ok := v.(string); !ok {
				t.Errorf("Received bad value: %v", v)
			}
			n++
			fmt.Println("RECEIVED: ", v, n)
			if n >= N {
				break
			}
		}
		fmt.Println("FINISHED")
		waitch <- true
	}()

	select {
	case <-waitch:
	case <-time.After(5 * time.Second):
	}
	if n < N {
		t.Fatal("Did not finish receiving within the time frame!")
	}

}

func ExampleSend() {

	sc := NewChan("mychan", false, "127.0.0.1:7711")
	defer sc.Stop()

	ch := sc.SendChan()
	for i := 0; i < 100; i++ {
		ch <- fmt.Sprintf("Message %d", i)
	}

	// Output:
}

func ExampleRecv() {

	c := NewChan("mychan", false, "127.0.0.1:7711")
	rch := c.RecvChan()

	i := 0
	for _ := range rch {

		i++
		if i == 10 {
			fmt.Println("finished")
			break
		}

	}

	// Output:
	// finished

}
