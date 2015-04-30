package tasque

import (
	"fmt"
	"testing"
	"time"
)

var counter = 0

func HandleIncrement(t *Task) error {

	v, found := t.Params["num"]
	if !found {
		panic("no num in params")
	}

	counter += int(v.(float64))
	return nil
}

func TestTasque(t *testing.T) {

	worker := NewWorker(4, "127.0.0.1:7711")

	worker.Handle(FuncHandler(HandleIncrement, "incr"))

	worker.Run()

	client := NewClient(time.Second, "127.0.0.1:7711")

	for i := 0; i < 100; i++ {
		task := NewTask("incr").Set("num", 10)
		err := client.Do(task)
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(500 * time.Millisecond)
	fmt.Println(counter)
	if counter == 0 {
		t.Errorf("Task did not run!")
	}
}
