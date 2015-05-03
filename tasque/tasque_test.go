package tasque

import (
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

var counter = 0

var IncrementHandler = FuncHandler(func(t *Task) error {

	v, found := t.Params["num"]
	if !found {
		panic("no num in params")
	}

	counter += int(v.(float64))
	return nil
}, "incr")

func TestTasque(t *testing.T) {

	worker := NewWorker(4, "127.0.0.1:7711")

	worker.Handle(IncrementHandler)

	worker.Run()

	client := NewClient(5*time.Second, "127.0.0.1:7711")

	for i := 0; i < 100; i++ {
		task := NewTask(IncrementHandler.Id()).Set("num", 10)
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

var Downloader = FuncHandler(func(t *Task) error {

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

func Example() {

	worker := NewWorker(4, "127.0.0.1:7711")

	worker.Handle(Downloader)

	worker.Run()

	client := NewClient(5*time.Second, "127.0.0.1:7711")

	task := NewTask(Downloader.Id()).Set("url", "http://google.com")
	err := client.Do(task)
	if err != nil {
		panic(err)
	}

	time.Sleep(2000 * time.Millisecond)
	// Output:
	// Downloaded http://google.com successfully
}
