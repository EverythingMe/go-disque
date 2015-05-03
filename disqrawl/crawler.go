package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/EverythingMe/go-disque/disqchan"
	"github.com/EverythingMe/go-disque/tasque"
	"github.com/PuerkitoBio/goquery"
)

type CrawlHandler struct {
	seenUrls map[string]struct{}
	lock     sync.RWMutex
	dir      string
	ch       *disqchan.Chan
}

func NewCrawlHandler(persistDir string, addrs ...string) *CrawlHandler {
	return &CrawlHandler{
		seenUrls: make(map[string]struct{}),
		lock:     sync.RWMutex{},
		dir:      persistDir,
		ch:       disqchan.NewChan("found_urls", false, addrs...),
	}
}

func (c CrawlHandler) Id() string {
	return "crawl"
}

func (c *CrawlHandler) seen(u *url.URL) bool {
	c.lock.RLock()

	_, found := c.seenUrls[u.String()]
	c.lock.RUnlock()
	if found {
		return true
	}

	c.lock.Lock()
	c.seenUrls[u.String()] = struct{}{}
	c.lock.Unlock()
	return false
}

func (c *CrawlHandler) crawl(u string) ([]string, error) {
	baseUrl, err := url.Parse(u)
	_ = baseUrl

	if err != nil {
		log.Printf("Error parseing url %s: %s", u, err)
		return nil, err
	}

	res, err := http.Get(u)
	if err != nil {
		log.Println("Error downloading %s: %s", u, err)
		return nil, err
	}

	fp, err := os.Create(fmt.Sprintf("/%s/%x", c.dir, md5.Sum([]byte(u))))
	if err != nil {
		log.Println("Error persisting %s: %s", u, err)
		return nil, err
	}

	if _, err := fp.WriteString(u); err != nil {
		return nil, err
	}
	if _, err := fp.WriteString("\n\n"); err != nil {
		return nil, err
	}

	doc, err := goquery.NewDocumentFromReader(io.TeeReader(res.Body, fp))
	if err != nil {
		log.Printf("Error crawling %s: %s", u, err)
		return nil, err
	}

	fp.Close()
	res.Body.Close()

	urls := make([]string, 0, 20)

	doc.Find("a").Each(func(i int, s *goquery.Selection) {

		if href, found := s.Attr("href"); found {

			if !strings.HasPrefix(href, "javascript:") {
				link, err := url.Parse(href)
				if err == nil {

					link = baseUrl.ResolveReference(link)
					if !c.seen(link) {
						urls = append(urls, link.String())
					}

				} else {
					log.Println(err)
				}
			}

		}

	})

	return urls, nil
}

func (c *CrawlHandler) Handle(t *tasque.Task) error {

	u, found := t.Params["url"]
	if !found {
		return fmt.Errorf("No url in task")
	}

	ch := c.ch.SendChan()
	if url, ok := u.(string); !ok {
		return fmt.Errorf("Invalid valid for url: %s", u)
	} else {
		urls, err := c.crawl(url)
		if err != nil {
			return err
		}
		log.Printf("Got %d urls for %s", len(urls), url)

		for _, u := range urls {
			ch <- u
		}
		return nil
	}

}

func crawlManager(nodes []string) {

	seen := map[interface{}]struct{}{}

	ch := disqchan.NewChan("found_urls", true, nodes...)
	client := tasque.NewClient(5*time.Second, nodes...)

	tasque.NewTask("crawl").Set("url", "http://news.google.com").Do(client)

	rc := ch.RecvChan()
	i := 0
	for href := range rc {
		i++
		if i%100 == 0 {
			fmt.Println("%d (%d) urls seen", i, len(seen))
		}

		_, found := seen[href]
		if found {
			continue
		}
		seen[href] = struct{}{}

		//fmt.Println(href)
		tasque.NewTask("crawl").Set("url", href).Do(client)
	}

}
func main() {

	crawl := flag.Bool("crawler", false, "if set, we act as a crawler")
	qnodes := flag.String("qnodes", "127.0.0.1:7711", "Comma separated list of disque nodes")
	workers := flag.Int("workers", 5, "number of concurrent workers")

	flag.Parse()

	nodes := strings.Split(*qnodes, ",")
	fmt.Println("Nodes: ", nodes)

	if *crawl {
		c := NewCrawlHandler("/tmp/crawls", nodes...)

		w := tasque.NewWorker(*workers, nodes...)
		w.Handle(c)
		w.Run()
	} else {
		crawlManager(nodes)
	}

	//fmt.Println(c.crawl("http://ynet.co.il"))
}
