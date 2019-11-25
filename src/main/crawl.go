
package main

import (
	"fmt"
)

type Fetcher interface {
	// Fetch 返回 URL 的 body 内容，并且将在这个页面上找到的 URL 放到一个 slice 中。
	Fetch(url string) (body string, urls []string, err error)
}



// Crawl 使用 fetcher 从某个 URL 开始递归的爬取页面，直到达到最大深度。
func Crawl(url string, urlsCh chan []string, fetcher Fetcher) {
	body, urls, err := fetcher.Fetch(url)
	fmt.Printf("found: %s %q\n", url, body)

	if err != nil {
		urlsCh <- []string{}
	} else {
		urlsCh <- urls
	}
}

func master(urlsCh chan []string, fetcher Fetcher) {
	urlFetchedM := make(map[string]interface{})
	n := 1
	for urls := range urlsCh {
		for _, url := range urls {
			if _, ok := urlFetchedM[url]; !ok {
				urlFetchedM[url] = struct {}{}
				n++
				go Crawl(url, urlsCh, fetcher)
			}
		}
		n--
		if n == 0 {
			break
		}
	}

}

func main() {
	urlsCh := make(chan []string)
	go func() {
		urlsCh <- []string{"https://golang.org/"}
	}()
	master(urlsCh, fetcher)

	}

// fakeFetcher 是返回若干结果的 Fetcher。
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher 是填充后的 fakeFetcher。
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}

