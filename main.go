package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"
)

var (
	port = flag.String("port", "", "Port where server will starts")
)

// show cli usage
func usage() {
	flag.PrintDefaults()
	os.Exit(2)
}

//main struct
type queue struct {
	// data map
	data map[string][]string
	// receivers map
	receivers map[string][]chan string
	// quit channel
	quit chan os.Signal
}

// NewQueue returns new queue instance
func NewQueue() *queue {
	q := queue{
		data:      map[string][]string{},
		receivers: map[string][]chan string{},
		quit:      make(chan os.Signal),
	}

	//starts writer goroutine
	go q.writer()

	return &q
}

// main process to write data
func (q *queue) writer() {
	for {
		select {
		// quit if process exited
		case <-q.quit:
			return
		default:
			// check if there are waiting receivers
			for queueName, receivers := range q.receivers {
				// check if there are some data in this queue
				if dataQueue, ok := q.data[queueName]; ok && len(dataQueue) > 0 && len(receivers) > 0 {
					// send data to first receiver
					receivers[0] <- dataQueue[0]
					//remove receiver and data from list
					q.receivers[queueName] = q.receivers[queueName][1:]
					q.data[queueName] = q.data[queueName][1:]
					// delete empty map
					if len(q.receivers[queueName]) == 0 {
						delete(q.receivers, queueName)
					}
					if len(q.data[queueName]) == 0 {
						delete(q.data, queueName)
					}
				}
			}
		}
	}
}

// AddToQueue put some data to queue
func (q *queue) AddToQueue(queueName, value string) {
	q.data[queueName] = append(q.data[queueName], value)
}

// GetFromQueue get data from queue
func (q *queue) GetFromQueue(queueName string, timeout int) (string, error) {
	// user channel to receive data
	receiverChannel := make(chan string)

	// set timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer func() {
		cancel()
	}()

	// add user to receivers list
	q.receivers[queueName] = append(q.receivers[queueName], receiverChannel)

	select {
	// quit if process exited
	case <-q.quit:
		return "", errors.New("quit")
	// quit if timeout
	case <-ctx.Done():
		return "", errors.New("timeout")
	// quit if receive data
	case value := <-receiverChannel:
		return value, nil
	}
}

//HttpHandler main function to handle http requests
func (q *queue) HttpHandler(w http.ResponseWriter, r *http.Request) {
	// check request method
	if r.Method != "GET" && r.Method != "PUT" {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	// check url if there are queue name
	re := regexp.MustCompile(`^/(\w+)$`)
	result := re.FindStringSubmatch(r.URL.Path)
	if len(result) != 2 {
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		//check timeout param
		timeoutStr := r.URL.Query().Get("timeout")
		var timeout int
		// if not set default timeout
		timeout, err := strconv.Atoi(timeoutStr)
		if err != nil {
			timeout = 60 * 60 * 12
		}

		value, err := q.GetFromQueue(result[1], timeout)
		if err != nil {
			http.Error(w, "", http.StatusNotFound)
			return
		}

		fmt.Fprint(w, value)
	case "PUT":
		//check v param
		value := r.URL.Query().Get("v")
		if value == "" {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		q.AddToQueue(result[1], value)
	}

	return
}

func main() {
	flag.Parse()
	// check port flag if empty show hw to use cli
	if *port == "" {
		log.Printf("missing argument port")
		usage()
	}

	quitChannel := make(chan os.Signal)
	//Handle os signals to shut down goroutines
	signal.Notify(quitChannel, os.Interrupt, syscall.SIGTERM)
	// new queue instance
	q := NewQueue()

	srv := &http.Server{
		Addr: ":" + *port,
	}
	http.HandleFunc("/", q.HttpHandler)

	fmt.Printf("Server starts on :%s\n", *port)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer func() {
		cancel()
	}()
	//wait shutdown
	<-quitChannel
	// close all goroutines
	close(q.quit)
	// shutdown server
	srv.Shutdown(ctx)
}
