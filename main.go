package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"strings"
)

var url string
var streamer *Streamer

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: jpegdemux bind http://stream/url")
		os.Exit(1)
	}
	bind := os.Args[1]
	url = os.Args[2]
	log.Println("Streaming:", url)

	streamer = NewStreamer()
	go streamer.Run()

	http.HandleFunc("/stream.jpg", streamHandler)
	http.HandleFunc("/snapshot.jpg", snapshotHandler)
	log.Fatal(http.ListenAndServe(bind, nil))
}

func snapshotHandler(wr http.ResponseWriter, req *http.Request) {
	log.Println("GET /snapshot.jpg from:", req.Header["X-Forwarded-For"])
	stream := streamer.GetStream()
	defer stream.Close()

	for part := range stream.Parts {
		wr.Header()["Content-Type"] = part.Header["Content-Type"]
		wr.WriteHeader(http.StatusOK)
		wr.Write(part.Body)
		break // just one
	}
}

func streamHandler(wr http.ResponseWriter, req *http.Request) {
	log.Println("GET /stream.jpg from:", req.Header["X-Forwarded-For"])
	stream := streamer.GetStream()
	defer stream.Close()
	writer := multipart.NewWriter(wr)
	contentType := fmt.Sprintf("multipart/x-mixed-replace;boundary=%s", writer.Boundary())
	wr.Header()["Content-Type"] = []string{contentType}
	wr.WriteHeader(http.StatusOK)
	for part := range stream.Parts {
		bodyWriter, err := writer.CreatePart(part.Header)
		if err != nil {
			break
		}
		_, err = bodyWriter.Write(part.Body)
		if err != nil {
			break
		}
		log.Printf("Wrote part to stream: %d bytes", len(part.Body))
	}
}

type Part struct {
	Header textproto.MIMEHeader
	Body   []byte
}

type Message struct {
	name     string
	argument interface{}
	response chan interface{}
}

type Stream struct {
	streamer *Streamer
	Parts    chan *Part
}

func (self *Stream) Close() {
	streamer.CloseStream(self)
}

type Streamer struct {
	C          chan Message
	streams    []*Stream
	connected  bool
	sourceBody io.ReadCloser
	parts      chan *Part
}

func NewStreamer() *Streamer {
	return &Streamer{
		C: make(chan Message, 1),
	}
}

func (self *Streamer) GetStream() *Stream {
	m := Message{name: "new", response: make(chan interface{}, 1)}
	self.C <- m
	r := <-m.response
	return r.(*Stream)
}

func (self *Streamer) CloseStream(stream *Stream) {
	m := Message{name: "close", argument: stream}
	self.C <- m
}

func (self *Streamer) processMessage(m Message) {
	switch m.name {
	case "new":
		stream := &Stream{Parts: make(chan *Part, 1)}
		self.streams = append(self.streams, stream)
		m.response <- stream
		log.Println("New stream")
		if !self.connected {
			self.connectSource()
		}
	case "close":
		log.Println("Closed stream")
		streams := []*Stream{}
		for _, stream := range self.streams {
			if stream != m.argument.(*Stream) {
				streams = append(streams, stream)
			}
		}
		self.streams = streams
		if len(streams) == 0 {
			self.disconnectSource()
		}
	case "sourceDropped":
		if self.connected {
			// attempt to reconnect
			self.connectSource()
		}
	}

}

func (self *Streamer) processPart(part *Part) {
	// forward onto all streams
	for _, stream := range self.streams {
		select {
		case stream.Parts <- part:
		}
	}
}

func (self *Streamer) Run() {
	for {
		select {
		case m := <-self.C:
			self.processMessage(m)
		case part := <-self.parts:
			self.processPart(part)
		}
	}
}

func (self *Streamer) disconnectSource() {
	if !self.connected {
		return
	}
	log.Println("Disconnecting source")
	self.sourceBody.Close()
	self.sourceBody = nil
	self.connected = false
}

func (self *Streamer) connectSource() {
	log.Println("Connecting to source")
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalln(err)
	}

	contentType := resp.Header["Content-Type"]
	parts := strings.Split(contentType[0], ";")
	mimeType := parts[0]
	if mimeType != "multipart/x-mixed-replace" {
		log.Fatalln("Mime type incorrect:", mimeType)
	}
	attributes := map[string]string{}
	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		kvs := strings.SplitN(part, "=", 2)
		if len(kvs) != 2 {
			continue
		}
		attributes[kvs[0]] = strings.Trim(kvs[1], `"`)
	}

	boundary := attributes["boundary"]
	if boundary == "" {
		log.Fatalln("Missing boundary in content-type:", contentType)
	}

	self.parts = make(chan *Part, 1)
	go func() {
		reader := multipart.NewReader(resp.Body, boundary)
		for {
			mpart, err := reader.NextPart()
			if err != nil {
				self.C <- Message{name: "sourceDropped", argument: err}
				return
			}

			body, _ := ioutil.ReadAll(mpart)
			part := &Part{
				Header: mpart.Header,
				Body:   body,
			}
			select {
			case self.parts <- part:
			}
		}
	}()
	self.connected = true
	self.sourceBody = resp.Body
}
