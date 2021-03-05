package main

import "fmt"

// SeenRequest wraps the string to check and the response channel.
type SeenRequest struct {
	text         string
	lineNo       int
	responseChan chan<- SeenResponse
}

// SeenResponse is what the SeenTracker responds with. If seen is True then
// lineNo will be set the line number at which it was first seen.
type SeenResponse struct {
	seen   bool
	lineNo int
}

// SeenTracker provides a threadsafe way to check if we've seen the text.
type SeenTracker struct {
	seen        map[string]int
	requestChan chan SeenRequest
}

func makeSeenTracker() SeenTracker {
	return SeenTracker{make(map[string]int), make(chan SeenRequest)}
}

// Start the tracker listening to the input channel.
func (s *SeenTracker) Start() {
	go func() {
		for req := range s.requestChan {
			if lineNo, ok := s.seen[req.text]; ok {
				req.responseChan <- SeenResponse{true, lineNo}
			} else {
				s.seen[req.text] = req.lineNo
				req.responseChan <- SeenResponse{false, 0}
			}
		}
	}()
}

// Check to see if the request exists.
func (s *SeenTracker) Check(text string, lineNo int) (err error) {
	respChannel := make(chan SeenResponse)
	s.requestChan <- SeenRequest{text, lineNo, respChannel}

	if resp := <-respChannel; resp.seen {
		return fmt.Errorf("%s has already been seen on line %d", text, resp.lineNo)
	}

	return nil
}
