/*
 Copyright (c) 2015 Magnus Skjegstad <magnus.skjegstad@unikernel.com>

 Permission to use, copy, modify, and distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.

 THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

package irmin

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
)

type client struct {
	baseURI *url.URL
	log     Log
}

type streamReply struct {
	Error  Value
	Result json.RawMessage
}

func NewClient(uri *url.URL, log Log) *client {
	return &client{uri, log}
}

// Call connects to the specified URL and attempts to unmarshal the reply. The result is stored in v.
func (c *client) Call(uri *url.URL, post *postRequest, v interface{}) (err error) {
	c.log.Printf("calling: %s\n", uri.String())
	var res *http.Response
	if post == nil {
		res, err = http.Get(uri.String())
	} else {
		j, err := json.Marshal(post)
		if err != nil {
			panic(err)
		}
		c.log.Printf("post body: %s\n", j)
		res, err = http.Post(uri.String(), "application/json", bytes.NewBuffer(j))
	}
	if err != nil {
		return
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	c.log.Printf("returned: %s\n", body)

	return json.Unmarshal(body, v)
}

// CallStream connects to the given URL and returns a channel with responses until the stream is closed. The channel contains raw replies and must be unmarshaled by the caller.
func (c *Conn) CallStream(uri *url.URL, post *postRequest) (_ <-chan *streamReply, err error) {
	var streamToken struct {
		Stream Value
	}
	var version struct {
		Version Value
	}

	var res *http.Response

	if post == nil {
		res, err = http.Get(uri.String())
	} else {
		j, err := json.Marshal(post)
		if err != nil {
			panic(err)
		}
		res, err = http.Post(uri.String(), "application/json", bytes.NewBuffer(j))
	}
	if err != nil {
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	go func() {
		wg.Wait() // close when all readers are done
		res.Body.Close()
	}()

	dec := json.NewDecoder(res.Body)
	if _, err = dec.Token(); err != nil { // read [ token
		return
	}

	err = dec.Decode(&streamToken)
	if err != nil || !bytes.Equal(streamToken.Stream, []byte("start")) { // look for stream start
		return
	}

	err = dec.Decode(&version)
	if err != nil {
		return
	}

	ch := make(chan *streamReply, 100)
	wg.Add(1)
	go func() {
		defer func() {
			close(ch)
			wg.Done()
		}()

		for dec.More() {
			s := new(streamReply)
			if err = dec.Decode(s); err != nil {
				return
			}
			if len(s.Result) == 0 { // If result is empty, look for stream end
				if err = dec.Decode(&streamToken); err != nil || bytes.Equal(streamToken.Stream, []byte("end")) { // look for stream end
					return
				}
			}
			ch <- s
		}
	}()
	return ch, nil
}
