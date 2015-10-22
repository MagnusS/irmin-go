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
	"fmt"
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
	if res.StatusCode != 200 {
		return fmt.Errorf("Irmin HTTP server returned status %#v", res.Status)
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	c.log.Printf("returned: %s\n", body)

	return json.Unmarshal(body, v)
}

// CallStream connects to the given URL and returns a channel with responses until the stream is closed. The channel contains raw replies and must be unmarshaled by the caller.
func (c *Conn) CallStream(uri *url.URL, post *postRequest) (<-chan *streamReply, error) {
	var streamToken struct {
		Stream Value
	}
	var version struct {
		Version Value
	}

	var res *http.Response
	var err error

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
		return nil, err
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	go func() {
		wg.Wait() // close when all readers are done
		res.Body.Close()
	}()

	dec := json.NewDecoder(res.Body)
	var t interface{}
	if t, err = dec.Token(); err != nil { // read [ token
		return nil, err
	}
	switch t.(type) {
	case json.Delim:
		d := t.(json.Delim).String()
		if d != "[" {
			descr := fmt.Errorf("expected [, got %s", d) // If we are unable to unmarshal error msg, return this error
			// Invalid format. Try to unmarshal error value, in case it was returned outside the stream
			rest, err := ioutil.ReadAll(res.Body)
			if err != nil {
				return nil, err
			}
			buf, err := ioutil.ReadAll(dec.Buffered())
			all := append([]byte(d), append(buf, rest...)...)
			var errormsg ErrorVersion
			err = json.Unmarshal(all, &errormsg)
			if err != nil {
				return nil, descr
			}
			if errormsg.Error != nil {
				return nil, fmt.Errorf("Server returned an error: %s", errormsg.Error.String())
			}
			return nil, descr
		}
	default:
		err = fmt.Errorf("expected delimiter")
		return nil, err
	}

	err = dec.Decode(&streamToken)
	if err != nil || !bytes.Equal(streamToken.Stream, []byte("start")) { // look for stream start
		return nil, err
	}

	err = dec.Decode(&version)
	if err != nil {
		return nil, err
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
