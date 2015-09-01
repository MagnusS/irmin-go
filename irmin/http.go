/*
 Copyright (c) 2015 Magnus Skjegstad <magnus@skjegstad.com>

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
	"unicode/utf8"
)

type SubCommandType int

const (
	COMMAND_PLAIN SubCommandType = iota
	COMMAND_TREE
	COMMAND_TAG
)

type StringArrayReply struct {
	Result  []IrminString
	Error   IrminString
	Version IrminString
}

type StringReply struct {
	Result  IrminString
	Error   IrminString
	Version IrminString
}

type PathArrayReply struct {
	Result  []IrminPath
	Error   IrminString
	Version IrminString
}

type BoolReply struct {
	Result  bool
	Error   IrminString
	Version IrminString
}

type Task struct {
	Date     string        `json:"date"`
	Uid      string        `json:"uid"`
	Owner    IrminString   `json:"owner"`
	Messages []IrminString `json:"messages"`
}

type BodyRequest struct {
	Task Task        `json:"task"`
	Body IrminString `json:"params"`
}

type CommandsReply StringArrayReply
type ListReply PathArrayReply
type MemReply BoolReply
type ReadReply StringArrayReply
type CloneReply StringReply
type UpdateReply StringReply
type RemoveReply StringReply
type RemoveRecReply StringReply

type StreamReply struct {
	Error  IrminString
	Result json.RawMessage
}

type RestConn struct {
	base_uri  *url.URL
	tree      string
	taskowner string
}

// Create an Irmin REST HTTP connection data structure
func Create(uri url.URL) *RestConn {
	r := new(RestConn)
	r.base_uri = &uri
	return r
}

// Set tree position used for Tree sub-commands. Empty defaults to master
func (rest *RestConn) SetTree(tree string) {
	rest.tree = tree
}

// Read the current tree position use for Tree sub-commands. Empty defaults to master.
func (rest *RestConn) Tree() string {
	return rest.tree
}

func (rest *RestConn) TaskOwner() string {
	return rest.taskowner
}

// Create invocation URL for a command with an optional sub command type (typically COMMAND_TAG or COMMAND_TREE).
// Note that the commands generally applies to master or head respectively if Tree() is not set in the data structure
func (rest *RestConn) MakeCallUrl(ct SubCommandType, command string, path IrminPath) (*url.URL, error) {
	var suffix *url.URL
	var err error

	p := path.URL()

	var parent_command string
	var parent_param string

	switch ct {
	case COMMAND_PLAIN:
		parent_command = ""
		parent_param = ""
	case COMMAND_TREE:
		if rest.Tree() != "" { // Ignore the parameter if Tree is not set
			parent_command = "tree"
			parent_param = rest.Tree()
		}
	case COMMAND_TAG:
		parent_command = "tag"
		parent_param = ""
	default:
		return nil, fmt.Errorf("unknown command type %d", ct)
	}

	if parent_command == "" {
		if suffix, err = url.Parse(fmt.Sprintf("/%s%s", url.QueryEscape(command), p.String())); err != nil {
			return nil, err
		}
	} else {
		if suffix, err = url.Parse(fmt.Sprintf("/%s/%s/%s/%s%s", url.QueryEscape(parent_command), url.QueryEscape(parent_param), url.QueryEscape(command), p.String())); err != nil {
			return nil, err
		}
	}

	return rest.base_uri.ResolveReference(suffix), nil
}

func (rest *RestConn) runCommand(ct SubCommandType, command string, path IrminPath, post *bytes.Buffer, v interface{}) (err error) {
	uri, err := rest.MakeCallUrl(ct, command, path)
	if err != nil {
		return
	}
	var res *http.Response
	if post == nil {
		res, err = http.Get(uri.String())
	} else {
		t := Task{"0", "0", NewIrminString("irminowner"), []IrminString{NewIrminString("message")}}
		//p := []IrminString{NewIrminString("testparam")}
		p := NewIrminString("testparam")
		x := BodyRequest{t, p}
		j, err := json.Marshal(&x)
		if err != nil {
			panic(err)
		}
		fmt.Printf("body %s\n", j)
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

	return json.Unmarshal(body, v)
}

// Run the specified command and return a channel with responses until the stream is closed. The channel contains raw replies and must be unmarshaled by the caller.
func (rest *RestConn) runStreamCommand(ct SubCommandType, command string, path IrminPath, post *bytes.Buffer) (_ <-chan *StreamReply, err error) {
	var stream_token struct {
		Stream IrminString
	}
	var version struct {
		Version IrminString
	}

	uri, err := rest.MakeCallUrl(ct, command, path)
	if err != nil {
		return
	}

	var res *http.Response

	if post == nil {
		res, err = http.Get(uri.String())
	} else {
		res, err = http.Post(uri.String(), "application/json", post)
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

	err = dec.Decode(&stream_token)
	if err != nil || !bytes.Equal(stream_token.Stream, []byte("start")) { // look for stream start
		return
	}

	err = dec.Decode(&version)
	if err != nil {
		return
	}

	ch := make(chan *StreamReply, 100)
	wg.Add(1)
	go func() {
		defer func() {
			close(ch)
			wg.Done()
		}()

		for dec.More() {
			s := new(StreamReply)
			if err = dec.Decode(s); err != nil {
				return
			}
			if len(s.Result) == 0 { // If result is empty, look for stream end
				if err = dec.Decode(&stream_token); err != nil || bytes.Equal(stream_token.Stream, []byte("end")) { // look for stream end
					return
				}
			}
			ch <- s
		}
	}()
	return ch, nil
}

func (rest *RestConn) AvailableCommands() ([]string, error) {
	var data CommandsReply
	var err error
	if err = rest.runCommand(COMMAND_TREE, "", IrminPath{}, nil, &data); err != nil {
		return []string{}, err
	}
	if data.Error.String() != "" {
		return []string{}, fmt.Errorf(data.Error.String())
	}

	r := make([]string, len(data.Result))
	for i, v := range data.Result {
		r[i] = v.String()
	}
	return r, nil
}

func (rest *RestConn) Version() (string, error) {
	var data CommandsReply
	var err error
	if err = rest.runCommand(COMMAND_TREE, "", IrminPath{}, nil, &data); err != nil {
		return "", err
	}
	if data.Error.String() != "" {
		return "", fmt.Errorf(data.Error.String())
	}

	return data.Version.String(), nil
}

func (rest *RestConn) List(path IrminPath) ([]IrminPath, error) {
	var data ListReply
	var err error
	if err = rest.runCommand(COMMAND_TREE, "list", path, nil, &data); err != nil {
		return []IrminPath{}, err
	}
	if data.Error.String() != "" {
		return []IrminPath{}, fmt.Errorf(data.Error.String())
	}

	return data.Result, nil
}

func (rest *RestConn) Mem(path IrminPath) (bool, error) {
	var data MemReply
	var err error
	err = rest.runCommand(COMMAND_TREE, "mem", path, nil, &data)
	if err != nil {
		return false, err
	}
	if data.Error.String() != "" {
		return false, fmt.Errorf(data.Error.String())
	}
	return data.Result, nil
}

func (rest *RestConn) Read(path IrminPath) ([]byte, error) {
	var data ReadReply
	var err error
	if err = rest.runCommand(COMMAND_TREE, "read", path, nil, &data); err != nil {
		return []byte{}, err
	}
	if data.Error.String() != "" {
		return []byte{}, fmt.Errorf(data.Error.String())
	}
	if len(data.Result) > 1 {
		return []byte{}, fmt.Errorf("read %s returned more than one result", path.String())
	}
	if len(data.Result) == 1 {
		return data.Result[0], nil
	} else {
		return []byte{}, fmt.Errorf("invalid key %s", path.String())
	}
}

func (rest *RestConn) ReadString(path IrminPath) (string, error) {
	res, err := rest.Read(path)
	if err != nil {
		return "", err
	}
	if utf8.Valid(res) {
		return string(res), nil
	} else {
		return "", fmt.Errorf("path %s does not contain a valid utf8 string", path.String())
	}
}

// Update a key. Returns hash as string on success.
func (rest *RestConn) Update(path IrminPath, content *bytes.Buffer) (string, error) {
	// TODO This command should return the hash on success
	var data UpdateReply
	var err error
	if err = rest.runCommand(COMMAND_TREE, "update", path, content, &data); err != nil {
		return data.Result.String(), err
	}
	if data.Error.String() != "" {
		return "", fmt.Errorf(data.Error.String())
	}
	if data.Result.String() == "" {
		return "", fmt.Errorf("update seemed to succeed, but didn't return a hash", path.String(), data.Result.String())
	}

	return data.Result.String(), nil
}

/* Remove file */
func (rest *RestConn) Remove(path IrminPath) error {
	var data RemoveReply
	var err error
	if err = rest.runCommand(COMMAND_TREE, "remove", path, nil, &data); err != nil {
		return err
	}
	if data.Error.String() != "" {
		return fmt.Errorf(data.Error.String())
	}
	if len(data.Result) > 1 {
		return fmt.Errorf("remove %s returned more than one result", path.String())
	}

	return nil
}

/* Remove directory */
func (rest *RestConn) RemoveRec(path IrminPath) error {
	var data RemoveReply
	var err error
	if err = rest.runCommand(COMMAND_TREE, "remove-rec", path, nil, &data); err != nil {
		return err
	}
	if data.Error.String() != "" {
		return fmt.Errorf(data.Error.String())
	}
	if len(data.Result) > 1 {
		return fmt.Errorf("remove %s returned more than one result", path.String())
	}

	return nil
}

func (rest *RestConn) Iter() (<-chan *IrminPath, error) {
	var ch <-chan *StreamReply
	var err error
	if ch, err = rest.runStreamCommand(COMMAND_TREE, "iter", IrminPath{}, nil); err != nil || ch == nil {
		return nil, err
	}

	out := make(chan *IrminPath, 1)

	go func() {
		defer close(out)
		for m := range ch {
			p := new(IrminPath)
			if err := json.Unmarshal(m.Result, &p); err != nil {
				panic(err) // TODO This should be returned to caller
			}
			out <- p
		}
	}()

	return out, err
}

func (rest *RestConn) Clone(name string, force bool) error {
	var data CloneReply
	var err error
	path, err := ParseEncodedPath(url.QueryEscape(name)) // encode and wrap in IrminPath
	if err != nil {
		return err
	}
	command := "clone"
	if force {
		command = "clone-force"
	}
	if err = rest.runCommand(COMMAND_TREE, command, path, nil, &data); err != nil {
		return err
	}
	if data.Error.String() != "" {
		return fmt.Errorf(data.Error.String())
	}
	if len(data.Result) > 1 {
		return fmt.Errorf("%s %s returned more than one result", command, name)
	}
	if (data.Result.String() != "ok") || (data.Result.String() == "" && force) {
		return fmt.Errorf(data.Result.String())
	}

	return nil
}
