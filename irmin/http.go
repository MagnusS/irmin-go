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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"
	"unicode/utf8"
)

// SubCommandType describes the type of command URL created by MakeCallURL
type SubCommandType int

const (
	// CommandNormal - command is appended to base url
	CommandNormal SubCommandType = iota
	// CommandTree - command is executed in the context of a tree
	CommandTree
)

type stringArrayReply struct {
	Result  []Value
	Error   Value
	Version Value
}

type stringReply struct {
	Result  Value
	Error   Value
	Version Value
}

type pathArrayReply struct {
	Result  []Path
	Error   Value
	Version Value
}

type boolReply struct {
	Result  bool
	Error   Value
	Version Value
}

// Task describes the commit message stored in Irmin
type Task struct {
	Date     string  `json:"date"`
	UID      string  `json:"uid"`
	Owner    Value   `json:"owner"`
	Messages []Value `json:"messages"`
}

type postRequest struct {
	Task Task            `json:"task"`
	Data json.RawMessage `json:"params,omitempty"`
}

type commandsReply stringArrayReply
type listReply pathArrayReply
type memReply boolReply
type readReply stringArrayReply
type cloneReply stringReply
type updateReply stringReply
type removeReply stringReply
type removeRecReply stringReply
type headReply stringArrayReply

type streamReply struct {
	Error  Value
	Result json.RawMessage
}

// Conn is an Irmin REST API connection
type Conn struct {
	baseURI   *url.URL
	tree      string
	taskowner string
	log       Log
}

// Create an Irmin REST HTTP connection data structure
func Create(uri *url.URL, taskowner string) *Conn {
	r := new(Conn)
	r.baseURI = uri
	r.taskowner = taskowner
	r.log = IgnoreLog{}
	return r
}

// SetLog sets the log implementation. Log messages are ignored by default.
func (rest *Conn) SetLog(log Log) {
	rest.log = log
}

// FromTree returns new Conn with a new tree position. An empty tree value defaults to master branch.
func (rest *Conn) FromTree(tree string) *Conn {
	t := *rest
	t.tree = tree
	return &t
}

// Tree reads the current tree position use for Tree sub-commands. Empty defaults to master.
func (rest *Conn) Tree() string {
	return rest.tree
}

// TaskOwner returns name of task owner (commit author)
func (rest *Conn) TaskOwner() string {
	return rest.taskowner
}

// SetTaskOwner sets the commit author in Irmin
func (rest *Conn) SetTaskOwner(owner string) {
	rest.taskowner = owner
}

// NewTask creates a new task (commit message) that can be be submitted with a command
func NewTask(taskowner string, message string) Task {
	var t Task
	t.Date = fmt.Sprintf("%d", time.Now().Unix())
	t.UID = "0"
	t.Owner = NewValue(taskowner)
	t.Messages = []Value{NewValue(message)}
	return t
}

// NewTask creates a new task that can be be submitted with a command (commit message)
func (rest *Conn) NewTask(message string) Task {
	return NewTask(rest.taskowner, message)
}

// MakeCallURL creates an invocation URL for an Irmin REST command with an optional sub command type
func (rest *Conn) MakeCallURL(ct SubCommandType, command string, path Path) (*url.URL, error) {
	var suffix *url.URL
	var err error

	p := path.URL()

	var parentCommand string
	var parentParam string

	switch ct {
	case CommandNormal:
	case CommandTree:
		if rest.Tree() != "" { // Ignore the parameter if Tree is not set
			parentCommand = "tree"
			parentParam = url.QueryEscape(rest.Tree())
		}
	default:
		return nil, fmt.Errorf("unknown command type %d", ct)
	}

	if parentCommand == "" {
		if suffix, err = url.Parse(fmt.Sprintf("/%s%s", command, p.String())); err != nil {
			return nil, err
		}
	} else {
		if suffix, err = url.Parse(fmt.Sprintf("/%s/%s/%s%s", parentCommand, parentParam, command, p.String())); err != nil {
			return nil, err
		}
	}

	return rest.baseURI.ResolveReference(suffix), nil
}

// Run the specified HTTP command and return the full body of the result.
func (rest *Conn) runCommand(ct SubCommandType, command string, path Path, post *postRequest, v interface{}) (err error) {
	uri, err := rest.MakeCallURL(ct, command, path)
	if err != nil {
		return
	}
	rest.log.Printf("calling: %s\n", uri.String())
	var res *http.Response
	if post == nil {
		res, err = http.Get(uri.String())
	} else {
		j, err := json.Marshal(post)
		if err != nil {
			panic(err)
		}
		rest.log.Printf("post body: %s\n", j)
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
	rest.log.Printf("returned: %s\n", body)

	return json.Unmarshal(body, v)
}

// Run the specified command and return a channel with responses until the stream is closed. The channel contains raw replies and must be unmarshaled by the caller.
func (rest *Conn) runStreamCommand(ct SubCommandType, command string, path Path, post *postRequest) (_ <-chan *streamReply, err error) {
	var streamToken struct {
		Stream Value
	}
	var version struct {
		Version Value
	}

	uri, err := rest.MakeCallURL(ct, command, path)
	if err != nil {
		return
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

// AvailableCommands queries Irmin for a list of available commands
func (rest *Conn) AvailableCommands() ([]string, error) {
	var data commandsReply
	var err error
	if err = rest.runCommand(CommandTree, "", Path{}, nil, &data); err != nil {
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

// Version returns the Irmin version
func (rest *Conn) Version() (string, error) {
	var data commandsReply
	var err error
	if err = rest.runCommand(CommandTree, "", Path{}, nil, &data); err != nil {
		return "", err
	}
	if data.Error.String() != "" {
		return "", fmt.Errorf(data.Error.String())
	}

	return data.Version.String(), nil
}

// List returns a list of keys in a path
func (rest *Conn) List(path Path) ([]Path, error) {
	var data listReply
	var err error
	if err = rest.runCommand(CommandTree, "list", path, nil, &data); err != nil {
		return []Path{}, err
	}
	if data.Error.String() != "" {
		return []Path{}, fmt.Errorf(data.Error.String())
	}

	return data.Result, nil
}

// Mem returns true if a path exists
func (rest *Conn) Mem(path Path) (bool, error) {
	var data memReply
	var err error
	err = rest.runCommand(CommandTree, "mem", path, nil, &data)
	if err != nil {
		return false, err
	}
	if data.Error.String() != "" {
		return false, fmt.Errorf(data.Error.String())
	}
	return data.Result, nil
}

// Head returns the commit hash of HEAD
func (rest *Conn) Head() ([]byte, error) {
	var data headReply
	var err error
	if err = rest.runCommand(CommandTree, "head", nil, nil, &data); err != nil {
		return []byte{}, err
	}
	if data.Error.String() != "" {
		return []byte{}, fmt.Errorf("irmin error: %s", data.Error.String())
	}
	if len(data.Result) > 1 {
		return []byte{}, fmt.Errorf("head returned more than one result")
	}
	if len(data.Result) == 1 {
		hash, err := hex.DecodeString(data.Result[0].String())
		if err != nil {
			return []byte{}, fmt.Errorf("Unable to parse hash from Irmin: %s", data.Result[0])
		}
		return hash, nil
	}
	return []byte{}, fmt.Errorf("Invalid data from Irmin.")
}

// Read key value as byte array
func (rest *Conn) Read(path Path) ([]byte, error) {
	var data readReply
	var err error
	if err = rest.runCommand(CommandTree, "read", path, nil, &data); err != nil {
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
	}
	return []byte{}, fmt.Errorf("invalid key %s", path.String())
}

// ReadString reads a value as string. The value must contain a valid UTF-8 encoded string.
func (rest *Conn) ReadString(path Path) (string, error) {
	res, err := rest.Read(path)
	if err != nil {
		return "", err
	}
	if utf8.Valid(res) {
		return string(res), nil
	}
	return "", fmt.Errorf("path %s does not contain a valid utf8 string", path.String())
}

// Update a key. Returns hash as string on success.
func (rest *Conn) Update(t Task, path Path, contents []byte) (string, error) {
	var data updateReply
	var err error

	var body postRequest
	i := Value(contents)

	body.Data, err = i.MarshalJSON()
	if err != nil {
		return "", err
	}

	body.Task = t

	if err = rest.runCommand(CommandTree, "update", path, &body, &data); err != nil {
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

// Remove key
func (rest *Conn) Remove(t Task, path Path) error {
	var data removeReply
	var err error
	body := postRequest{t, nil}
	if err = rest.runCommand(CommandTree, "remove", path, &body, &data); err != nil {
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

// RemoveRec removes a key and its subtree recursively
func (rest *Conn) RemoveRec(t Task, path Path) error {
	var data removeReply
	var err error
	body := postRequest{t, nil}
	if err = rest.runCommand(CommandTree, "remove-rec", path, &body, &data); err != nil {
		return err
	}
	if data.Error.String() != "" {
		return fmt.Errorf(data.Error.String())
	}
	if data.Result.String() == "" {
		return fmt.Errorf("remove-rec %s returned empty result", path.String())
	}

	return nil
}

// Iter iterates through all keys in database. Returns results in a channel as they are received.
func (rest *Conn) Iter() (<-chan *Path, error) {
	var ch <-chan *streamReply
	var err error
	if ch, err = rest.runStreamCommand(CommandTree, "iter", Path{}, nil); err != nil || ch == nil {
		return nil, err
	}

	out := make(chan *Path, 1)

	go func() {
		defer close(out)
		for m := range ch {
			p := new(Path)
			if err := json.Unmarshal(m.Result, &p); err != nil {
				panic(err) // TODO This should be returned to caller
			}
			out <- p
		}
	}()

	return out, err
}

// Clone the current tree and create a named tag. Force overwrites a previous clone with the same name.
func (rest *Conn) Clone(t Task, name string, force bool) error {
	var data cloneReply
	var err error
	path, err := ParseEncodedPath(url.QueryEscape(name)) // encode and wrap in IrminPath
	if err != nil {
		return err
	}
	command := "clone"
	if force {
		command = "clone-force"
	}
	body := postRequest{t, nil}
	if err = rest.runCommand(CommandTree, command, path, &body, &data); err != nil {
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

// CompareAndSet sets a key if the current value is equal to the given value.
func (rest *Conn) CompareAndSet(t Task, path Path, oldcontents *[]byte, contents *[]byte) (string, error) {
	var data updateReply
	var err error

	var body postRequest

	post := [][]*Value{[]*Value{(*Value)(oldcontents)}, []*Value{(*Value)(contents)}}

	body.Data, err = json.Marshal(&post)
	if err != nil {
		return "", err
	}

	body.Task = t

	if err = rest.runCommand(CommandTree, "compare-and-set", path, &body, &data); err != nil {
		return data.Result.String(), err
	}
	if data.Error.String() != "" {
		return "", fmt.Errorf(data.Error.String())
	}
	if data.Result.String() == "" {
		return "", fmt.Errorf("compare-and-set seemed to succeed, but didn't return a hash", path.String(), data.Result.String())
	}

	return data.Result.String(), nil
}
