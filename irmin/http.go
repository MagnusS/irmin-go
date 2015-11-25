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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

type ErrorVersion struct {
	Error   Value
	Version Value
}

type stringArrayReply struct {
	ErrorVersion
	Result []Value
}

type stringReply struct {
	ErrorVersion
	Result Value
}

type pathArrayReply struct {
	ErrorVersion
	Result []Path
}

type boolReply struct {
	ErrorVersion
	Result bool
}

// Task describes the commit message stored in Irmin
type Task struct {
	Date     string  `json:"date"`
	UID      string  `json:"uid"`
	Owner    Value   `json:"owner"`
	Messages []Value `json:"messages"`
}

// CommitValuePair represents the value of a key at a specific commit
type CommitValuePair struct {
	Commit []byte
	Value  []byte
}

const (
	KeyDeleted = "-"
	KeyCreated = "+"
	KeyUpdated = "*"
)

// WatchPathChange contains one change received from a watch, usually as a part of WatchPathCommit
type WatchPathChange struct {
	Change string // *=Updated, +=Created, -=Deleted
	Key    Path
}

// WatchPathCommit contains a commit and updated, deleted or created keys as returned by WatchPath
type WatchPathCommit struct {
	Commit  []byte
	Changes []WatchPathChange
	Error   error // Only set if an error occurred and the watch needs to be restarted
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

// Conn is an Irmin REST API connection
type Conn struct {
	client
	tree      string
	taskowner string
}

// Create an Irmin REST HTTP connection data structure
func Create(uri *url.URL, taskowner string) *Conn {
	r := new(Conn)
	r.client = *NewClient(uri, IgnoreLog{})
	r.taskowner = taskowner
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
func (rest *Conn) MakeCallURL(command string, path Path, supportsTree bool) (*url.URL, error) {
	var suffix *url.URL
	var err error

	u := path.URL()
	p := strings.Replace(u.String(), "+", "%20", -1) // Replace + with %20, see https://github.com/golang/go/issues/4013

	if supportsTree && rest.Tree() != "" { // Ignore the parameter if Tree is not set
		t := url.QueryEscape(rest.Tree())
		if suffix, err = url.Parse(fmt.Sprintf("/tree/%s/%s%s", t, command, p)); err != nil {
			return nil, err
		}
	} else {
		if suffix, err = url.Parse(fmt.Sprintf("/%s%s", command, p)); err != nil {
			return nil, err
		}
	}

	return rest.baseURI.ResolveReference(suffix), nil
}

// AvailableCommands queries Irmin for a list of available commands
func (rest *Conn) AvailableCommands() ([]string, error) {
	var data commandsReply

	uri, err := rest.MakeCallURL("", Path{}, true)
	if err != nil {
		return []string{}, err
	}

	if err = rest.Call(uri, nil, &data); err != nil {
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
	uri, err := rest.MakeCallURL("", Path{}, true)
	if err != nil {
		return "", err
	}
	if err = rest.Call(uri, nil, &data); err != nil {
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
	uri, err := rest.MakeCallURL("list", path, true)
	if err != nil {
		return []Path{}, err
	}
	if err = rest.Call(uri, nil, &data); err != nil {
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
	uri, err := rest.MakeCallURL("mem", path, true)
	if err != nil {
		return false, err
	}
	if err = rest.Call(uri, nil, &data); err != nil {
		return false, err
	}
	if data.Error.String() != "" {
		return false, fmt.Errorf(data.Error.String())
	}
	return data.Result, nil
}

// Head returns the commit hash of HEAD. Returns nil if no current HEAD (db is empty)
func (rest *Conn) Head() ([]byte, error) {
	var data headReply
	uri, err := rest.MakeCallURL("head", nil, true)
	if err != nil {
		return []byte{}, err
	}
	if err = rest.Call(uri, nil, &data); err != nil {
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
	if len(data.Result) == 0 {
		return nil, nil
	}
	return []byte{}, fmt.Errorf("Invalid data from Irmin.")
}

// Read key value as byte array
func (rest *Conn) Read(path Path) ([]byte, error) {
	var data readReply
	uri, err := rest.MakeCallURL("read", path, true)
	if err != nil {
		return []byte{}, err
	}
	if err = rest.Call(uri, nil, &data); err != nil {
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

	uri, err := rest.MakeCallURL("update", path, true)
	if err != nil {
		return "", err
	}
	if err = rest.Call(uri, &body, &data); err != nil {
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
	uri, err := rest.MakeCallURL("remove", path, true)
	if err != nil {
		return err
	}
	body := postRequest{t, nil}
	if err = rest.Call(uri, &body, &data); err != nil {
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
	uri, err := rest.MakeCallURL("remove-rec", path, true)
	if err != nil {
		return err
	}
	body := postRequest{t, nil}
	if err = rest.Call(uri, &body, &data); err != nil {
		return err
	}
	if data.Error.String() != "" {
		return fmt.Errorf(data.Error.String())
		return fmt.Errorf("remove-rec %s returned empty result", path.String())
	}

	return nil
}

// Iter iterates through all keys in database. Returns results in a channel as they are received.
func (rest *Conn) Iter() (<-chan *Path, error) {
	uri, err := rest.MakeCallURL("iter", Path{}, true)
	if err != nil {
		return nil, err
	}
	var ch <-chan *streamReply
	if ch, err = rest.CallStream(uri, nil); err != nil || ch == nil {
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

// Watch a specific key for create/delete/update. Returns commit/value pairs. This function is not recursive (see WatchPath)
func (rest *Conn) Watch(path Path, firstCommit []byte) (<-chan *CommitValuePair, error) { // TODO not path
	type watchKeyReply [][]Value // An array of arrays of commit/value pairs

	var body *postRequest = nil
	if firstCommit != nil {
		body = new(postRequest)
		body.Task = rest.NewTask("Watching db")
		s := hex.EncodeToString(firstCommit)
		body.Data = json.RawMessage(fmt.Sprintf("[\"%s\", \"%s\"]", s, "hei"))
	}

	uri, err := rest.MakeCallURL("watch", path, true)
	if err != nil {
		return nil, err
	}

	var ch <-chan *streamReply
	if ch, err = rest.CallStream(uri, body); err != nil || ch == nil {
		return nil, err
	}

	out := make(chan *CommitValuePair, 1)

	go func() {
		defer close(out)
		for m := range ch {
			p := new([][]Value)
			if err := json.Unmarshal(m.Result, p); err != nil {
				panic(err) // TODO This should be returned to caller
			}
			for _, q := range *p {
				if len(q) != 2 {
					rest.log.Printf("length of response longer than 2 (%d), ignored", len(q))
					continue
				}
				c := new(CommitValuePair)
				c.Commit, err = hex.DecodeString(q[0].String())
				if err != nil {
					rest.log.Printf("Unable to decode commit hash from watch (ignored): %s", q[0].String())
					continue
				}
				c.Value = q[1]
				out <- c
			}
		}
	}()

	return out, err
}

// WatchPath watches a path recursively. Returns keys that are updated, deleted or created. On error, the last item in the channel
// will have .Error set - the channel is then closed.
func (rest *Conn) WatchPath(path Path, firstCommit []byte) (<-chan *WatchPathCommit, error) { // TODO not path
	uri, err := rest.MakeCallURL("watch-rec", path, true)
	if err != nil {
		return nil, err
	}

	var body *postRequest = nil
	if firstCommit != nil {
		body = new(postRequest)
		body.Task = rest.NewTask("Watching db")
		s := hex.EncodeToString(firstCommit)
		body.Data = json.RawMessage(fmt.Sprintf("[\"%s\"]", s, s))
	}

	var ch <-chan *streamReply
	if ch, err = rest.CallStream(uri, nil); err != nil || ch == nil {
		return nil, err
	}

	out := make(chan *WatchPathCommit, 1)

	type change struct {
		Change string `json:""`
		Key    Path   `json:""`
	}

	go func() {
		defer close(out)
		for m := range ch {
			c := new(WatchPathCommit)

			var q [2]json.RawMessage // array of raw messages
			if err := json.Unmarshal(m.Result, &q); err != nil {
				fmt.Printf("json(0): %s\n", m.Result)
				c.Error = err
				out <- c
				return
			}

			var s string // first entry in array is string (commit hash)
			if err := json.Unmarshal(q[0], &s); err != nil {
				fmt.Printf("json(1): %s\n", q[0])
				c.Error = err
				out <- c
				return
			}
			commit, err := hex.DecodeString(s)
			if err != nil {
				rest.log.Printf("Unable to decode commit hash from watch-rec (ignored): %s", s)
				continue
			}

			var changes []json.RawMessage // second entry is array of string/path pairs
			if err := json.Unmarshal(q[1], &changes); err != nil {
				fmt.Printf("json(2): %s\n", q[1])
				c.Error = err
				out <- c
				return
			}

			c.Commit = commit
			c.Changes = make([]WatchPathChange, len(changes))

			for x, pair := range changes {
				var k []json.RawMessage // split pair in hash + path
				if err := json.Unmarshal(pair, &k); err != nil {
					fmt.Printf("json(3): %s\n", pair)
					c.Error = err
					out <- c
					return
				}
				if len(k) != 2 {
					c.Error = fmt.Errorf("Expected string/path pair array of len 2, actual len was %d", len(k))
					out <- c
					return
				}

				var changetype string
				if err := json.Unmarshal(k[0], &changetype); err != nil {
					fmt.Printf("json(4): %s\n", k[0])
					c.Error = err
					out <- c
					return
				}

				var key Path
				if err := json.Unmarshal(k[1], &key); err != nil {
					fmt.Printf("json(5): %s\n", k[1])
					c.Error = err
					out <- c
					return
				}

				c.Changes[x].Change = changetype
				c.Changes[x].Key = key
			}

			out <- c
		}
	}()

	return out, err
}

// Clone the current tree and create a named tag. Force overwrites a previous clone with the same name.
func (rest *Conn) Clone(t Task, name string, force bool) error {
	var data cloneReply

	path, err := ParseEncodedPath(url.QueryEscape(name)) // encode and wrap in IrminPath
	if err != nil {
		return err
	}
	command := "clone"
	if force {
		command = "clone-force"
	}

	uri, err := rest.MakeCallURL(command, path, true)
	if err != nil {
		return err
	}

	body := postRequest{t, nil}
	if err = rest.Call(uri, &body, &data); err != nil {
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

	uri, err := rest.MakeCallURL("compare-and-set", path, true)
	if err != nil {
		return "", err
	}

	var body postRequest

	post := [][]*Value{[]*Value{(*Value)(oldcontents)}, []*Value{(*Value)(contents)}}

	body.Data, err = json.Marshal(&post)
	if err != nil {
		return "", err
	}

	body.Task = t

	if err = rest.Call(uri, &body, &data); err != nil {
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
