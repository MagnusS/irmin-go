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
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"
)

type View struct {
	srv  *RestConn
	head string
	node string
	path IrminPath
}

type CreateViewReply StringReply
type ViewReadReply StringReply
type ViewMergeReply StringReply
type ViewUpdateReply UpdateReply

func (rest *RestConn) CreateView(t Task, path IrminPath) (*View, error) {

	var data CreateViewReply

	var body PostRequest
	body.Data = nil
	body.Task = t

	// TODO Rename command to /create when https://github.com/mirage/irmin/issues/294 is fixed
	err := rest.runCommand(COMMAND_TREE, "view/create/create", path, &body, &data)
	if err != nil {
		return nil, err
	}
	if data.Error.String() != "" {
		return nil, fmt.Errorf(data.Error.String())
	}
	if data.Result.String() == "" {
		return nil, fmt.Errorf("empty result")
	}
	// TODO Simplify parsing if https://github.com/mirage/irmin/issues/295 is fixed
	r := strings.Split(data.Result.String(), "-") // Just basic error checking here, hashes not checked for errors
	if len(r) != 2 {
		return nil, fmt.Errorf("invalid result: %s", data.Result.String())
	}

	v := new(View)
	v.srv = rest
	v.head = r[0]
	v.node = r[1]
	v.path = path
	return v, nil
}

// Return original path the view was created from
func (view *View) Path() IrminPath {
	return view.path
}

// Return tree position the view was created from. An empty tree value uses master by default.
func (view *View) Tree() string {
	return view.srv.Tree()
}

// Read path from view
func (view *View) Read(path IrminPath) ([]byte, error) {
	var data ViewReadReply
	var err error
	cmd := fmt.Sprintf("view/%s/read", url.QueryEscape(view.node))
	if err = view.srv.runCommand(COMMAND_NORMAL, cmd, path, nil, &data); err != nil {
		return []byte{}, err
	}
	if data.Error.String() != "" {
		return []byte{}, fmt.Errorf(data.Error.String())
	}
	return data.Result, nil
}

// Read string from path. If string is not valid utf8 an error is returned.
func (view *View) ReadString(path IrminPath) (string, error) {
	// TODO This code duplicates functionality from rest.ReadString
	res, err := view.Read(path)
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
func (view *View) Update(t Task, path IrminPath, contents []byte) (string, error) {
	var data ViewUpdateReply
	var err error

	var body PostRequest
	i := IrminString(contents)

	body.Data, err = i.MarshalJSON()
	if err != nil {
		return "", err
	}

	body.Task = t

	cmd := fmt.Sprintf("view/%s/update", url.QueryEscape(view.node))
	if err = view.srv.runCommand(COMMAND_NORMAL, cmd, path, &body, &data); err != nil {
		return data.Result.String(), err
	}
	if data.Error.String() != "" {
		return "", fmt.Errorf(data.Error.String())
	}
	if data.Result.String() == "" {
		return "", fmt.Errorf("update seemed to succeed, but didn't return a hash", path.String(), data.Result.String())
	}

	view.node = data.Result.String() // Store new node position

	return view.node, nil
}

// Attempt to merge view into the specified branch and path. Empty tree defaults to master.
func (view *View) MergePath(t Task, tree string, path IrminPath) error {
	var data ViewMergeReply
	var err error

	var body PostRequest
	i := IrminString([]byte(view.head)) // body contains head

	body.Data, err = i.MarshalJSON()
	if err != nil {
		return err
	}

	body.Task = t

	cmd := fmt.Sprintf("tree/%s/view/%s/merge-path", url.QueryEscape(tree), url.QueryEscape(view.node))
	if err = view.srv.runCommand(COMMAND_NORMAL, cmd, path, &body, &data); err != nil {
		return err
	}
	if data.Error.String() != "" {
		return fmt.Errorf(data.Error.String())
	}
	// TODO Assumes succses if no error, should probably check result

	return nil
}

// Write the view into the specified tree and path. Overwrites existing values.
func (view *View) UpdatePath(t Task, tree string, path IrminPath) error {
	var data ViewUpdateReply
	var err error

	body := PostRequest{t, nil}

	cmd := fmt.Sprintf("tree/%s/view/%s/update-path", url.QueryEscape(tree), url.QueryEscape(view.node))
	if err = view.srv.runCommand(COMMAND_NORMAL, cmd, path, &body, &data); err != nil {
		return err
	}
	if data.Error.String() != "" {
		return fmt.Errorf(data.Error.String())
	}
	if data.Result.String() == "" {
		return fmt.Errorf("update-path seemed to succeed, but didn't return a hash", path.String(), data.Result.String())
	}

	return nil
}

// Iterate through all keys in a view. Returns results in a channel as they are received.
func (view *View) Iter() (<-chan *IrminPath, error) {
	var ch <-chan *StreamReply
	var err error
	cmd := fmt.Sprintf("view/%s/iter", url.QueryEscape(view.node))
	if ch, err = view.srv.runStreamCommand(COMMAND_NORMAL, cmd, IrminPath{}, nil); err != nil || ch == nil {
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
