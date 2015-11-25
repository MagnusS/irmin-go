/*
 Copyright (c) 2015 Magnus Skjegstad <magnus.skjegstad@unikernel.com>
 Copyright (c) 2015 Thomas Leonard <thomas.leonard@unikernel.com>

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
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func irminExec(t *testing.T, cmd string, args ...string) error {
	full_args := []string{cmd, "-s", "http", "--uri", "http://127.0.0.1:8085"}
	full_args = append(full_args, args...)
	t.Log("Running irmin %s", full_args)
	return exec.Command("irmin", full_args...).Run()
}

func spawnIrmin(t *testing.T) *exec.Cmd {
	t.Log("Starting Irmin")
	c := exec.Command("irmin", "init", "-d", "-v", "-s", "mem", "-a", "http://127.0.0.1:8085")
	//c := exec.Command("irmin", "init", "-d", "-v", "--root", "irmin_test", "-a", "http://:8080")
	stdout, err := c.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	err = c.Start()
	if err != nil {
		t.Fatal(err)
	}
	fromIrmin := bufio.NewReader(stdout)
	if fromIrmin == nil {
		t.Fatal(errors.New("NewReader returned nil!"))
	}
	for {
		line, err := fromIrmin.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		// TODO: we really want to know when it is "started", not "starting"
		if strings.HasPrefix(line, "Server starting on port") {
			break
		}
		t.Log("Unexpected Irmin output: %#v", line)
	}
	return c
}

func stopIrmin(t *testing.T, c *exec.Cmd) {
	t.Log("Stopping irmin")
	err := c.Process.Kill()
	if err != nil {
		t.Fatal(err)
	}
	err = c.Wait()
	if err != nil {
		t.Log(err)
	}
}

func TestIrminConnect(t *testing.T) {
	// Start Irmin
	irmin := spawnIrmin(t)
	// Stop Irmin
	defer stopIrmin(t, irmin)
}

func getConn(t *testing.T) *Conn {
	uri, err := url.Parse("http://127.0.0.1:8085")
	t.Log("Connecting to irmin @ ", uri)
	if err != nil {
		t.Fatal(err)
	}
	return Create(uri, "irmin-go-tester")
}

func TestHead(t *testing.T) {
	irmin := spawnIrmin(t)
	defer stopIrmin(t, irmin)

	t.Log("Testing Head on empty db")

	r := getConn(t)

	v, err := r.Head()
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Fatal("head should be nil, but was %s\n", hex.EncodeToString(v))
	}

	t.Log("Testing Head on non-empty db")

	key := "head-test"
	data := []byte("foo")
	hash, err := r.Update(r.NewTask("update key"), ParsePath(key), data)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("update hash is: %s\n", hash)

	v, err = r.Head()
	if err != nil {
		t.Fatal(err)
	}
	h := hex.EncodeToString(v)
	t.Logf("head is: %s\n", h)

	if h != hash {
		t.Fatal("Hash returned by update did not match head (%s vs %s)", h, hash)
	}

}

func TestUpdate(t *testing.T) {
	irmin := spawnIrmin(t)
	defer stopIrmin(t, irmin)

	r := getConn(t)
	key := "update-test"
	t.Logf("update key '%s'", key)
	data := []byte("Hello \"world")
	hash, err := r.Update(r.NewTask("update key"), ParsePath(key), data)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("update hash: %s\n", hash)
	t.Logf("read key `%s`", key)
	d, err := r.ReadString(ParsePath(key))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(data, []byte(d)) != 0 {
		t.Fatal("update/read failed. Written value '%s' != '%s'", string(data), d)
	}
}

func TestWatch(t *testing.T) {
	irmin := spawnIrmin(t)
	defer stopIrmin(t, irmin)

	// Connect to Irmin
	r := getConn(t)

	// expect function waits for expected result with a timeout
	expect := func(ch <-chan *CommitValuePair, path Path, val []byte) {
		// Timeout channel
		timeout := make(chan bool, 1)
		go func() {
			time.Sleep(1 * time.Second)
			timeout <- true
		}()
		// Wait for Watch result or timeout
		select {
		case v := <-ch:
			if bytes.Compare(v.Value, val) != 0 {
				t.Fatalf("Watch result did not contain expected value (expected %s, got %s)", string(val), string(v.Value))
			} else {
				d, err := r.Read(path) // read path to verify content
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Compare(val, d) != 0 {
					t.Fatalf("Update and Watch succeeded, but Read returned old value (was '%s', should be '%s')", string(d), string(val))
				}
			}
		case <-timeout:
			t.Fatal("Timed out while waiting for Watch result")
		}
	}

	// Test watching an existing key
	{
		path := ParsePath("/watch-test/1")
		data := []byte("foo")
		t.Logf("update key '%s'='%s'", path.String(), string(data))
		hash, err := r.Update(r.NewTask("update key"), path, data)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("update hash is %s", hash)

		t.Logf("Set Watch on existing key %s", path.String())
		ch, err := r.Watch(path, nil)
		if err != nil {
			t.Fatal(err)
		}

		data = []byte("bar")
		t.Logf("update key '%s'='%s'", path.String(), string(data))
		hash, err = r.Update(r.NewTask("update key"), path, data)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("update hash is %s", hash)

		expect(ch, path, data)
	}

	// Test watching a non-existing key
	{
		path := ParsePath("/watch-test/2")
		t.Logf("Set Watch on non-existing key %s", path.String())
		ch, err := r.Watch(path, nil)
		if err != nil {
			t.Fatal(err)
		}

		data := []byte("barbar")
		t.Logf("update key '%s'='%s'", path.String(), string(data))
		hash, err := r.Update(r.NewTask("update key"), path, data)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("update hash is %s", hash)

		expect(ch, path, data)
	}

	// Test multiple updates
	{
		path := ParsePath("/watch-test/3")
		t.Logf("Set Watch for multiple updates on key %s", path.String())
		ch, err := r.Watch(path, nil)
		if err != nil {
			t.Fatal(err)
		}

		data := []byte("foo")
		t.Logf("update key '%s'='%s'", path.String(), string(data))
		hash, err := r.Update(r.NewTask("update key"), path, data)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("update hash is %s", hash)

		expect(ch, path, data)

		for i := 0; i < 4; i++ {
			data := []byte(fmt.Sprintf("bar %d", i))
			t.Logf("update key '%s'='%s'", path.String(), string(data))
			hash, err = r.Update(r.NewTask("update key"), path, data)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("update hash is %s", hash)
			expect(ch, path, data)
			time.Sleep(250 * time.Millisecond)
		}
	}

	// Test multiple updates, no delay
	{
		path := ParsePath("/watch-test/4")
		t.Logf("Set Watch for multiple updates on key %s (no delay)", path.String())
		ch, err := r.Watch(path, nil)
		if err != nil {
			t.Fatal(err)
		}

		data := []byte("foo")
		t.Logf("update key '%s'='%s'", path.String(), string(data))
		hash, err := r.Update(r.NewTask("update key"), path, data)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("update hash is %s", hash)

		expect(ch, path, data)

		t.Log("Trigger watch 1000 times")
		for i := 0; i < 1000; i++ {
			data := []byte(fmt.Sprintf("bar %d", i))
			hash, err = r.Update(r.NewTask("update key"), path, data)
			if err != nil {
				t.Fatal(err)
			}
			expect(ch, path, data)
		}
		t.Log("done")
	}
}
