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

package main

import (
	"encoding/hex"
	"fmt"
	"net/url"

	"../irmin"
)

// irmin init -d -v --root /tmp/irmin/test -a http://:8080

func main() {
	uri, err := url.Parse("http://127.0.0.1:8080")
	if err != nil {
		panic(err)
	}

	r := irmin.Create(uri, "api-tester")
	{ // get version
		v, err := r.Version()
		if err != nil {
			panic(err)
		}
		fmt.Printf("version: %s\n", v)
	}
	{ // get head
		v, err := r.Head()
		if err != nil {
			panic(err)
		}
		fmt.Printf("head: %s\n", hex.EncodeToString(v))
	}
	{ // list commands
		fmt.Printf("supported commands:\n")
		s, err := r.AvailableCommands()
		if err != nil {
			panic(err)
		}
		for i, v := range s {
			fmt.Printf("%d: %s\n", i, v)
		}
	}
	{ // list
		paths, err := r.List(irmin.ParsePath("/a"))
		if err != nil {
			panic(err)
		}
		fmt.Printf("list /\n")
		for i, v := range paths {
			fmt.Printf("%d: %s\n", i, v.String())
		}
	}
	{ // iter
		var ch <-chan *irmin.Path
		if ch, err = r.Iter(); err != nil {
			panic(err)
		}

		for p := range ch {
			fmt.Printf("iter: %s\n", (*p).String())
		}
	}
	{ // iter on head
		head, err := r.Head()
		if err != nil {
			panic(err)
		}
		t := r.FromTree(hex.EncodeToString(head))
		var ch <-chan *irmin.Path
		if ch, err = t.Iter(); err != nil {
			panic(err)
		}

		for p := range ch {
			fmt.Printf("iter from HEAD: %s\n", (*p).String())
		}
	}
	{ // iter + read
		var ch <-chan *irmin.Path
		if ch, err = r.Iter(); err != nil {
			panic(err)
		}

		for p := range ch {
			d, err := r.ReadString(*p)
			if err != nil {
				panic(err)
			}
			fmt.Printf("%s=%s\n", (*p).String(), d)
		}
	}
	{ // update + read
		key := "g"
		fmt.Printf("update %s=hello world\n", key)
		data := []byte("Hello \"world")
		hash, err := r.Update(r.NewTask("update key"), irmin.ParsePath(key), data)
		if err != nil {
			panic(err)
		}
		fmt.Printf("update hash: %s\n", hash)
		fmt.Printf("read %s\n", key)
		d, err := r.ReadString(irmin.ParsePath(key))
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s=%s\n", key, d)
	}
	/* compare-and-set is not yet implemented in irmin, see https://github.com/mirage/irmin/issues/288
	{ // compare-and-set
		key := "g"
		oldData := []byte("Hello world")
		newData := []byte("asdf")
		fmt.Printf("compare-and-set %s=%s to %s\n", key, oldData, newData)
		hash, err := r.CompareAndSet(r.NewTask("compare-and-set key"), irmin.ParsePath(key), &oldData, &newData)
		if err != nil {
			panic(err)
		}
		fmt.Printf("compare-and-set hash: %s\n", hash)
		fmt.Printf("read %s\n", key)
		d, err := r.ReadString(irmin.ParsePath(key))
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s=%s\n", key, d)
	}
	*/
}
