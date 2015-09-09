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
	"../../irmin"
	"fmt"
	"net/url"
)

func list_db(r *irmin.RestConn) {
	ch, err := r.Iter() // Iterate through all keys
	if err != nil {
		panic(err)
	}

	for path := range ch {
		d, err := r.ReadString(*path) // Read key
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s=%s\n", (*path).String(), d)
	}
}

func main() {
	uri, _ := url.Parse("http://127.0.0.1:8080")
	r := irmin.Create(uri, "view-example")

	// Check for /view-test and remove if it exists
	b, err := r.Mem(irmin.ParsePath("/view-test/exists"))
	if err != nil {
		panic(err)
	}
	if b {
		fmt.Printf("view path exists, removing...\n")
		if err = r.RemoveRec(r.NewTask("removing existing /view-test"), irmin.ParsePath("/view-test")); err != nil {
			panic(err)
		}
	}

	// Create a key in /view-test
	s, err := r.Update(r.NewTask("update key /view-test/exists"), irmin.ParsePath("/view-test/exists"), irmin.NewIrminString("hello world"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("update=%s\n", s)

	list_db(r)

	// Create view #1 from /view-test
	v1, err := r.CreateView(r.NewTask("create view 1"), irmin.ParsePath("/view-test/"))
	if err != nil {
		panic(err)
	}
	s, err = v1.Update(r.NewTask("add key"), irmin.ParsePath("from-view-1"), irmin.NewIrminString("hello world from view 1"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("update view 1=%s\n", s)

	// Create view #2 from /view-test
	v2, err := r.CreateView(r.NewTask("create view 2"), irmin.ParsePath("/view-test/"))
	if err != nil {
		panic(err)
	}
	s, err = v2.Update(r.NewTask("add key"), irmin.ParsePath("from-view-2"), irmin.NewIrminString("hello world from view 2"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("update view 2=%s\n", s)

	// Merge view 2

	fmt.Printf("merge view 2=%s\n", s)
	err = v2.MergePath(r.NewTask("merge view 2"), "master", irmin.ParsePath("/view-test/"))
	if err != nil {
		panic(err)
	}

	// Merge view 1

	fmt.Printf("merge view 1=%s\n", s)
	err = v1.MergePath(r.NewTask("merge view 1"), "master", irmin.ParsePath("/view-test/"))
	if err != nil {
		panic(err)
	}

	list_db(r)
}
