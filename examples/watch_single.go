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

	key := irmin.ParsePath("/view-test/exists")
	fmt.Printf("Watching %s\n", key.String())
	fmt.Printf("(run examples/views/views.go example to test)\n")
	ch, err := r.Watch(key)
	if err != nil {
		panic(err)
	}
	for c := range ch {
		fmt.Printf("commit: %s value: %s\n", hex.EncodeToString(c.Commit), c.Value)
	}
}
