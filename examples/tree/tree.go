package main

import (
	"../../irmin"
	"fmt"
	"net/url"
)

func main() {
	uri, _ := url.Parse("http://127.0.0.1:8080")
	r := irmin.Create(uri, "tree")

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
