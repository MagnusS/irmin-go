package main

import (
	"../irmin"
	"fmt"
	"net/url"
)

// irmin init -d -v --root /tmp/irmin/test -a http://:8080

/*func show_tree(base string, tree string, path IrminPath) {
	data, err := irmin_list(base, tree, path)
	if err != nil {
		panic(err)
	}
	for _, v := range data.Result {
		mem, err := irmin_mem(base, tree, v)
		if err != nil {
			panic(err)
		}
		r_str := func() string {
			d, err := irmin_read(base, tree, v)
			if err != nil {
				panic(err)
			}
			if len(d.Result) > 0 {
				return string(d.Result[0])
			} else {
				return "<none>"
			}
		}
		fmt.Printf("%30s\t\t%5t\t\t%20s\n", v, mem.Result, r_str())
		show_tree(base, tree, v)
	}
}*/

func main() {
	uri, err := url.Parse("http://127.0.0.1:8080")
	if err != nil {
		panic(err)
	}

	r := irmin.Create(*uri, "api-tester")
	{ // get version
		v, err := r.Version()
		if err != nil {
			panic(err)
		}
		fmt.Printf("version: %s\n", v)
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
		var ch <-chan *irmin.IrminPath
		if ch, err = r.Iter(); err != nil {
			panic(err)
		}

		for p := range ch {
			fmt.Printf("iter: %s\n", (*p).String())
		}
	}
	{ // iter + read
		var ch <-chan *irmin.IrminPath
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
		data := []byte("Hello world")
		hash, err := r.Update(r.NewTask("update key"), irmin.ParsePath(key), &data)
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
}
