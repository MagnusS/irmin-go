## Go implementation of Irmin HTTP bindings

This library is a Go implementation of the [Irmin](https://github.com/mirage/irmin.git) HTTP API. The HTTP API is partly documented [here](https://github.com/mirage/irmin/wiki/REST-API). Not all calls are available in Irmin version 0.10.0 or older. `Version()` can be used to check the Irmin version.

To install this library, run:
```bash
go get https://github.com/MagnusS/irmin-go/irmin
```

#### Examples

##### Connecting to Irmin

```go
uri, err := url.Parse("http://127.0.0.1:8080")
if err != nil {
		panic(err)
}
conn := irmin.Create(uri, "example-app")
```

##### Check Irmin version
```go
v, err := conn.Version()
if err != nil {
 panic(err)
}
fmt.Printf("Connected to Irmin version %s\n", v)
```

##### Create or update a key
```go
task := conn.NewTask("Update key") // Commit message
key := irmin.ParsePath("/a/b")
v := []byte("Hello world")
hash, err := conn.Update(task, key, v) // Returns commit hash
if err != nil {
 panic(err)
}
```

##### Read a value
```go
key := irmin.ParsePath("/a/b")
v, err := conn.ReadString(key)
if err != nil {
 panic(err)
}
fmt.Printf("%s=%s\n", key.String(), v)
```

##### Iterate through all keys
```go
ch, err := conn.Iter() // Iterate through all keys
if err != nil {
 panic(err)
}

for key := range ch {
 v, err := conn.ReadString(key)
 if err != nil {
  panic(err)
 }
	fmt.Printf("%s=%s\n", key.String(), v)
}
```

##### Other examples

 - [Misc. common commands](examples/main.go)
 - [Iterate through all keys](examples/tree/tree.go)
 - [Creating and merging views/transactions](examples/views/views.go)
 - [Watch a key for changes](examples/watch_single.go)
 - [Watch a path recursively for changes](examples/watch_path.go)
 
To run an example, clone `irmin-go` and run `go run examples/[example code]`.

#### Installing Irmin
Installation instructions for Irmin are available [here](https://github.com/mirage/irmin/blob/master/README.md). When installing with `opam`, the `--dev` parameter can be used to install the latest development version.

To set up a test database with Irmin, this command will create a new database in `/tmp/irmin/test` (if it doesn't exist) and start listening for HTTP requests on port 8080:

```
irmin init -d -v --root /tmp/irmin/test -a http://:8080
```

#### Supported API calls

 - head
 - read
 - mem
 - list
 - iter
 - update
 - clone, clone-force
 - compare-and-set
 - remove, remove-rec
 - watch, watch-rec
 - tree/{list, mem, head, read, update, remove, remove-rec, iter, watch, watch-rec, clone, clone-force, compare-and-set}
 - view/{create, update, read, merge-path, update-path}
