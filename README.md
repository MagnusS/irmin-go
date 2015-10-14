## Go implementation of Irmin HTTP REST bindings

This code is work in progress and not all commands are implemented yet. The Irmin REST API is not yet stable and is partly documented [here](https://github.com/mirage/irmin/wiki/REST-API).

#### Implemented commands

 - head
 - read
 - mem
 - list
 - iter
 - update
 - clone, clone-force
 - compare-and-set
 - remove, remove-rec
 - view/{update, read, merge-path, update-path}

```
irmin init -d -v --root /tmp/irmin/test -a http://:8080
# examples
go run examples/views/views.go
go run examples/main.go
go run examples/tree/tree.go
```
