package main

import (
	"congcache"
	"flag"
	"fmt"
	"log"
	"net/http"
)

var db = map[string]string{
	"Lily": "666",
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func createGroup(name string, db map[string]string) *congcache.Group {
	return congcache.NewGroup(name, 2<<10, congcache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

func startCacheServer(addr string, addrs []string, gee *congcache.Group) {
	peers := congcache.NewPool(addr)
	peers.Set(addrs...)
	gee.RegisterPeers(peers)
	log.Println("congcache is running at", addr)
	peers.Serve()
}

func startAPIServer(apiAddr string, gee *congcache.Group) {
	http.Handle("/api", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			view, err := gee.Get(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(view.ByteSlice())

		}))
	log.Println("fontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))

}

func main() {
	var port int
	flag.IntVar(&port, "port", 8001, "congcache server port")
	var api bool
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

	apiAddr := "http://localhost:9999"
	addrMap := map[int]string{
		8001: "localhost:8001",
		8002: "localhost:8002",
		8003: "localhost:8003",
	}

	var addrs []string
	for _, v := range addrMap {
		addrs = append(addrs, v)
	}

	g := createGroup("score", db)
	if api {
		go startAPIServer(apiAddr, g)
	}
	startCacheServer(addrMap[port], []string(addrs), g)
}
