package congcache

import (
	"congcache/consistenthash"
	"congcache/pb"
	"corpc"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

const (
	defaultBasePath = "/_cache/"
	defaultReplicas = 50
)

type Pool struct {
	self       string
	mu         sync.Mutex
	peers      *consistenthash.Map
	rpcGetters map[string]*rpcGetter
}

func NewPool(self string) *Pool {
	return &Pool{
		self: self,
	}
}

func (p *Pool) Log(fotmat string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(fotmat, v...))
}

func (p *Pool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.rpcGetters = make(map[string]*rpcGetter, len(peers))
	for _, peer := range peers {
		p.rpcGetters[peer] = &rpcGetter{baseURL: peer}
	}
}

func (p *Pool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %v", peer)
		return p.rpcGetters[peer], true
	}
	return nil, false
}

type GroupCache struct{}

func (s *GroupCache) Get(args *pb.Request, reply *pb.Response) error {
	// define your service ...
	group := GetGroup(args.Group)
	if group == nil {
		return errors.New("error getting group")
	}
	view, err := group.Get(args.Key)
	if err != nil {
		return errors.New("error getting key")
	}
	reply.Value = view.ByteSlice()
	return nil
}

func (p *Pool) Serve() {
	listener, err := net.Listen("tcp", p.self)
	if err != nil {
		log.Fatalf("error listening: %v", err)
	}
	corpc := corpc.NewServer()

	pb.RegisterGroupCacheServer(corpc, &GroupCache{})

	corpc.Serve(listener)
}

type rpcGetter struct {
	baseURL string
	client  pb.GroupCacheClient
}

func (h *rpcGetter) Get(in *pb.Request) (out *pb.Response, err error) {
	if h.client == nil {
		conn, err := net.Dial("tcp", h.baseURL)
		if err != nil {
			return out, err
		}
		h.client = pb.NewGroupCacheClient(corpc.NewClient(conn))
	}
	out, err = h.client.Get(in)
	if err != nil {
		return
	}
	return
}

var _ PeerGetter = (*rpcGetter)(nil)
