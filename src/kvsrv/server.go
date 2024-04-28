package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu    sync.Mutex
	kvMap map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	// Acquire lock
	kv.mu.Lock()

	// Get value of a key
	value, exists := kv.kvMap[args.Key]
	if exists {
		reply.Value = value
	}

	// Release lock
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Acquire lock
	kv.mu.Lock()

	// Add new KV, replace existing value if present
	kv.kvMap[args.Key] = args.Value

	// Release lock
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {

	// Acquire lock
	kv.mu.Lock()

	// Get value of a key
	value, exists := kv.kvMap[args.Key]
	if exists {
		kv.kvMap[args.Key] += args.Value
	} else {
		kv.kvMap[args.Key] = args.Value
	}

	// Return the old value
	reply.Value = value

	// Release lock
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvMap = make(map[string]string)

	return kv
}
