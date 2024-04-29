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
	mu          sync.Mutex
	kvMap       map[string]string
	callHistory map[int64]KVTransaction
}

type KVTransaction struct {
	transId int64
	value   string
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

	// If this call transaction equals a transaction ID
	// that is present in the history, return the previous value
	transaction, exists := kv.callHistory[args.ClientId]
	if exists && transaction.transId == args.TransId {
		reply.Value = kv.callHistory[args.ClientId].value
		// Release lock
		kv.mu.Unlock()
		return
	}

	// Delete the previous history
	delete(kv.callHistory, args.ClientId)
	// Commit KV
	kv.kvMap[args.Key] = args.Value
	// Commit history
	kv.callHistory[args.ClientId] = KVTransaction{args.TransId, ""}

	// Release lock
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {

	// Acquire lock
	kv.mu.Lock()

	// If this call transaction equals a transaction ID
	// that is present in the history, return the previous value
	transaction, exists := kv.callHistory[args.ClientId]
	if exists && transaction.transId == args.TransId {
		reply.Value = kv.callHistory[args.ClientId].value
		// Release lock
		kv.mu.Unlock()
		return
	}

	// Delete the previous history
	delete(kv.callHistory, args.ClientId)
	// Commit KV
	value, exists := kv.kvMap[args.Key]
	if exists {
		kv.kvMap[args.Key] += args.Value
	} else {
		kv.kvMap[args.Key] = args.Value
	}
	// Commit history
	kv.callHistory[args.ClientId] = KVTransaction{args.TransId, value}
	// Return old value
	reply.Value = value

	// Release lock
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvMap = make(map[string]string)
	kv.callHistory = make(map[int64]KVTransaction)

	return kv
}
