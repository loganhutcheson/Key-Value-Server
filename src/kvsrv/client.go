package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	Id     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.Id = nrand()

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	args := GetArgs{key}
	reply := GetReply{}

	// Keep trying until server sends back success
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			break
		}
	}

	return reply.Value
}

// shared by Put and Append.
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {

	args := PutAppendArgs{key, value, ck.Id, nrand()}
	reply := PutAppendReply{}

	// Keep trying forever, until Call is OK
	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			break
		}
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
