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
	mu sync.Mutex
	// Your definitions here.
	cache map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if _, found := kv.cache[args.Key]; found {
		reply.Value = kv.cache[args.Key]
	} else {
		reply.Value = ""
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	kv.cache[args.Key] = args.Value
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var oldValue string
	kv.mu.Lock()
	if _, found := kv.cache[args.Key]; !found {
		kv.cache[args.Key] = ""
	}
	oldValue = kv.cache[args.Key]
	
	kv.cache[args.Key] = kv.cache[args.Key] + args.Value
	kv.mu.Unlock()
	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.cache = make(map[string]string)
	return kv
}
