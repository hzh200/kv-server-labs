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
	storage map[string]string
	cache map[int64]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	delete(kv.cache, args.LastReceivedId)
	// if message, found := kv.cache[args.Id]; found {
	// 	reply.Value = message
	// 	kv.mu.Unlock()
	// 	return
	// }
	if _, found := kv.storage[args.Key]; found {
		reply.Value = kv.storage[args.Key]
	} else {
		reply.Value = ""
	}
	// kv.cache[args.Id] = reply.Value
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	delete(kv.cache, args.LastReceivedId)
	if _, found := kv.cache[args.Id]; found {
		kv.mu.Unlock()
		return
	}
	kv.storage[args.Key] = args.Value
	kv.cache[args.Id] = ""
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	delete(kv.cache, args.LastReceivedId)
	if message, found := kv.cache[args.Id]; found {
		reply.Value = message
		kv.mu.Unlock()
		return
	}
	if _, found := kv.storage[args.Key]; !found {
		kv.storage[args.Key] = ""
	}
	oldValue := kv.storage[args.Key]
	
	kv.storage[args.Key] = kv.storage[args.Key] + args.Value
	kv.cache[args.Id] = oldValue
	reply.Value = oldValue
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.cache = make(map[int64]string)
	return kv
}
