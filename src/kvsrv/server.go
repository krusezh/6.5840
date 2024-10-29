package kvsrv

import (
	"log"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// change to struct{}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	kvs      map[string]string
	oldCache map[int64]string
	cache    map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.kvs[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	old := kv.kvs[args.Key]
	kv.kvs[args.Key] = args.Value
	reply.Value = old
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.oldCache, args.Last)
	delete(kv.cache, args.Last)
	if old, ok := kv.oldCache[args.Id]; ok {
		reply.Value = old
	} else if old, ok := kv.cache[args.Id]; ok {
		reply.Value = old
	} else {
		old := kv.kvs[args.Key]
		kv.cache[args.Id] = old
		kv.kvs[args.Key] = old + args.Value
		reply.Value = old
	}
	// DPrintf("kv: %d cache: %d old cache: %d", getStringMapSize(kv.kvs), getMapSize(kv.cache), getMapSize(kv.oldCache))
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.oldCache = make(map[int64]string)
	kv.cache = make(map[int64]string)

	go func() {
		ticker := time.NewTicker(time.Millisecond * 500)
		for {
			<-ticker.C
			kv.mu.Lock()
			kv.oldCache = kv.cache
			kv.cache = make(map[int64]string)
			// DPrintf("Tick at %v ", t)
			// DPrintf("kv: %d cache: %d old cache: %d", getStringMapSize(kv.kvs), getMapSize(kv.cache), getMapSize(kv.oldCache))
			kv.mu.Unlock()
		}
	}()

	return kv
}

func getStringMapSize(m map[string]string) uintptr {
	var size uintptr

	// Size of map header
	size += reflect.TypeOf(m).Size()

	// Iterate over the map to calculate size of keys and values
	for k, v := range m {
		size += getStringSize(k) + getStringSize(v)
	}

	return size
}
func getMapSize(m map[int64]string) uintptr {
	var size uintptr

	// Size of map header
	size += reflect.TypeOf(m).Size()

	// Iterate over the map to calculate size of keys and values
	for _, v := range m {
		size += int64Size() + getStringSize(v)
	}

	return size
}

func int64Size() uintptr {
	return unsafe.Sizeof(int64(0))
}

func getStringSize(s string) uintptr {
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return uintptr(stringHeader.Len) + unsafe.Sizeof(stringHeader)
}
