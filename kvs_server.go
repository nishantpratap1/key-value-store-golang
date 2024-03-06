// prompt: create kvs that  has cache , serverproxy and supports all CRUD operations , also implement strategy to take backup/snapshot of data , and keep TTL for every value
// kvs server code
package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

const (
	DefaultTTL = 15 * time.Second // TTL set to 5 minutes for all keys
)

// struct for keyvalue
type KeyValue struct {
	Value     string
	Timestamp time.Time
}

// struct for keyvaluestore
type KeyValueStore struct {
	data map[string]KeyValue
	ttl  time.Duration
	mu   sync.RWMutex
}

// to create  instance of class
func NewKeyValueStore() *KeyValueStore {
	kvs := &KeyValueStore{
		data: make(map[string]KeyValue),
		ttl:  DefaultTTL,
	}
	return kvs
}

// CRUD

// to get values from kvs
func (kvs *KeyValueStore) GET(key string) (value string, found bool) {
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()
	item, ok := kvs.data[key]
	if !ok {
		return "NOT_FOUND", false
	}
	return item.Value, false
}

func (kvs *KeyValueStore) SET(key, value string) bool {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	kvs.data[key] = KeyValue{Value: value, Timestamp: time.Now()}
	return true
}

func (kvs *KeyValueStore) UPDATE(key, value string) (message string, updated bool) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	_, ok := kvs.data[key]
	if !ok {
		return "VALUE_NOT_EXIST", false
	}
	kvs.data[key] = KeyValue{Value: value, Timestamp: time.Now()}
	return "VALUE_UPDATED", true
}

func (kvs *KeyValueStore) DELETE(key string) (message string, deleted bool) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	_, ok := kvs.data[key]
	if !ok {
		return "VALUE_NOT_EXIST", false
	}
	delete(kvs.data, key)
	return "VALUE_DELETED", true
}

type ServerProxy struct {
	kvs   *KeyValueStore
	cache map[string]KeyValue
	mu    sync.Mutex
}

func NewServerProxy(kvs *KeyValueStore) *ServerProxy {
	sp := &ServerProxy{
		kvs:   kvs,
		cache: make(map[string]KeyValue),
	}
	return sp
}

// to get values from cache
func (sp *ServerProxy) GET(key string) (value string, found bool) {

	sp.mu.Lock()
	defer sp.mu.Unlock()
	if value, ok := sp.cache[key]; ok {
		fmt.Printf("Value for key '%s' retrieved from cache: %v\n", key, value)
		return value.Value, true
	}
	value, ok := sp.kvs.GET(key)
	if ok {
		sp.cache[key] = KeyValue{Value: value, Timestamp: time.Now()}
	}
	return value, true
}

func (sp *ServerProxy) SET(key, value string) bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.kvs.data[key] = KeyValue{Value: value, Timestamp: time.Now()}
	return true
}

func (sp *ServerProxy) UPDATE(key, value string) (message string, updated bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	_, ok := sp.kvs.GET(key)
	if !ok {
		return "VALUE_NOT_EXIST", false
	}
	sp.kvs.UPDATE(key, value)
	sp.cache[key] = KeyValue{Value: value, Timestamp: time.Now()}
	return "VALUE_UPDATED", true
}

func (sp *ServerProxy) DELETE(key string) (message string, deleted bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	_, ok := sp.kvs.GET(key)
	if !ok {
		return "VALUE_NOT_EXIST", false
	}
	sp.kvs.DELETE(key)
	delete(sp.cache, key)
	return "VALUE_DELETED", true
}

func ClearExpiredKeys(kvs *KeyValueStore, sp *ServerProxy) {
	fmt.Println("ClearExpiredKeys func called")
	for {
		time.Sleep(2 * time.Second)
		kvs.mu.Lock()
		sp.mu.Lock()
		for key, value := range kvs.data {
			if time.Since(value.Timestamp) > DefaultTTL {
				delete(kvs.data, key)
				delete(sp.cache, key)
				fmt.Printf("Expired key '%s' deleted from cache and kvs\n", key)
			}
		}
		kvs.mu.Unlock()
		sp.mu.Unlock()
	}
}

// BackupFileName represents the name of the backup file
const BackupFileName = "backup.json"

// BackupSnapshot represents the snapshot of the key-value store's data
type BackupSnapshot struct {
	Data map[string]KeyValue `json:"data"`
}

func BackupKeyValueStore(kvs *KeyValueStore) {
	fmt.Println("BackupKeyValueStore func called")
	for {
		time.Sleep(5 * time.Second)
		kvs.mu.RLock()
		snapshot := BackupSnapshot{Data: kvs.data}
		kvs.mu.RUnlock()

		file, err := os.Create(BackupFileName)
		if err != nil {
			fmt.Println("Error creating backup file:", err)
			continue
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		if err := encoder.Encode(snapshot); err != nil {
			fmt.Println("Error encoding backup data:", err)
			continue
		}

		fmt.Println("Backup created successfully")
	}
}

func main() {
	fmt.Println("KEY-VALUE-STORE THAT CACHE KEY-VALUES, IT FETCHES VALUES FROM CACHE IF NOT IN CACHE THEN IT FETCHES FROM KEY-VALUE-STORE")
	kvs := NewKeyValueStore()
	proxy := NewServerProxy(kvs)
	ln, err := net.Listen("tcp", ":8081")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer ln.Close()

	go ClearExpiredKeys(kvs, proxy)
	go BackupKeyValueStore(kvs)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn, proxy)
	}
}

type Response struct {
	Value   string
	Message string
	Found   bool
	Success bool
}

func handleConnection(conn net.Conn, proxy *ServerProxy) {
	defer conn.Close()

	var request struct {
		Action string
		Key    string
		Value  string
	}
	decoder := gob.NewDecoder(conn)
	if err := decoder.Decode(&request); err != nil {
		fmt.Println("Error decoding request:", err)
		return
	}
	var response Response

	switch request.Action {
	case "GET":
		value, ok := proxy.GET(request.Key)
		response.Value = value
		response.Found = ok
	case "SET":
		proxy.SET(request.Key, request.Value)
		response.Success = true
	case "DELETE":
		value, ok := proxy.DELETE(request.Key)
		response.Success = ok
		response.Message = value
	case "UPDATE":
		value, ok := proxy.UPDATE(request.Key, request.Value)
		response.Success = ok
		response.Message = value
	default:
		fmt.Println("Invalid action:", request.Action)
	}

	encoder := gob.NewEncoder(conn)
	if err := encoder.Encode(response); err != nil {
		fmt.Println("Error encoding response:", err)
	}
}

//server side ( Decode karo , encode karo )
