package main

import (
	"encoding/gob"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	DefaultTTL = 10 * time.Second // TTL set to 5 minutes for all keys
)

// KeyValue represents a key-value pair with a timestamp.
type KeyValue struct {
	Value     string
	Timestamp time.Time
}

// KeyValueStore represents an in-memory key-value store with TTL support.
type KeyValueStore struct {
	data map[string]KeyValue
	ttl  time.Duration
	mu   sync.RWMutex
}

// create instance of kvs
func NewKeyValueStore() *KeyValueStore {
	store := &KeyValueStore{
		data: make(map[string]KeyValue),
		ttl:  DefaultTTL,
	}
	return store
}

// GET func will fetch values from kvs
func (kvs *KeyValueStore) GET(key string) (string, bool) {
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()
	item, ok := kvs.data[key]
	if !ok {
		return "NOT_FOUND", false
	}
	return item.Value, true
}

// SET func will sets values in kvs
func (kvs *KeyValueStore) SET(key, value string) bool {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	kvs.data[key] = KeyValue{Value: value, Timestamp: time.Now()}
	return true
}

// Update func will update values in kvs
func (kvs *KeyValueStore) UPDATE(key, value string) bool {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	_, ok := kvs.data[key]
	if !ok {
		return false
	}
	kvs.data[key] = KeyValue{Value: value, Timestamp: time.Now()}
	return true
}

// Update func will delete values in kvs
func (kvs *KeyValueStore) DELETE(key string) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	if _, ok := kvs.data[key]; ok {
		delete(kvs.data, key)
		return nil
	}
	return fmt.Errorf("key '%s' not found", key)
}

// ServerProxy represents a server proxy that caches data and handles client requests.
type ServerProxy struct {
	kvs   *KeyValueStore
	cache map[string]KeyValue
	mu    sync.RWMutex
}

// create instance of serverproxy
func NewServerProxy(kvs *KeyValueStore) *ServerProxy {
	sp := &ServerProxy{
		kvs:   kvs,
		cache: make(map[string]KeyValue),
	}
	return sp
}

// GET func will fetch values from cache
func (sp *ServerProxy) GET(key string) (string, bool) {
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

// SET func will  set values in kvs ,  it will automatically sets when someone calls gets func
func (sp *ServerProxy) SET(key string, value string) bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	//sp.cache[key] = KeyValue{Value: value, Timestamp: time.Now()}
	sp.kvs.SET(key, value)
	return true
}

// Update func will update values in cache and kvs on conditions
func (sp *ServerProxy) UPDATE(key, value string) bool {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	_, ok := sp.kvs.data[key]
	if !ok {
		return false
	}
	sp.kvs.data[key] = KeyValue{Value: value, Timestamp: time.Now()}
	_, present := sp.cache[key]
	if present {
		sp.cache[key] = KeyValue{Value: value, Timestamp: time.Now()}
	}
	return true
}

// Update func will delete values in kvs
func (sp *ServerProxy) DELETE(key string) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	delete(sp.cache, key)
	return sp.kvs.DELETE(key)
}

func main() {
	fmt.Println("KEY-VALUE-STORE THAT CACHE KEY-VALUES , IT FETCHES VALUES FROM CACHE IF NOT IN CACHE THEN IT FETCHES FROM KEY-VALUE-STORE")
	kvs := NewKeyValueStore()
	proxy := NewServerProxy(kvs)
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			fmt.Println(err)
			continue
		}
		go handleConnection(conn, proxy)
		go ClearExpiredKeys(proxy, kvs, DefaultTTL)
	}
}

// this  func will clear all the expired keys from  cache and kvs
func ClearExpiredKeys(sp *ServerProxy, kvs *KeyValueStore, DefaultTTL time.Duration) {
	fmt.Println("cleanExpiredKeys func called")
	for {
		fmt.Println("cleanExpiredKeys func called inside for loop ")
		time.Sleep(5 * time.Second)
		kvs.mu.Lock()
		sp.mu.Lock()
		for key, value := range kvs.data {
			if time.Since(value.Timestamp) > DefaultTTL {
				delete(kvs.data, key)
				delete(sp.cache, key)
			}
		}
		kvs.mu.Unlock()
		sp.mu.Unlock()
	}
}

// Response represents the response structure from the server.
type Response struct {
	Value   string
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
		proxy.DELETE(request.Key)
		response.Success = true
	case "UPDATE":
		success := proxy.UPDATE(request.Key, request.Value)
		response.Success = success
	default:
		fmt.Println("Invalid action:", request.Action)
	}

	encoder := gob.NewEncoder(conn)
	if err := encoder.Encode(response); err != nil {
		fmt.Println("Error encoding response:", err)
	}
}
