package main

import (
	"encoding/gob"
	"fmt"
	"net"
)

// Response represents the response structure from the server.
type Response struct {
	Value   string
	Found   bool
	Success bool
}

// Client represents a client that communicates with the server.
type Client struct{}

// SendRequest sends a request to the server and returns the response.
func (c *Client) SendRequest(action, key, value string) (string, bool) {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return "", false
	}
	defer conn.Close()

	request := struct {
		Action string
		Key    string
		Value  string
	}{Action: action, Key: key, Value: value}

	encoder := gob.NewEncoder(conn)
	if err := encoder.Encode(request); err != nil {
		fmt.Println("Error encoding request:", err)
		return "", false
	}

	var response Response
	decoder := gob.NewDecoder(conn)
	if err := decoder.Decode(&response); err != nil {
		fmt.Println("Error decoding response:", err)
		return "", false
	}

	return response.Value, response.Found
}

func main() {
	client := &Client{}

	// Example usage
	//client.SendRequest("SET", "name", "John")
	//client.SendRequest("SET", "name8", "paytm")
	value, found := client.SendRequest("GET", "name", "")
	if found {
		fmt.Println("Name:", value)
	} else {
		fmt.Println("Name not found")
	}
	value1, found1 := client.SendRequest("GET", "name8", "")
	if found1 {
		fmt.Println("Name:", value1)
	} else {
		fmt.Println("Name not found")
	}

	// client.SendRequest("UPDATE", "name", "Alice")
	// value, found = client.SendRequest("GET", "name", "")
	// if found {
	// 	fmt.Println("Updated Name:", value)
	// } else {
	// 	fmt.Println("Name not found")
	// }
}
