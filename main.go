package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("tfclient starting...")

	if len(os.Args) > 1 {
		fmt.Printf("Args: %v\n", os.Args[1:])
	}
}
