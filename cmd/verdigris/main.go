package main

import (
	"fmt"
	"os"
)

// version is a placeholder. It will be populated from build metadata in a
// later milestone. For now it exists so the binary has something to print
// and so the build can be verified end to end.
const version = "0.0.0-scaffold"

func main() {
	fmt.Fprintf(os.Stdout, "verdigris %s\n", version)
}
