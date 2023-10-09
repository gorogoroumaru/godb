package main

import (
	"bufio"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func runCommandInput(command string, args ...string) (string, error) {
	cmd := exec.Command(command, args...)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()

	reader := bufio.NewReader(stdout)

	cmd.Start()

	for i := 1; i <= 14; i++ {
		input := fmt.Sprintf("insert %d user%d person%d@example.com\n", i, i, i)
		stdin.Write([]byte(input))
		time.Sleep(100 * time.Microsecond)

		reader.ReadString('\n')
		reader.ReadString('\n')
		reader.ReadString('\n')
	}


	stdin.Write([]byte(".btree\n"))
	time.Sleep(100 * time.Microsecond)

	var output []byte

	for i := 0; i < 19; i++ {
		time.Sleep(100 * time.Microsecond)
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		output = append(output, line...)
	}

	stdin.Write([]byte(".exit\n"))
	time.Sleep(100 * time.Microsecond)

	return string(output), nil
}

func Test_godb(t *testing.T) {
	command := "./godb"
	arg := "./my.db"

	output, err := runCommandInput(command, arg)
	if err != nil {
		t.Errorf("Error executing command: %v", err)
	}

	expected := []string{"db > Tree: ",
	    "- internal (size 1)",
	    "  - leaf (size 7)",
	    "    - 1",
		"    - 2",
		"    - 3",
		"    - 4",
		"    - 5",
	    "    - 6",
	    "    - 7",
	    "  - key 7",
    	"  - leaf (size 7)",
	    "    - 8",
	    "    - 9",
	    "    - 10",
	    "    - 11",
        "    - 12",
	    "    - 13",
	    "    - 14"}

	outputStr := strings.Split(output, "\n")

	for i := 0; i < len(expected); i++ {
		if outputStr[i] != expected[i] {
			t.Errorf("Output is not equal to expected: %v", err)
		}
	}

	return
}
