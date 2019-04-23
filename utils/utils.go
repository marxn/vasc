package utils

import "bytes"
import "os/exec"
import "github.com/marxn/vasc/global" 

func ExecShellCmd(s string) (string, error) {
	cmd := exec.Command("/bin/bash", "-c", s)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	return out.String(), err
}