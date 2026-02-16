//go:build goexperiment.jsonv2 && !windows

package spotify

import "os/exec"

func modifyChromeCmd(*exec.Cmd) {}
