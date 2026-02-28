//go:build goexperiment.jsonv2 && windows

package spotify

import (
	"os/exec"
	"syscall"
)

// Windows process creation flags for hiding windows
const (
	// CREATE_NO_WINDOW prevents the process from creating a console window
	CREATE_NO_WINDOW = 0x08000000
	// DETACHED_PROCESS prevents the process from inheriting the parent's console
	DETACHED_PROCESS = 0x00000008
)

func modifyChromeCmd(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	// Prevent Chrome GUI window popups and console windows on Windows.
	// These flags ensure Chrome runs completely invisible even when not in headless mode.
	cmd.SysProcAttr.HideWindow = true
	cmd.SysProcAttr.CreationFlags = CREATE_NO_WINDOW | DETACHED_PROCESS
	// Ensure the process doesn't inherit our console
	cmd.SysProcAttr.NoInheritHandles = false
}
