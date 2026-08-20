package sysutil

import "testing"

func TestChangeSSHPortIsPermanentlyDisabled(t *testing.T) {
	if err := ChangeSSHPort("2022"); err == nil {
		t.Fatal("ChangeSSHPort must never succeed in the performance fork")
	}
}
