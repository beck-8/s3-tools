package main

var CurrentCommit string

// BuildVersion is the local build version
const BuildVersion = "0.0.1"

func UserVersion() string {
	return BuildVersion + "+git." + CurrentCommit
}
