package raft

type Entry struct {
	term int
	cmd  interface{}
}
