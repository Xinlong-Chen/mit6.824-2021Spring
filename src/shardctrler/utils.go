package shardctrler

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const debug = false

type logTopic string

const (
	dClient logTopic = "CLNT"
	dError  logTopic = "ERRO"
	dInfo   logTopic = "INFO"
	dLog    logTopic = "LOG1"
	dLog2   logTopic = "LOG2"
	dTest   logTopic = "TEST"
	dTrace  logTopic = "TRCE"
	dWarn   logTopic = "WARN"
	dServer logTopic = "SEVR"
)

var debugStart time.Time

func init() {
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
