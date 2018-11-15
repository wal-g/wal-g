package walg

import (
	"log"
	"os"
)

var infoLogger = log.New(os.Stderr, "INFO: ", log.LstdFlags|log.Lmicroseconds)
var warningLogger = log.New(os.Stderr, "WARNING: ", log.LstdFlags|log.Lmicroseconds)
var errorLogger = log.New(os.Stderr, "ERROR: ", log.LstdFlags|log.Lmicroseconds)
