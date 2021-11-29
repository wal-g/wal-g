package libsodium

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/wal-g/tracelog"
)

type keyTransformRegEntry struct {
	typ string
	fun func(userInput string) ([]byte, error)
}

var keyTransformReg = []keyTransformRegEntry{
	{typ: "base64", fun: keyTransformBase64},
	{typ: "hex", fun: keyTransformHex},
	{typ: "none", fun: func(userInput string) ([]byte, error) { return keyTransformLegacy(userInput, true) }},
}

func keyTransform(userInput string, transformType string, expectedLen int) ([]byte, error) {
	var err error
	var decoded []byte

	for _, entry := range keyTransformReg {
		if entry.typ == transformType {
			decoded, err = entry.fun(userInput)
			if err != nil {
				return nil, err
			}

			if len(decoded) != libsodiumKeybytes {
				return nil, fmt.Errorf("key must be exactly %d bytes (got %d bytes)", libsodiumKeybytes, len(decoded))
			}

			return decoded, nil
		}
	}

	// unknown transform
	var builder strings.Builder
	for idx, entry := range keyTransformReg {
		if idx > 0 {
			if idx+1 == len(keyTransformReg) {
				builder.WriteString(" or ")
			} else {
				builder.WriteString(", ")
			}
		}

		builder.WriteString(entry.typ)
	}

	return nil, fmt.Errorf("unknown key transform '%s' (must be %s)", transformType, builder.String())
}

func keyTransformBase64(userInput string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(userInput)
	if err != nil {
		return nil, fmt.Errorf("while base64 decoding key: %v", err)
	}

	return decoded, nil
}

func keyTransformHex(userInput string) ([]byte, error) {
	decoded, err := hex.DecodeString(userInput)
	if err != nil {
		return nil, fmt.Errorf("while hex decoding key: %v", err)
	}

	return decoded, nil
}

// Mimics the behaviour of older versions of wal-g.
func keyTransformLegacy(userInput string, displayWarning bool) ([]byte, error) {
	if len(userInput) < minimalKeyLength {
		return nil, newErrShortKey(len(userInput))
	}

	if len(userInput) > libsodiumKeybytes {
		if displayWarning {
			tracelog.WarningLogger.Println("libsodium keys must be exactly %d bytes, your key exceeds that length and will be truncated", libsodiumKeybytes)
		}
		return []byte(userInput[:libsodiumKeybytes]), nil
	}

	if len(userInput) < libsodiumKeybytes {
		if displayWarning {
			tracelog.WarningLogger.Println("libsodium keys must be exactly %d bytes, your key will be padded to the right with zero bytes", libsodiumKeybytes)
		}
		buf := make([]byte, libsodiumKeybytes)
		copy(buf[:libsodiumKeybytes], userInput)
		return buf, nil
	}

	return []byte(userInput), nil
}
