package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

type RawMongoOp struct {
	OP  string          `json:"op,omitempty"`
	DB  string          `json:"db,omitempty"`
	ID  int             `json:"id,omitempty"`
	Cmd json.RawMessage `json:"dc,omitempty"`
}

func ReadRawStage(ctx context.Context, r io.Reader, size int, wg *sync.WaitGroup) (<-chan RawMongoOp, <-chan error, error) {
	cmds := make(chan RawMongoOp, size)
	errc := make(chan error, 1)
	dec := json.NewDecoder(r)

	t, err := dec.Token()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse patron json: %v", err)
	}

	if t != json.Delim('[') {
		return nil, nil, fmt.Errorf("expected the begging of the array of commands")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(cmds)
		defer close(errc)

		for dec.More() {
			var cmd RawMongoOp
			if err := dec.Decode(&cmd); err != nil {
				errc <- fmt.Errorf("cannot parse command from patron: %v", err)
				return
			}
			select {
			case cmds <- cmd:
			case <-ctx.Done():
				return
			}
		}

		if _, err = dec.Token(); err != nil {
			errc <- fmt.Errorf("expected the end of the array of commands")
			return
		}
	}()

	return cmds, errc, nil
}
