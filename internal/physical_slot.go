package internal

/*
This object represents data about a physical slot.
The data can be retrieved from Postgres with the queryRunner,
and is consumed by the walReceiveHandler.
*/

import(
  "github.com/jackc/pglogrepl"
)

// The PhysicalSlot represents a Physical Replication Slot.
type PhysicalSlot struct {
	Name       string
	Exists     bool
	Active     bool
	RestartLSN pglogrepl.LSN
}

//NewPhysicalSlot is a helper function to declare a new PhysicalSlot object and set vaues from the parsed arguments
func NewPhysicalSlot(name string, exists bool, active bool, restartLSN string) (PhysicalSlot, error) {
  var err error
  slot := PhysicalSlot{Name: name, Exists: exists, Active: active}
  if exists {
    slot.RestartLSN, err = pglogrepl.ParseLSN(restartLSN)
  }
  return slot, err
}
