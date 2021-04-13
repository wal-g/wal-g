package internal

/*
This object represents data about a physical slot.
The data can be retrieved from Postgres with the queryRunner,
and is consumed by the walReceiveHandler.
*/

import (
	"regexp"

	"github.com/jackc/pglogrepl"
	"github.com/pkg/errors"
)

// The PhysicalSlot represents a Physical Replication Slot.
type PhysicalSlot struct {
	Name       string
	Exists     bool
	Active     bool
	RestartLSN pglogrepl.LSN
}

//NewPhysicalSlot is a helper function to declare a new PhysicalSlot object and set vaues from the parsed arguments
func NewPhysicalSlot(name string, exists bool, active bool, restartLSN string) (slot PhysicalSlot, err error) {
	err = ValidateSlotName(name)
	if err != nil {
		return
	}
	slot.Name = name
	slot.Exists = exists
	slot.Active = active
	if exists {
		slot.RestartLSN, err = pglogrepl.ParseLSN(restartLSN)
	}
	return
}

// ValidateSlotName validates pgSlotName to be a valid slot name
func ValidateSlotName(pgSlotName string) (err error) {
	// Check WALG_SLOTNAME env variable (can be any of [0-9A-Za-z_], and 1-63 characters long)
	invalid, err := regexp.MatchString(`\W`, pgSlotName)
	if err != nil {
		return
	}
	if len(pgSlotName) > 63 || invalid {
		err = genericWalReceiveError{
			errors.Errorf("Slot name (%s) can only contain 1-63 word characters ([0-9A-Za-z_])",
				pgSlotName)}
	}
	return
}
