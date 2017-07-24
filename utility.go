package walg

import (
	"fmt"
	"time"
)

type BackupTime struct {
	Name string
	Time time.Time
}

type TimeSlice []BackupTime

func (p TimeSlice) Len() int {
	return len(p)
}

func (p TimeSlice) Less(i, j int) bool {
	return p[i].Time.After(p[j].Time)
}

func (p TimeSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

//defer timeTrack(time.Now(), "EXTRACT ALL")
func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
}
