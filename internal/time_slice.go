package internal

// TimeSlice represents a backup and its
// last modified time.
type TimeSlice []BackupTime

func (timeSlice TimeSlice) Len() int {
	return len(timeSlice)
}

func (timeSlice TimeSlice) Less(i, j int) bool {
	return timeSlice[i].Time.After(timeSlice[j].Time)
}

func (timeSlice TimeSlice) Swap(i, j int) {
	timeSlice[i], timeSlice[j] = timeSlice[j], timeSlice[i]
}
