package cache

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var p = &DefaultEMAParams

var logStorageStatuses = false

func TestKey(t *testing.T) {
	t.Run("make string and parse", func(t *testing.T) {
		key := Key{Name: "some_name", Hash: "some_hash"}
		str := key.String()
		parsedKey := ParseKey(str)
		assert.Equal(t, key, parsedKey)
	})

	t.Run("works with # in name", func(t *testing.T) {
		key := Key{Name: "some#name", Hash: "some_hash"}
		str := key.String()
		parsedKey := ParseKey(str)
		assert.Equal(t, key, parsedKey)
	})

	t.Run("does not work with # in hash", func(t *testing.T) {
		key := Key{Name: "some_name", Hash: "some#hash"}
		str := key.String()
		parsedKey := ParseKey(str)
		assert.NotEqual(t, key, parsedKey)
	})
}

func Test_storageStatus_alive(t *testing.T) {
	tests := []struct {
		name      string
		actual    Aliveness
		potential Aliveness
		wasAlive  bool
		nowAlive  bool
	}{
		{"now alive when was alive and dropped to 88", 88, 100, true, true},
		{"now alive when was dead and reached 99", 99, 100, false, true},
		{"now dead when was alive and dropped to 87", 87, 100, true, false},
		{"now dead when was dead and reached 98", 98, 100, false, false},
		{"now alive when was alive with 0 potential", 0, 0, true, true},
		{"now dead when was dead with 0 potential", 0, 0, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := storageStatus{
				PotentialAliveness: tt.potential,
				ActualAliveness:    tt.actual,
				WasAlive:           tt.wasAlive,
			}
			alive := s.alive(p)
			assert.Equalf(t, tt.nowAlive, alive, "{%v, %v, %v}.alive()", tt.potential, tt.actual, tt.wasAlive)
		})
	}
}

func Test_storageStatus_applyExplicitCheckResult(t *testing.T) {
	now := time.Now()

	t.Run("apply alive status", func(t *testing.T) {
		s := storageStatus{PotentialAliveness: 100, ActualAliveness: 0, WasAlive: false, Updated: time.Time{}}
		s = s.applyExplicitCheckResult(true, now)
		assert.Equal(t, Aliveness(100), s.PotentialAliveness)
		assert.Equal(t, Aliveness(100), s.ActualAliveness)
		assert.Equal(t, true, s.WasAlive)
		assert.Equal(t, now, s.Updated)
	})

	t.Run("apply dead status", func(t *testing.T) {
		s := storageStatus{PotentialAliveness: 100, ActualAliveness: 100, WasAlive: true, Updated: time.Time{}}
		s = s.applyExplicitCheckResult(false, now)
		assert.Equal(t, Aliveness(100), s.PotentialAliveness)
		assert.Equal(t, Aliveness(0), s.ActualAliveness)
		assert.Equal(t, false, s.WasAlive)
		assert.Equal(t, now, s.Updated)
	})
}

func Test_storageStatus_applyOperationResult(t *testing.T) {
	logStatus := func(idx int, s storageStatus) {
		if !logStorageStatuses {
			return
		}
		alive := s.alive(p)
		alivenessFactor := s.alivenessFactor(p)
		fmt.Printf("#%-5d: alive: %v,\taliveness: %8.6f\n", idx, alive, alivenessFactor)
	}

	t.Run("make dead status alive after 20 successful operations", func(t *testing.T) {
		s := deadStatus()
		fmt.Printf("Initial status\n")
		logStatus(0, s)

		iteration := 0
		fmt.Printf("\nMonitor status changes\n")
		for {
			now := time.Now()
			iteration++
			newS := s.applyOperationResult(p, true, 100, now)
			logStatus(iteration, newS)
			assert.Greater(t, newS.ActualAliveness, s.ActualAliveness)
			assert.True(t, almostEqual(newS.PotentialAliveness, s.PotentialAliveness, 0.001))
			assert.Equal(t, newS.WasAlive, s.alive(p))
			assert.Equal(t, now, newS.Updated)

			if newS.alive(p) {
				break
			}
			s = newS
		}

		assert.Equal(t, 28, iteration)
	})

	t.Run("make alive status dead after 10 unsuccessful operations", func(t *testing.T) {
		s := aliveStatus()
		fmt.Printf("Initial status\n")
		logStatus(0, s)

		iteration := 0
		fmt.Printf("\nMonitor status changes\n")
		for {
			now := time.Now()
			iteration++
			newS := s.applyOperationResult(p, false, 100, now)
			logStatus(iteration, newS)
			assert.Less(t, newS.ActualAliveness, s.ActualAliveness)
			assert.True(t, almostEqual(newS.PotentialAliveness, s.PotentialAliveness, 0.001))
			assert.Equal(t, newS.WasAlive, s.alive(p))
			assert.Equal(t, now, newS.Updated)

			if !newS.alive(p) {
				break
			}
			s = newS
		}

		assert.Equal(t, 6, iteration)
	})

	t.Run("preserves alive status with 10% of failures", func(t *testing.T) {
		s := aliveStatus()
		fmt.Printf("Initial status\n")
		logStatus(0, s)
		success := func(i int) bool { return i%10 != 0 }

		minAliveness := math.MaxFloat64
		fmt.Printf("\nMonitor status changes\n")
		for i := 1; i <= 1000; i++ {
			s = s.applyOperationResult(p, success(i), 100, time.Now())
			logStatus(i, s)
			if s.alivenessFactor(p) < minAliveness {
				minAliveness = s.alivenessFactor(p)
			}
			assert.True(t, s.alive(p))
		}
		fmt.Printf("Min aliveness: %v\n", minAliveness)
	})

	t.Run("makes alive status dead with 12.5% of failures", func(t *testing.T) {
		s := aliveStatus()
		fmt.Printf("Initial status\n")
		logStatus(0, s)
		success := func(i int) bool { return i%8 != 0 }

		becomeDead := false
		becomeAliveAgain := false
		minAliveness := math.MaxFloat64
		fmt.Printf("\nMonitor status changes\n")
		for i := 1; i <= 1000; i++ {
			s = s.applyOperationResult(p, success(i), 100, time.Now())
			logStatus(i, s)
			if s.alivenessFactor(p) < minAliveness {
				minAliveness = s.alivenessFactor(p)
			}
			if !becomeDead && !s.alive(p) {
				becomeDead = true
			}
			if !becomeAliveAgain && becomeDead && s.alive(p) {
				becomeAliveAgain = true
			}
		}
		fmt.Printf("Min aliveness: %v\n", minAliveness)

		assert.True(t, becomeDead, "not become dead")
		assert.False(t, becomeAliveAgain, "become alive again")
	})

	t.Run("preserves dead status with 5% of failures", func(t *testing.T) {
		s := deadStatus()
		fmt.Printf("Initial status\n")
		logStatus(0, s)
		success := func(i int) bool { return i%20 != 0 }

		maxAliveness := 0.0
		fmt.Printf("\nMonitor status changes\n")
		for i := 1; i <= 100; i++ {
			s = s.applyOperationResult(p, success(i), 100, time.Now())
			logStatus(i, s)
			if s.alivenessFactor(p) > maxAliveness {
				maxAliveness = s.alivenessFactor(p)
			}
			assert.False(t, s.alive(p))
		}
		fmt.Printf("Max aliveness: %v\n", maxAliveness)
	})

	t.Run("makes dead status alive with 4% of failures", func(t *testing.T) {
		s := deadStatus()
		fmt.Printf("Initial status\n")
		logStatus(0, s)
		success := func(i int) bool { return i%25 != 0 }

		becomeAlive := false
		becomeDeadAgain := false
		maxAliveness := 0.0
		fmt.Printf("\nMonitor status changes\n")
		for i := 1; i <= 1000; i++ {
			s = s.applyOperationResult(p, success(i), 100, time.Now())
			logStatus(i, s)
			if s.alivenessFactor(p) > maxAliveness {
				maxAliveness = s.alivenessFactor(p)
			}
			if !becomeAlive && s.alive(p) {
				becomeAlive = true
			}
			if !becomeDeadAgain && becomeAlive && !s.alive(p) {
				becomeDeadAgain = true
			}
		}
		fmt.Printf("Max aliveness: %v\n", maxAliveness)

		assert.True(t, becomeAlive, "not become alive")
		assert.False(t, becomeDeadAgain, "become dead again")
	})

	t.Run("works with different weights", func(t *testing.T) {
		weights := [10]float64{1000, 2000, 500, 3000, 1000, 2000, 1500, 2500, 500, 1000}
		alives := [10]bool{false, true, true, false, true, false, true, false, false, true}
		s := aliveStatus()

		for i := 0; i < 10; i++ {
			now := time.Now()
			newS := s.applyOperationResult(p, alives[i], weights[i], now)
			fmt.Printf("applied result %v with weight %f\n", alives[i], weights[i])

			if alives[i] {
				assert.Greater(t, newS.alivenessFactor(p), s.alivenessFactor(p))
			} else {
				assert.Less(t, newS.alivenessFactor(p), s.alivenessFactor(p))
			}

			fmt.Printf(
				"#%2d: aliveness:\t%#6.2f actual\t\t%#6.2f potential\t\t%#5.1f %%\n",
				i+1,
				newS.ActualAliveness,
				newS.PotentialAliveness,
				newS.ActualAliveness/newS.PotentialAliveness*100.0,
			)

			s = newS
		}

		assert.False(t, s.alive(p))
	})
}

func almostEqual(a, b Aliveness, inaccuracy float64) bool {
	return math.Abs(float64(a)-float64(b)) <= inaccuracy
}

var keys = []Key{{Name: "0"}, {Name: "1"}, {Name: "2"}, {Name: "3"}}

func Test_storageStatuses_applyExplicitCheckResult(t *testing.T) {
	t.Run("update existsing statuses", func(t *testing.T) {
		ss := storageStatuses{
			keys[0]: aliveStatus(),
			keys[1]: aliveStatus(),
			keys[2]: deadStatus(),
			keys[3]: deadStatus(),
		}
		ssCpy := storageStatuses{}
		for k, v := range ss {
			ssCpy[k] = v
		}

		checkResult := map[Key]bool{
			keys[0]: true,
			keys[1]: false,
			keys[2]: true,
			keys[3]: false,
		}
		newSS := ss.applyExplicitCheckResult(checkResult, time.Now())

		t.Run("applies to all statuses", func(t *testing.T) {
			assert.Equal(t, true, newSS[keys[0]].alive(p))
			assert.Equal(t, false, newSS[keys[1]].alive(p))
			assert.Equal(t, true, newSS[keys[2]].alive(p))
			assert.Equal(t, false, newSS[keys[3]].alive(p))
		})

		t.Run("updates checking times", func(t *testing.T) {
			for i := 0; i < 4; i++ {
				assert.NotEqual(t, newSS[keys[i]].Updated.Unix(), time.Time{}.Unix())
			}
		})

		t.Run("does not change source map", func(t *testing.T) {
			assert.Equal(t, ssCpy, ss)
		})
	})

	t.Run("applies to empty", func(t *testing.T) {
		ss := storageStatuses{}
		checkResult := map[Key]bool{keys[0]: true}
		newSS := ss.applyExplicitCheckResult(checkResult, time.Now())

		assert.Len(t, newSS, 1)
	})
}

func Test_storageStatuses_isRelevant(t *testing.T) {
	t.Run("unrelevant if nil", func(t *testing.T) {
		var ss storageStatuses = nil
		relevant := ss.isRelevant(time.Hour, keys[0], keys[1])
		assert.False(t, relevant)
	})

	t.Run("unrelevant if empy", func(t *testing.T) {
		ss := storageStatuses{}
		relevant := ss.isRelevant(time.Hour, keys[0], keys[1])
		assert.False(t, relevant)
	})

	t.Run("unrelevant if any requested does not exist", func(t *testing.T) {
		ss := storageStatuses{
			keys[0]: relevantStatus(),
			keys[1]: relevantStatus(),
			keys[3]: relevantStatus(),
		}
		relevant := ss.isRelevant(time.Hour, keys[0], keys[1], keys[2])
		assert.False(t, relevant)
	})

	t.Run("unrelevant if any requested is outdated", func(t *testing.T) {
		ss := storageStatuses{
			keys[0]: relevantStatus(),
			keys[1]: relevantStatus(),
			keys[2]: outdatedStatus(),
			keys[3]: relevantStatus(),
		}
		relevant := ss.isRelevant(time.Hour, keys[0], keys[1], keys[2])
		assert.False(t, relevant)
	})

	t.Run("relevant if all requested are relevant", func(t *testing.T) {
		ss := storageStatuses{
			keys[0]: relevantStatus(),
			keys[1]: relevantStatus(),
			keys[2]: relevantStatus(),
			keys[3]: outdatedStatus(),
		}
		relevant := ss.isRelevant(time.Hour, keys[0], keys[1], keys[2])
		assert.True(t, relevant)
	})
}

func Test_statusCache_splitByRelevance(t *testing.T) {
	t.Run("all are outdated if nil", func(t *testing.T) {
		var ss storageStatuses = nil
		relevant, outdated := ss.splitByRelevance(time.Hour, []Key{keys[0], keys[1], keys[2]})
		assert.Empty(t, relevant)
		assert.Len(t, outdated, 3)
	})

	t.Run("all are outdated if empty", func(t *testing.T) {
		ss := storageStatuses{}
		relevant, outdated := ss.splitByRelevance(time.Hour, []Key{keys[0], keys[1], keys[2]})
		assert.Empty(t, relevant)
		assert.Len(t, outdated, 3)
	})

	t.Run("split requested keys by relevance", func(t *testing.T) {
		ss := storageStatuses{
			keys[0]: relevantStatus(),
			keys[1]: outdatedStatus(),
			keys[2]: relevantStatus(),
			keys[3]: outdatedStatus(),
		}
		relevant, outdated := ss.splitByRelevance(time.Hour, []Key{keys[0], keys[1], keys[2]})
		wantRelevant := storageStatuses{
			keys[0]: ss[keys[0]],
			keys[2]: ss[keys[2]],
		}
		wantOutdated := storageStatuses{
			keys[1]: ss[keys[1]],
		}
		assert.Equal(t, wantRelevant, relevant)
		assert.Equal(t, wantOutdated, outdated)
	})
}

func Test_storageStatuses_filter(t *testing.T) {
	t.Run("works with nil statuses", func(t *testing.T) {
		var ss storageStatuses = nil
		newSS := ss.filter([]Key{keys[0]})
		assert.Len(t, newSS, 0)
	})

	t.Run("works with empty statuses", func(t *testing.T) {
		ss := storageStatuses{}
		newSS := ss.filter([]Key{keys[0]})
		assert.Len(t, newSS, 0)
	})

	t.Run("returns only requested keys", func(t *testing.T) {
		ss := storageStatuses{
			keys[0]: aliveStatus(),
			keys[1]: deadStatus(),
			keys[3]: outdatedStatus(),
		}
		newSS := ss.filter([]Key{keys[0], keys[1], keys[2]})
		wantSS := storageStatuses{
			keys[0]: ss[keys[0]],
			keys[1]: ss[keys[1]],
		}
		assert.Equal(t, wantSS, newSS)
	})
}

func Test_storageStatuses_aliveMap(t *testing.T) {
	t.Run("works with nil statuses", func(t *testing.T) {
		var ss storageStatuses = nil
		gotAM := ss.aliveMap(p)
		assert.Len(t, gotAM, 0)
	})

	t.Run("works with empty statuses", func(t *testing.T) {
		ss := storageStatuses{}
		gotAM := ss.aliveMap(p)
		assert.Len(t, gotAM, 0)
	})

	t.Run("provides map with aliveness by names", func(t *testing.T) {
		ss := storageStatuses{
			keys[0]: aliveStatus(),
			keys[1]: deadStatus(),
			keys[2]: aliveStatus(),
			keys[3]: deadStatus(),
		}
		gotAM := ss.aliveMap(p)
		wantAM := AliveMap{
			keys[0].Name: true,
			keys[1].Name: false,
			keys[2].Name: true,
			keys[3].Name: false,
		}
		assert.Equal(t, wantAM, gotAM)
	})
}

func Test_mergeByRelevance(t *testing.T) {
	t.Run("takes more relevant statuses", func(t *testing.T) {
		a := storageStatuses{
			keys[0]: relevantStatus(),
			keys[1]: outdatedStatus(),
			keys[2]: outdatedStatus(),
		}
		b := storageStatuses{
			keys[0]: outdatedStatus(),
			keys[1]: relevantStatus(),
			keys[3]: relevantStatus(),
		}
		got := mergeByRelevance(a, b)
		want := storageStatuses{
			keys[0]: a[keys[0]],
			keys[1]: b[keys[1]],
			keys[2]: a[keys[2]],
			keys[3]: b[keys[3]],
		}
		assert.Equal(t, want, got)
	})

	t.Run("consider nil statuses empty", func(t *testing.T) {
		var a storageStatuses
		var b storageStatuses

		a, b = nil, storageStatuses{
			keys[0]: outdatedStatus(),
			keys[1]: relevantStatus(),
		}
		got := mergeByRelevance(a, b)
		want := storageStatuses{
			keys[0]: b[keys[0]],
			keys[1]: b[keys[1]],
		}
		assert.Equal(t, want, got)

		a, b = storageStatuses{
			keys[0]: outdatedStatus(),
			keys[1]: relevantStatus(),
		}, nil
		got = mergeByRelevance(a, b)
		want = storageStatuses{
			keys[0]: a[keys[0]],
			keys[1]: a[keys[1]],
		}
		assert.Equal(t, want, got)
	})
}

func aliveStatus() storageStatus {
	return status(true, false)
}

func deadStatus() storageStatus {
	return status(false, false)
}

func relevantStatus() storageStatus {
	return status(false, true)
}

func outdatedStatus() storageStatus {
	return status(false, false)
}

func status(alive, relevant bool) storageStatus {
	s := storageStatus{}

	if alive {
		s.PotentialAliveness = 100
		s.ActualAliveness = 100
		s.WasAlive = true
	} else {
		s.PotentialAliveness = 100
		s.ActualAliveness = 0
		s.WasAlive = false
	}

	if relevant {
		s.Updated = time.Now()
	} else {
		s.Updated = time.Time{}
	}
	return s
}
