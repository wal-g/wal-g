package stats

import "math"

type OperationWeight float64

var (
	OperationExists OperationWeight = 1_000

	OperationList OperationWeight = 2_000

	OperationRead = func(fileSize int64) OperationWeight {
		return scaleWeight(1_000, sizeInMB(fileSize))
	}

	OperationPut = func(fileSize int64) OperationWeight {
		return scaleWeight(1_000, sizeInMB(fileSize))
	}

	OperationDelete = func(files int) OperationWeight {
		return scaleWeight(500, files)
	}

	OperationCopy OperationWeight = 2_000
)

// scaleWeight logarithmically. So, if the scale is 1 or less, the weight doesn't change. If the scale is 10, the weight
// doubles. If 100, triples. And so on.
func scaleWeight(weightForScale OperationWeight, scale int) OperationWeight {
	if scale <= 1 {
		return weightForScale
	}
	scaleFloat := 1 + math.Log10(float64(scale))
	metricForFileSize := float64(weightForScale) * scaleFloat
	return OperationWeight(metricForFileSize)
}

const megaByte = 1024 * 1024

func sizeInMB(sizeInBytes int64) int {
	return int(sizeInBytes / megaByte)
}
