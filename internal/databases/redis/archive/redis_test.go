package archive

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnsureRestoreCompatibility(t *testing.T) {
	data := []map[string]interface{}{
		{
			"b":    "7.0.15",
			"r":    "17.0.15",
			"res":  true,
			"errr": nil,
		},
		{
			"b":    "17.0.15",
			"r":    "7.0.15",
			"res":  false,
			"errr": nil,
		},
		{
			"b":    "6.2.15",
			"r":    "6.4.18",
			"res":  true,
			"errr": nil,
		},
		{
			"b":    "6.4.19",
			"r":    "6.4.18",
			"res":  false,
			"errr": nil,
		},
		{
			"b":    "6.4.18",
			"r":    "6.4.18",
			"res":  true,
			"errr": nil,
		},
	}

	for _, test := range data {
		res, err := EnsureRestoreCompatibility(test["b"].(string), test["r"].(string))
		assert.Equal(t, res, test["res"].(bool))
		assert.Equal(t, err, test["errr"])
	}
}
