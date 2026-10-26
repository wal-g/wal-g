package postgres_test

import "testing"

func TestWalSendCLIStandby64MB(t *testing.T) { walSendCLIStandby(t, 64) }
