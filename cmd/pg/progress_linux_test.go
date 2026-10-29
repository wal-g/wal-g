package pg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/tracelog"
	"golang.org/x/sys/unix"
)

// Use a real terminal to exercise detection and log coordination together.
func TestInteractiveProgress(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("disabled=%t", disabled), func(t *testing.T) {
			t.Setenv("TERM", "xterm")
			master, terminal := openProgressTerminal(t)
			info, warning, errorLog, debug := tracelog.InfoLogger, tracelog.WarningLogger, tracelog.ErrorLogger, tracelog.DebugLogger
			t.Cleanup(func() {
				tracelog.InfoLogger, tracelog.WarningLogger, tracelog.ErrorLogger, tracelog.DebugLogger = info, warning, errorLog, debug
			})
			require.NoError(t, tracelog.Setup(terminal, tracelog.NormalLogLevel))
			cmd := &cobra.Command{Use: "backup-push"}
			addProgressFlag(cmd)
			// Without an explicit flag, even a terminal must retain the old output.
			if !disabled {
				require.NoError(t, cmd.Flags().Set(progressFlag, "true"))
			}
			ctx := context.Background()
			cmd.SetContext(ctx)
			cmd.SetErr(terminal)
			stop := startProgress(cmd)
			tracelog.InfoLogger.Println("backup message")
			stop()
			assert.Equal(t, ctx, cmd.Context())
			assert.Same(t, terminal, tracelog.InfoLogger.Writer())
			var output strings.Builder
			buffer := make([]byte, 4096)
			for {
				n, err := unix.Read(int(master.Fd()), buffer)
				if n > 0 {
					output.Write(buffer[:n])
				}
				if errors.Is(err, unix.EAGAIN) {
					break
				}
				require.NoError(t, err)
			}
			got := output.String()
			assert.Contains(t, got, "backup message\r\n")
			if disabled {
				assert.NotContains(t, got, "\x1b")
			} else {
				assert.Contains(t, got, "backup-push [")
				assert.Contains(t, got, "\r\x1b[2KINFO:")
			}
		})
	}
}

func TestProgressAdaptsToTerminalResize(t *testing.T) {
	t.Setenv("TERM", "xterm")
	master, terminal := openProgressTerminal(t)
	cmd := &cobra.Command{Use: "backup-push"}
	addProgressFlag(cmd)
	require.NoError(t, cmd.Flags().Set(progressFlag, "true"))
	cmd.SetContext(context.Background())
	cmd.SetErr(terminal)
	stop := startProgress(cmd)
	defer stop()
	buffer := make([]byte, 4096)
	// Drain the initial frame drawn at the original 120-column width.
	_, err := unix.Read(int(master.Fd()), buffer)
	require.NoError(t, err)
	require.NoError(t, unix.IoctlSetWinsize(int(terminal.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: 24, Col: 32}))
	fds := []unix.PollFd{{Fd: int32(master.Fd()), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, 3000)
	require.NoError(t, err)
	require.Positive(t, n, "no frame after resize")
	n, err = unix.Read(int(master.Fd()), buffer)
	require.NoError(t, err)
	parts := strings.Split(string(buffer[:n]), "\r\x1b[2K")
	frame := parts[len(parts)-1]
	assert.Less(t, len(frame), 32, "frame wraps in resized terminal: %q", frame)
}

func openProgressTerminal(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = master.Close() })
	require.NoError(t, unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0))
	number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	require.NoError(t, err)
	terminal, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR|unix.O_NOCTTY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = terminal.Close() })
	require.NoError(t, unix.IoctlSetWinsize(int(terminal.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: 24, Col: 120}))
	return master, terminal
}
