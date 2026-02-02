// SIGUSR1 on Windows is set by PostgreSQL itself to 30.
// https://github.com/postgres/postgres/blob/39dcfda2d23ac39f14ecf4b83e01eae85d07d9e5/src/include/port/win32_port.h#L170

// The various POSIX-purpose signal values do not exist natively on Windows.
// Since Go uniformly supports os.Signal, the integer values for signals may
// differ across platforms or may not even exist as a uniform variant.
// PostgreSQL encountered this variability in 2003 when they began building on
// more Windows/GNU platforms beyond their original Cygwin target, such as
// MinGW.
// https://github.com/postgres/postgres/commit/12c942383296bd626131241c012c2ab81b081738
//
// To further reassure that, yes, this is chaos and correct, consider, as
// Meng Zhuo observed [1] while reviewing proposed code to add the SIGUSR1
// signal ID to Windows syscall types, MinGW does not even define SIGUSR1.
// Furthermore, the original addition of Signal support to Windows in Go was
// intended only to support SIGKILL ([2]) and led to extensive discussion about
// how Signal is a UNIX/POSIX construct being indirectly forced into Go and
// Windows ([3]). Even Cygwin has four different variants of the signal. [4]
//
// In 2018, Windows added support for UNIX sockets [5][6]; however, the
// standard Linux signals [7] remain out of scope and are dependent on the
// build platform.
//
// [1]: https://go-review.googlesource.com/c/go/+/390695/1/src/syscall/types_windows.go#95
// [2]: https://codereview.appspot.com/4437091/diff/37002/src/pkg/os/exec_windows.go#newcode32
// [3]: https://codereview.appspot.com/4437091
// [4]: https://github.com/cygwin/cygwin/blob/c43ec5f5951c7f4b882a0f8e619601a45ae70a91/newlib/libc/include/sys/signal.h#L258-L371
// [5]: https://devblogs.microsoft.com/commandline/af_unix-comes-to-windows
// [6]: https://man7.org/linux/man-pages/man7/unix.7.html
// [7]: https://man7.org/linux/man-pages/man7/signal.7.html

package postgres

import "syscall"

// SIGUSR1 PostgreSQL win32_port.h uses 30
const SIGUSR1 = syscall.Signal(30)
