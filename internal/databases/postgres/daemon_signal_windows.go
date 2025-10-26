// SIGUSR1 on windows is set by postgres itself to 30.
// https://github.com/postgres/postgres/blob/39dcfda2d23ac39f14ecf4b83e01eae85d07d9e5/src/include/port/win32_port.h#L170

// Signals do not really exist in windows so even though Go uniformly supports os.Signal the integers on any system
// may be different because go covers systems that may be posix or not. Postgres tackled this way back in 2003 when
// they moved to build on envs more envs than cygwin like MinGW.
// https://github.com/postgres/postgres/commit/12c942383296bd626131241c012c2ab81b081738
//
// To further reassure that yes this is chaos and correct consider, as Meng Zhuo observed[1] while reviewing code to
// add the SIGUSR1 signal id to windows syscall types, minGW does not even have SIGUSR1. Furthermore, the original
// addition of Signal to windows Go was to only support SIGKILL ([2]) and quite the discussion about how Signal is a
// UNIX/POSIX construct being forced into Go and Windows indirectly ([3]). Even cygwin has 4 different variables of the
// signal. [4]
// [1]: https://go-review.googlesource.com/c/go/+/390695/1/src/syscall/types_windows.go#95
// [2]: https://codereview.appspot.com/4437091/diff/37002/src/pkg/os/exec_windows.go#newcode32
// [3]: https://codereview.appspot.com/4437091
// [4]: https://github.com/cygwin/cygwin/blob/c43ec5f5951c7f4b882a0f8e619601a45ae70a91/newlib/libc/include/sys/signal.h#L258-L371

package postgres

import "syscall"

// SIGUSR1 postgres win32_port.h uses 30
const SIGUSR1 = syscall.Signal(30)
