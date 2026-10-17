#!/bin/bash
# Lightweight drop-in replacement for the walg-daemon-client binary (~4MB).
# Implements the same unix-socket protocol:
#   message = <1 byte type> <2 bytes big-endian total length> <body>
#   body for 1-arg commands  = raw string (see daemon.getMessage)
#   body for 2-arg commands  = <1 byte arg count> <2 bytes arg len> <arg> ...
# Responses: 'O' - ok, 'N' - archive does not exist (exit 74), 'E' - error (exit 1).
#
# Requires: nc with unix socket support (openbsd/netcat: -U)
#
# Usage:
#   walg-daemon-client.sh socket command [command_args] [-timeout 60s] [-connection-timeout 5s]
# Commands:
#   wal-push wal_filepath
#   wal-fetch wal_name destination_filename
#   wal-prefetch count

set -u

EX_IOERR=74

TIMEOUT=60          # daemon operation timeout, seconds
CONN_TIMEOUT=5      # socket connect timeout, seconds

usage() {
	cat >&2 <<'EOF'
Usage:
  walg-daemon-client.sh socket command [command_args] [-timeout duration] [-connection-timeout duration]

Arguments:
  socket            - name of unix socket to communicate with wal-g daemon
  command           - command to send to the daemon: wal-push, wal-fetch, wal-prefetch
  command_args      - command specific arguments

Commands:
  wal-push wal_filepath
  wal-fetch wal_name destination_filename
  wal-prefetch count
EOF
	exit 1
}

# convert "60s", "5m", "2m30s" style duration to integer seconds (like the Go client)
parse_duration() {
	local d=$1 total=0 n
	while [[ -n $d ]]; do
		n=${d%%[smh]*}
		[[ $n =~ ^[0-9]+$ ]] || { echo "invalid duration: $1" >&2; exit 1; }
		case $d in
			*s*) ((total += n)); d=${d#*s} ;;
			*m*) ((total += n * 60)); d=${d#*m} ;;
			*h*) ((total += n * 3600)); d=${d#*h} ;;
			*)  echo "invalid duration: $1" >&2; exit 1 ;;
		esac
	done
	echo "$total"
}

# append a big-endian uint16 as "\xHH\xHH" escape sequence to $ESC
esc_u16() {
	local v=$1
	ESC+="\\x$(printf '%02x' $(( (v >> 8) & 0xff )))\\x$(printf '%02x' $(( v & 0xff )))"
}

[[ $# -lt 2 ]] && usage

SOCKET=$1
COMMAND=$(echo "$2" | tr '[:upper:]' '[:lower:]')
shift 2

pos=()
while [[ $# -gt 0 ]]; do
	case $1 in
		-timeout|--timeout)
			[[ $# -lt 2 ]] && usage
			TIMEOUT=$(parse_duration "$2"); shift 2 ;;
		-timeout=*|--timeout=*)
			TIMEOUT=$(parse_duration "${1#*=}"); shift ;;
		-connection-timeout|--connection-timeout)
			[[ $# -lt 2 ]] && usage
			CONN_TIMEOUT=$(parse_duration "$2"); shift 2 ;;
		-connection-timeout=*|--connection-timeout=*)
			CONN_TIMEOUT=$(parse_duration "${1#*=}"); shift ;;
		--) shift; pos+=("$@"); break ;;
		-*) echo "unknown flag: $1" >&2; usage ;;
		*) pos+=("$1"); shift ;;
	esac
done

case $COMMAND in
	wal-push)
		TYPE='F'
		[[ ${#pos[@]} -eq 1 ]] || usage ;;
	wal-fetch)
		TYPE='f'
		[[ ${#pos[@]} -eq 2 ]] || usage ;;
	wal-prefetch)
		TYPE='P'
		[[ ${#pos[@]} -eq 1 ]] || { echo "wal-prefetch count" >&2; usage; }
		[[ ${pos[0]} =~ ^[0-9]+$ ]] || { echo "wal-prefetch: invalid count '${pos[0]}'" >&2; exit 1; }
		(( pos[0] > 0 )) || { echo "wal-prefetch: count must be positive" >&2; exit 1; } ;;
	*)
		echo "unsupported command $COMMAND" >&2; usage ;;
esac

[[ -e $SOCKET ]] || { echo "daemon socket '$SOCKET' doesn't exist or is unavailable" >&2; exit 1; }

command -v nc >/dev/null 2>&1 || { echo "nc is required but not installed" >&2; exit 1; }

# build the message as an escape sequence string, then emit raw bytes via printf '%b'
ESC="\\x$(printf '%02x' "'$TYPE")"
if [[ ${#pos[@]} -eq 1 ]]; then
	# single-argument body is the raw string itself
	esc_u16 $(( 3 + ${#pos[0]} ))
	ESC+="${pos[0]}"
else
	# multi-argument body: arg count byte, then per arg: u16 length + bytes
	body_len=$(( 1 + 2 + ${#pos[0]} + 2 + ${#pos[1]} ))
	esc_u16 $(( 3 + body_len ))
	ESC+="\\x02"
	for arg in "${pos[@]}"; do
		esc_u16 "${#arg}"
		ESC+="$arg"
	done
fi

REPLY=$(printf '%b' "$ESC" | nc -U -w "$TIMEOUT" "$SOCKET" && printf X) ; status=$?
REPLY=${REPLY%X}
[[ $status -ne 0 ]] && { echo "failed to communicate with daemon via $SOCKET" >&2; exit 1; }
[[ -z $REPLY ]] && { echo "daemon response error [message type: $TYPE, args: ${pos[*]:-}]" >&2; exit 1; }

case $REPLY in
	O*) exit 0 ;;
	N*|n*)
		echo "daemon command run error [message type: $TYPE, args: ${pos[*]:-}, daemon response: archive does not exist]" >&2
		exit $EX_IOERR ;;
	*)
		echo "daemon command run error [message type: $TYPE, args: ${pos[*]:-}, daemon response: $REPLY]" >&2
		exit 1 ;;
esac
