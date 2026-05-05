//! walg-daemon-client: minimal Rust client for the WAL-G daemon.
//!
//! Wire protocol stays byte-for-byte compatible with the Go reference in
//! `internal/daemon/client.go`

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

const VERSION:    &str = match option_env!("WALG_VERSION")    { Some(s) => s, None => "devel" };
const REVISION:   &str = match option_env!("WALG_REVISION")   { Some(s) => s, None => "devel" };
const BUILD_DATE: &str = match option_env!("WALG_BUILD_DATE") { Some(s) => s, None => "devel" };

const EXIT_IO_ERROR: u8 = 74;

#[derive(Copy, Clone)]
#[repr(u8)]
enum MsgType {
    Push  = b'F',
    Fetch = b'f',
    Ok    = b'O',
    Nx    = b'N',
}

struct Cmd {
    name:     &'static str,
    msg_type: MsgType,
    args:     &'static [&'static str],
}

const COMMANDS: &[Cmd] = &[
    Cmd { name: "wal-push",  msg_type: MsgType::Push,  args: &["wal_filepath"] },
    Cmd { name: "wal-fetch", msg_type: MsgType::Fetch, args: &["wal_name", "destination_filename"] },
];

fn find_command(name: &str) -> Option<&'static Cmd> {
    COMMANDS.iter().find(|c| c.name.eq_ignore_ascii_case(name))
}

/// Parse a Go-style duration ("60s", "5m", "1h30m", "200ms", "1.5s").
fn parse_duration(s: &str) -> Option<Duration> {
    if s.is_empty() {
        return None;
    }
    let mut total_ns: u128 = 0;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let mut int_part: u128 = 0;
        let mut saw_digit = false;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            int_part = int_part.checked_mul(10)?.checked_add((bytes[i] - b'0') as u128)?;
            i += 1;
            saw_digit = true;
        }
        let mut frac_num: u128 = 0;
        let mut frac_den: u128 = 1;
        if i < bytes.len() && bytes[i] == b'.' {
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                if frac_den < 1_000_000_000 {
                    frac_num = frac_num * 10 + (bytes[i] - b'0') as u128;
                    frac_den *= 10;
                }
                i += 1;
                saw_digit = true;
            }
        }
        if !saw_digit {
            return None;
        }

        let unit_ns: u128 = match bytes.get(i)? {
            b'n' if bytes.get(i + 1) == Some(&b's') => { i += 2; 1 }
            b'u' if bytes.get(i + 1) == Some(&b's') => { i += 2; 1_000 }
            // µs: U+00B5 in UTF-8 = 0xC2 0xB5
            0xC2 if bytes.get(i + 1) == Some(&0xB5) && bytes.get(i + 2) == Some(&b's') => {
                i += 3; 1_000
            }
            b'm' if bytes.get(i + 1) == Some(&b's') => { i += 2; 1_000_000 }
            b's' => { i += 1; 1_000_000_000 }
            b'm' => { i += 1; 60 * 1_000_000_000 }
            b'h' => { i += 1; 3_600 * 1_000_000_000 }
            _ => return None,
        };
        total_ns = total_ns
            .checked_add(int_part.checked_mul(unit_ns)?)?
            .checked_add(frac_num * unit_ns / frac_den)?;
    }
    u64::try_from(total_ns).ok().map(Duration::from_nanos)
}

/// Build the request frame.
///
/// 0 args : header only,             total = 3
/// 1 arg  : raw arg bytes,            total = 3 + len(arg)
/// N args : `[count:1][u16_len][bytes]`...,  total = 3 + 1 + sum(2 + len_i)
fn build_frame(t: MsgType, args: &[&str]) -> Result<Vec<u8>, &'static str> {
    let body_len: usize = match args.len() {
        0 => 0,
        1 => args[0].len(),
        n => {
            if n > 255 {
                return Err("too many args");
            }
            1 + args.iter().map(|a| 2 + a.len()).sum::<usize>()
        }
    };
    let total = 3 + body_len;
    if total > 0xFFFF {
        return Err("frame too large");
    }

    let mut buf = Vec::with_capacity(total);
    buf.push(t as u8);
    buf.push((total >> 8) as u8);
    buf.push(total as u8);

    match args.len() {
        0 => {}
        1 => buf.extend_from_slice(args[0].as_bytes()),
        _ => {
            buf.push(args.len() as u8);
            for a in args {
                let l = u16::try_from(a.len()).map_err(|_| "arg too long")?;
                buf.extend_from_slice(&l.to_be_bytes());
                buf.extend_from_slice(a.as_bytes());
            }
        }
    }
    Ok(buf)
}

/// Connect, send, read first response byte.
///
/// std::os::unix::net::UnixStream has no connect_timeout; we rely on the
/// kernel's fast-fail on AF_UNIX (immediate ECONNREFUSED if the daemon is not
/// listening) and only bound the I/O phase via SO_RCVTIMEO / SO_SNDTIMEO.
fn send_request(
    socket_path: &Path,
    frame: &[u8],
    op_timeout: Duration,
) -> std::io::Result<u8> {
    let mut stream = UnixStream::connect(socket_path)?;
    stream.set_read_timeout(Some(op_timeout))?;
    stream.set_write_timeout(Some(op_timeout))?;
    stream.write_all(frame)?;
    let mut buf = [0u8; 512];
    let n = stream.read(&mut buf)?;
    if n == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "empty daemon response",
        ));
    }
    Ok(buf[0])
}

const USAGE_FLAGS: &str = "\nFlags:\n  \
    -timeout duration\n        \
        daemon operation execution timeout (default 1m0s)\n";

fn print_usage(err: &str, cmd_usage: Option<&str>) {
    println!("Error: {err}");
    println!();
    println!("walg-daemon-client is lightweight client for WAL-G daemon");
    println!();
    println!("Usage:");
    match cmd_usage {
        Some(u) => println!("  walg-daemon-client socket {u} [flags]"),
        None => {
            println!("  walg-daemon-client socket command [command_args] [flags]");
            println!();
            println!("Arguments:");
            println!("  socket\t- name of unix socket to communicate with wal-g daemon");
            println!("  command\t- command to send to the daemon: wal-push, wal-fetch");
            println!("  command_args\t- command specific arguments");
        }
    }
    print!("{USAGE_FLAGS}");
}

fn split_flag(s: &str) -> (&str, Option<&str>) {
    match s.find('=') {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None    => (s, None),
    }
}

fn run(argv: Vec<String>) -> ExitCode {
    if argv.len() >= 2 && argv[1].eq_ignore_ascii_case("--version") {
        println!("walg-daemon-client\t{VERSION}\t{REVISION}\t{BUILD_DATE}");
        return ExitCode::SUCCESS;
    }
    if argv.len() < 3 {
        print_usage("not enough arguments", None);
        return ExitCode::from(1);
    }

    let socket_path = Path::new(&argv[1]);
    let cmd = match find_command(&argv[2]) {
        Some(c) => c,
        None => {
            print_usage(&format!("unsupported command {}", argv[2]), None);
            return ExitCode::from(1);
        }
    };
    if argv.len() < 3 + cmd.args.len() {
        let usage = format!("{} {}", cmd.name, cmd.args.join(" "));
        print_usage("not enough command arguments", Some(&usage));
        return ExitCode::from(1);
    }

    let msg_args: Vec<&str> = argv[3..3 + cmd.args.len()]
        .iter()
        .map(String::as_str)
        .collect();

    let mut op_timeout = Duration::from_secs(60);

    let mut i = 3 + cmd.args.len();
    while i < argv.len() {
        let raw = &argv[i];
        let stripped = raw
            .strip_prefix("--")
            .or_else(|| raw.strip_prefix('-'));
        let stripped = match stripped {
            Some(s) => s,
            None => {
                eprintln!("unexpected positional arg: {raw}");
                return ExitCode::from(1);
            }
        };
        let (name, inline) = split_flag(stripped);
        let val: String = match inline {
            Some(v) => v.to_string(),
            None => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("flag needs an argument: -{name}");
                    return ExitCode::from(1);
                }
                argv[i].clone()
            }
        };
        if name != "timeout" {
            eprintln!("unknown flag: -{name}");
            return ExitCode::from(1);
        }
        match parse_duration(&val) {
            Some(d) => op_timeout = d,
            None => {
                eprintln!("invalid value {val} for flag -{name}");
                return ExitCode::from(1);
            }
        }
        i += 1;
    }

    if !socket_path.exists() {
        eprintln!(
            "daemon socket '{}' doesn't exist or is unavailable",
            socket_path.display()
        );
        return ExitCode::from(1);
    }

    let frame = match build_frame(cmd.msg_type, &msg_args) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("frame encoding error: {e}");
            return ExitCode::from(1);
        }
    };

    match send_request(socket_path, &frame, op_timeout) {
        Ok(b) if b == MsgType::Ok as u8 => ExitCode::SUCCESS,
        Ok(b) if b == MsgType::Nx as u8 => {
            println!(
                "daemon command run error [message type: {}, daemon response: {}]",
                cmd.msg_type as u8 as char,
                b as char
            );
            ExitCode::from(EXIT_IO_ERROR)
        }
        Ok(b) => {
            eprintln!(
                "daemon command run error [message type: {}, daemon response: {}]",
                cmd.msg_type as u8 as char,
                b as char
            );
            ExitCode::from(1)
        }
        Err(e) => {
            eprintln!("unix socket error: {e}");
            ExitCode::from(1)
        }
    }
}

fn main() -> ExitCode {
    run(std::env::args().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_basic() {
        assert_eq!(parse_duration("60s"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1h30m"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_duration("200ms"), Some(Duration::from_millis(200)));
        assert_eq!(parse_duration("1.5s"), Some(Duration::from_millis(1500)));
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("60"), None);
        assert_eq!(parse_duration("60x"), None);
    }

    #[test]
    fn frame_one_arg_is_raw_bytes() {
        let f = build_frame(MsgType::Push, &["00000001"]).unwrap();
        assert_eq!(f[0], b'F');
        assert_eq!(u16::from_be_bytes([f[1], f[2]]) as usize, 3 + 8);
        assert_eq!(&f[3..], b"00000001");
    }

    #[test]
    fn frame_two_args_uses_count_and_lenprefix() {
        let f = build_frame(MsgType::Fetch, &["a", "bb"]).unwrap();
        assert_eq!(f[0], b'f');
        assert_eq!(u16::from_be_bytes([f[1], f[2]]) as usize, 3 + 1 + 2 + 1 + 2 + 2);
        assert_eq!(f[3], 2); // count
        assert_eq!(u16::from_be_bytes([f[4], f[5]]), 1);
        assert_eq!(f[6], b'a');
        assert_eq!(u16::from_be_bytes([f[7], f[8]]), 2);
        assert_eq!(&f[9..], b"bb");
    }
}
