// SPDX-License-Identifier: Elastic-2.0
//! A daemon's log lines, written to stderr or stdout without panicking when
//! the write fails (BUG-0276).
//!
//! `eprintln!` and `println!` panic when their write fails. A seat's stderr
//! and stdout are a file on its own data volume (`{statedir}/logs/<seat>.log`,
//! opened by flintctl), so on a full volume the first line that needs a new
//! block panicked whichever thread wrote it, and a background thread that
//! panics stops for good: the disk guard died on its `Ok -> Shed` line before
//! it applied it, and the seat never shed or reclaimed again.
//!
//! `elog!` and `olog!` take `eprintln!`'s and `println!`'s arguments and drop
//! a failed write instead. A lost line is the lesser harm: what it said is in
//! the seat's counters, `FLINTINFO` and the journal. Every daemon crate denies
//! `clippy::print_stderr` and `clippy::print_stdout`, so the panicking forms
//! cannot come back.
//!
//! In a crate's own unit tests the lines go through the standard macros, so
//! the test harness captures them as before; `cfg!(test)` expands in the
//! calling crate, which is what makes that the caller's choice.

/// `eprintln!`, dropping a failed write rather than panicking.
#[macro_export]
macro_rules! elog {
    () => {
        $crate::elog!("")
    };
    ($($arg:tt)*) => {
        if cfg!(test) {
            #[allow(clippy::print_stderr)]
            let () = ::std::eprintln!($($arg)*);
        } else {
            $crate::stderr_line(::std::format_args!($($arg)*));
        }
    };
}

/// `println!`, dropping a failed write rather than panicking.
#[macro_export]
macro_rules! olog {
    () => {
        $crate::olog!("")
    };
    ($($arg:tt)*) => {
        if cfg!(test) {
            #[allow(clippy::print_stdout)]
            let () = ::std::println!($($arg)*);
        } else {
            $crate::stdout_line(::std::format_args!($($arg)*));
        }
    };
}

/// One line to stderr; a failed write is dropped. What `elog!` calls.
#[doc(hidden)]
pub fn stderr_line(line: std::fmt::Arguments<'_>) {
    use std::io::Write as _;
    let _ = writeln!(std::io::stderr(), "{line}");
}

/// One line to stdout; a failed write is dropped. What `olog!` calls.
#[doc(hidden)]
pub fn stdout_line(line: std::fmt::Arguments<'_>) {
    use std::io::Write as _;
    let _ = writeln!(std::io::stdout(), "{line}");
}

#[cfg(test)]
mod tests {
    /// A write to a pipe whose reader is gone fails (`EPIPE`), as a write to
    /// a full volume does (`ENOSPC`). `eprintln!` panics on either; the line
    /// writer a daemon's `elog!` calls must not.
    #[cfg(unix)]
    #[test]
    fn a_failed_write_is_dropped_not_a_panic() {
        use std::io::Write as _;
        // SAFETY: plain POSIX calls on descriptors this test owns. Stderr is
        // saved first and put back before any assertion can print.
        let (saved, write_end) = unsafe {
            let mut fds = [0i32; 2];
            assert_eq!(libc::pipe(fds.as_mut_ptr()), 0, "pipe");
            let saved = libc::dup(2);
            assert!(saved >= 0, "dup stderr");
            libc::close(fds[0]); // no reader: every write now fails
            assert_eq!(libc::dup2(fds[1], 2), 2, "dup2 onto stderr");
            (saved, fds[1])
        };
        // The control: the stream really fails, so the writer below is
        // handed a failed write and not a no-op.
        let control = std::io::stderr().write_all(b"x\n").is_err();
        let r = std::panic::catch_unwind(|| {
            crate::stderr_line(format_args!("this line has nowhere to go: {}", 1));
            crate::stderr_line(format_args!(""));
        });
        unsafe {
            libc::dup2(saved, 2);
            libc::close(saved);
            libc::close(write_end);
        }
        assert!(
            control,
            "the control: a write to the broken pipe did not fail"
        );
        assert!(r.is_ok(), "the line writer panicked on a failed write");
    }

    /// Both macros take what `eprintln!` and `println!` take.
    #[test]
    fn the_macros_take_the_standard_arguments() {
        let n = 3;
        crate::elog!();
        crate::elog!("plain");
        crate::elog!("{n} and {}", n + 1);
        crate::olog!("{n:>4}|{:?}", "x");
    }
}
