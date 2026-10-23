// SPDX-License-Identifier: Elastic-2.0
use super::*;
use std::collections::HashMap;

struct R(HashMap<&'static str, Datum>);

impl Row for R {
    fn get(&self, name: &str) -> Option<&Datum> {
        self.0.get(name)
    }
}

fn row(vals: &[(&'static str, Datum)]) -> R {
    R(vals.iter().cloned().collect())
}

fn num(x: f64) -> Datum {
    Datum::Num(x)
}

/// `src` over `r`, as a reply writes the result.
fn run(src: &str, r: &R) -> Result<String, String> {
    let d = parse(src)?.eval(r)?;
    Ok(String::from_utf8_lossy(&d.text()).into_owned())
}

fn run0(src: &str) -> String {
    run(src, &row(&[])).expect(src)
}

/// RediSearch writes a whole number as an integer and anything else as
/// C's `%.12g`; every one of these goes through C's own `snprintf` too.
#[test]
fn a_number_is_written_as_redisearch_writes_it() {
    for (x, want) in [
        (3.0, "3"),
        (-0.0, "0"),
        (0.1 + 0.2, "0.3"),
        (1.0 / 3.0, "0.333333333333"),
        (1e20, "1e+20"),
        (1e-7, "1e-07"),
        (0.287_682_072_451_780_9, "0.287682072452"),
        (123_456_789_012_345.0, "123456789012345"),
        (1.5, "1.5"),
        (-2.25e-5, "-2.25e-05"),
        (9.999_999_999_999_9e11, "1e+12"),
        (f64::INFINITY, "inf"),
        (f64::NEG_INFINITY, "-inf"),
        (f64::NAN, "nan"),
    ] {
        assert_eq!(num_to_str(x), want, "{x:e}");
    }
    let mut buf = [0 as libc::c_char; 64];
    for x in [
        0.1 + 0.2,
        1.0 / 3.0,
        1e20,
        1e-7,
        2.5e-5,
        123.456_789_012_345_6,
        99_999_999_999.95,
        6.02e23,
        -1234.5678e-9,
        0.000_123_456_789_012_345,
        547_758.3 % 5.1,
    ] {
        // SAFETY: the buffer is 64 bytes and `%.12g` of one double is at
        // most 24; the format takes exactly the one double passed.
        let n = unsafe { libc::snprintf(buf.as_mut_ptr(), buf.len(), c"%.12g".as_ptr(), x) };
        let c: Vec<u8> = buf[..n as usize].iter().map(|b| *b as u8).collect();
        assert_eq!(fmt_g(x, 12).as_bytes(), c.as_slice(), "{x:e}");
    }
}

/// The grammar: a sign belongs to its number, so `@a-1` is two values in a
/// row; comparisons do not chain; `!` takes one comparison; `^` binds to
/// its right.
#[test]
fn the_grammar_is_redisearch_s() {
    let r = row(&[("a", num(5.0)), ("b", num(2.0))]);
    assert!(
        parse("@a-1")
            .expect_err("refused")
            .starts_with("Syntax error at offset 2 near '-1'")
    );
    assert_eq!(run("@a - 1", &r).as_deref(), Ok("4"));
    assert!(
        parse("1 < 2 < 3")
            .expect_err("refused")
            .starts_with("Syntax error")
    );
    assert!(
        parse("-@a")
            .expect_err("refused")
            .starts_with("Syntax error at offset 0")
    );
    assert!(
        parse("(1 + 2")
            .expect_err("refused")
            .starts_with("Syntax error")
    );
    assert_eq!(parse("foo").expect_err("refused"), "Unknown symbol 'foo'");
    assert_eq!(
        parse("foo(1)").expect_err("refused"),
        "Unknown function name 'foo'"
    );
    assert_eq!(
        parse("log(1, 2)").expect_err("refused"),
        "Function 'log' expects 1 arguments, but got 2"
    );
    assert_eq!(
        parse("split()").expect_err("refused"),
        "Function 'split' expects between 1 and 3 arguments, but got 0"
    );
    assert_eq!(run("!@a == 5", &r).as_deref(), Ok("0"));
    assert_eq!(run("!@a == 4", &r).as_deref(), Ok("1"));
    assert_eq!(run("!0 && 0", &r).as_deref(), Ok("0"));
    assert_eq!(run("2 ^ 3 ^ 2", &r).as_deref(), Ok("512"));
    assert_eq!(run("1 + 2 * 3 - 4 / @b", &r).as_deref(), Ok("5"));
    assert_eq!(run("1 || 0 && 0", &r).as_deref(), Ok("1"));
    assert_eq!(run("LOG(@a) > 1", &r).as_deref(), Ok("1"));
    assert_eq!(run("$1 + 1", &r).as_deref(), Ok("2"), "`$` passes unread");
    assert_eq!(run0("NULL == NULL"), "1");
    assert_eq!(run0("-inf < 0"), "1");
}

/// Values as RediSearch's tests and its rules give them.
#[test]
fn arithmetic_and_comparisons_follow_redisearch() {
    assert_eq!(run0("547758.3 % 5.1"), "3.00000000008");
    assert_eq!(run0("-54775808 % -5"), "-3");
    assert_eq!(run0("1 / 0"), "inf");
    assert_eq!(run0("0 / 0"), "nan");
    assert_eq!(run0("exp(1000)"), "inf");
    assert_eq!(run0("'3' + '4'"), "7");
    assert_eq!(run0("'3' == 3"), "1");
    assert_eq!(run0("'abc' == 3"), "0");
    assert_eq!(run0("'abc' != 3"), "1");
    assert_eq!(run0("'b' > 'a'"), "1");
    assert_eq!(run0("'B' < 'a'"), "1", "bytes, case and all");
    assert_eq!(
        run(" 'abc' < 3", &row(&[])),
        Err("Error converting string".into())
    );
    assert_eq!(
        run("'abc' + 1", &row(&[])),
        Err("Could not convert value to a number".into())
    );
    assert_eq!(run0("'x\\'y'"), "x'y");
    assert_eq!(run0("\"a\\qb\""), "a\\qb");
    let missing = run("@gone + 1", &row(&[]));
    assert_eq!(
        missing,
        Err("Could not find the value for a parameter name, consider using EXISTS if applicable for gone".into())
    );
    assert_eq!(run0("exists(@gone)"), "0");
    assert_eq!(
        run("exists(@a)", &row(&[("a", num(0.0))])).as_deref(),
        Ok("1")
    );
    assert_eq!(
        run0("case(0, @gone, 'no')"),
        "no",
        "the branch not taken is not read"
    );
}

/// The shapes redisvl sends: its router's FILTER and its hybrid query's
/// APPLY.
#[test]
fn redisvl_s_expressions_evaluate() {
    let f = parse(
        "(@route_name == 'greeting' && @distance < 0.3) || (@route_name == 'farewell' && @distance < 0.5)",
    )
    .expect("parses");
    let keep = |name: &str, d: f64| {
        f.eval(&row(&[
            ("route_name", Datum::str(name)),
            ("distance", num(d)),
        ]))
        .expect("evaluates")
        .truthy()
    };
    assert!(keep("greeting", 0.2));
    assert!(!keep("greeting", 0.4));
    assert!(keep("farewell", 0.4));
    assert!(!keep("other", 0.1));
    let r = row(&[("vector_distance", num(0.5)), ("__score", num(1.25))]);
    assert_eq!(run("(2 - @vector_distance)/2", &r).as_deref(), Ok("0.75"));
    let h = parse("0.30000000000000004*@text_score + 0.7*@vector_similarity").expect("parses");
    assert_eq!(h.props(), ["text_score", "vector_similarity"]);
    let r = row(&[("text_score", num(1.25)), ("vector_similarity", num(0.75))]);
    assert_eq!(
        String::from_utf8_lossy(&h.eval(&r).expect("evaluates").text()),
        "0.9"
    );
}

#[test]
fn string_functions_follow_redisearch() {
    assert_eq!(run0("substr('hello world', 6, 5)"), "world");
    assert_eq!(run0("substr('hello', -3, 2)"), "ll");
    assert_eq!(run0("substr('hello', 1, -1)"), "ell");
    assert_eq!(run0("substr('hello', 10, 2)"), "");
    assert_eq!(
        run("substr(3, 0, 1)", &row(&[])),
        Err("Invalid type for substr. Expected string".into())
    );
    assert_eq!(run0("lower('MiXeD')"), "mixed");
    assert_eq!(run0("upper('MiXeD')"), "MIXED");
    assert_eq!(run0("lower(3)"), "", "null for a number");
    assert_eq!(run0("format('%s-%s %%', 'a', 3.5)"), "a-3.5 %");
    assert_eq!(run0("format('%s', NULL)"), "(null)");
    assert_eq!(
        run("format('%s %s', 'a')", &row(&[])),
        Err("Not enough arguments for format".into())
    );
    assert_eq!(
        run("format('%d', 1)", &row(&[])),
        Err("Unknown format specifier passed".into())
    );
    let s = parse("split(' a, b,,c ,')")
        .expect("parses")
        .eval(&row(&[]))
        .expect("splits");
    assert_eq!(
        s,
        Datum::Arr(vec![Datum::str("a"), Datum::str("b"), Datum::str("c")])
    );
    let s = parse("split('x|y z', '| ', 'z')")
        .expect("parses")
        .eval(&row(&[]))
        .expect("splits");
    assert_eq!(s, Datum::Arr(vec![Datum::str("x"), Datum::str("y")]));
    assert_eq!(run0("contains('aaaa', 'aa')"), "3");
    assert_eq!(run0("contains('abc', '')"), "4");
    assert_eq!(run0("startswith('abc', 'ab')"), "1");
    assert_eq!(run0("strlen('héllo')"), "6", "bytes");
    assert_eq!(run0("to_number('1e3') + 1"), "1001");
    assert_eq!(
        run("to_number('x')", &row(&[])),
        Err("to_number: cannot convert string 'x'".into())
    );
    assert_eq!(run0("to_str(2.5)"), "2.5");
    assert_eq!(run0("abs(-2) + floor(2.7) + ceil(2.1) + sqrt(16)"), "11");
    assert_eq!(run0("log2(8)"), "3");
    assert_eq!(run0("log('x')"), "nan");
}

/// Dates in UTC, against Python's `time.strftime` and `calendar.timegm`
/// for 1700000000 (2023-11-14T22:13:20Z, a Tuesday).
#[test]
fn date_functions_follow_redisearch() {
    assert_eq!(run0("timefmt(1700000000)"), "2023-11-14T22:13:20Z");
    assert_eq!(
        run0(
            "timefmt(1700000000, '%a %A %b %B %c|%C|%D|%e|%F|%g|%G|%I|%j|%p|%r|%R|%T|%u|%U|%V|%w|%W|%x|%X|%y|%Y')"
        ),
        "Tue Tuesday Nov November Tue Nov 14 22:13:20 2023|20|11/14/23|14|2023-11-14|23|2023|10|318|PM|10:13:20 PM|22:13|22:13:20|2|46|46|2|46|11/14/23|22:13:20|23|2023"
    );
    // 2021-01-03, a Sunday, in ISO week 53 of 2020.
    assert_eq!(
        run0("timefmt(1609632000, '%U %W %V %G %j')"),
        "01 00 53 2020 003"
    );
    assert_eq!(run0("timefmt(0, '%z %Z %%')"), "+0000 GMT %");
    assert!(
        run("timefmt(0, '%k')", &row(&[])).is_err(),
        "not POSIX: refused"
    );
    assert_eq!(run0("timefmt('x')"), "");
    assert_eq!(run0("hour(1700000000)"), "1699999200");
    assert_eq!(run0("minute(1700000000)"), "1699999980");
    assert_eq!(run0("day(1700000000)"), "1699920000");
    assert_eq!(run0("month(1700000000)"), "1698796800");
    assert_eq!(run0("year(1700000000)"), "2023");
    assert_eq!(run0("monthofyear(1700000000)"), "10");
    assert_eq!(run0("dayofmonth(1700000000)"), "14");
    assert_eq!(run0("dayofweek(1700000000)"), "2");
    assert_eq!(run0("dayofyear(1700000000)"), "317");
    assert_eq!(run0("year(-1)"), "", "null for a negative time");
    assert_eq!(
        run0("parsetime('2023-11-14T22:13:20Z', '%Y-%m-%dT%H:%M:%SZ')"),
        "1700000000"
    );
    assert_eq!(
        run0("parsetime('Feb 29 2024 12:30:05', '%b %d %Y %T')"),
        "1709209805"
    );
    assert_eq!(run0("parsetime('07:05 pm', '%I:%M %p')"), "-2209006500");
    // A day not given is 0: the last day of the month before.
    assert_eq!(run0("parsetime('2024', '%Y')"), "1703980800");
    assert_eq!(run0("parsetime('nope', '%Y')"), "");
}

#[test]
fn geodistance_is_metres_to_two_decimals() {
    let d = run0("geodistance('-87.6298,41.8781', '-122.4194,37.7749')");
    let want =
        (crate::geo::distance(-87.6298, 41.8781, -122.4194, 37.7749) * 100.0).round() / 100.0;
    assert_eq!(d, num_to_str(want));
    assert_eq!(
        run0("geodistance('-87.6298,41.8781', -87.6298, 41.8781)"),
        "0"
    );
    assert_eq!(run0("geodistance(1, 2, 1, 2)"), "0");
    assert_eq!(run0("geodistance('x', 'y')"), "nan");
}
