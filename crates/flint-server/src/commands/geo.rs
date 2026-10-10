// SPDX-License-Identifier: Elastic-2.0
//! Geo: GEOADD, GEOPOS, GEODIST, GEOHASH, GEORADIUS, GEORADIUSBYMEMBER,
//! their `_RO` forms, GEOSEARCH and GEOSEARCHSTORE. A geo set is a sorted
//! set whose scores are 52-bit geohashes, so every sorted-set command reads
//! and writes one; these are upstream's `geo.c` over it, in its order of
//! checks, with Valkey 9.1's replies where Redis 8.2's differ.
//!
//! WHY THE ARITHMETIC IS COPIED RATHER THAN RE-DERIVED. What a search
//! answers rests on exact doubles: the cell a point is stored in, the nine
//! cells a search reads and their order (which is the order of a reply
//! asked for no sort), whether a point on a radius's edge is in, and a
//! coordinate's last digits. So each step is upstream's (`geohash.c`,
//! `geohash_helper.c`), in its order of operations.
//!
//! Three of those steps are a multiply and an add that the C compiler fuses
//! into one rounding on arm64: a cell's edges (`geohashDecode`), the
//! haversine's `u*u + ...` (`geohashGetDistance`) and BYBOX's half
//! diagonal. Valkey 9.1 and Redis 8.2 built for arm64 both do (`fmadd` in
//! their disassembly), and they are what this is checked against; `mul_add`
//! does the same on every platform. An x86-64 build of either, which rounds
//! twice, can differ from both in a coordinate's last digit.

use super::*;

/// One coordinate's range (`GeoHashRange`).
#[derive(Clone, Copy, Debug)]
struct Range {
    min: f64,
    max: f64,
}

/// The ranges a score encodes (`geohashGetCoordRange`): latitudes stop
/// where Web Mercator does.
const LON: Range = Range {
    min: -180.0,
    max: 180.0,
};
const LAT: Range = Range {
    min: -85.051_128_78,
    max: 85.051_128_78,
};

/// The bits per coordinate in a score: 26 each, 52 in all.
const STEP_MAX: u8 = 26;

const EARTH_RADIUS_IN_METERS: f64 = 6_372_797.560_856;
const MERCATOR_MAX: f64 = 20_037_726.37;
const D_R: f64 = std::f64::consts::PI / 180.0;

/// `0123456789bcdefghjkmnpqrstuvwxyz`, GEOHASH's alphabet.
const GEOALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// A cell: `step` bits of each coordinate, interleaved (`GeoHashBits`).
#[derive(Clone, Copy, Debug, PartialEq)]
struct Cell {
    bits: u64,
    step: u8,
}

/// The cell `GZERO` leaves, which a search skips.
const NO_CELL: Cell = Cell { bits: 0, step: 0 };

/// A cell's edges (`GeoHashArea`).
#[derive(Clone, Copy, Debug)]
struct Edges {
    lon: Range,
    lat: Range,
}

/// `interleave64`: `lat` in the even bits, `lon` in the odd.
fn interleave(lat: u32, lon: u32) -> u64 {
    fn spread(v: u32) -> u64 {
        let mut x = u64::from(v);
        x = (x | (x << 16)) & 0x0000_FFFF_0000_FFFF;
        x = (x | (x << 8)) & 0x00FF_00FF_00FF_00FF;
        x = (x | (x << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
        x = (x | (x << 2)) & 0x3333_3333_3333_3333;
        (x | (x << 1)) & 0x5555_5555_5555_5555
    }
    spread(lat) | (spread(lon) << 1)
}

/// `deinterleave64`: the even bits and the odd, as (lat, lon).
fn deinterleave(bits: u64) -> (u32, u32) {
    fn squash(v: u64) -> u32 {
        let mut x = v & 0x5555_5555_5555_5555;
        x = (x | (x >> 1)) & 0x3333_3333_3333_3333;
        x = (x | (x >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
        x = (x | (x >> 4)) & 0x00FF_00FF_00FF_00FF;
        x = (x | (x >> 8)) & 0x0000_FFFF_0000_FFFF;
        x = (x | (x >> 16)) & 0x0000_0000_FFFF_FFFF;
        x as u32
    }
    (squash(bits), squash(bits >> 1))
}

/// `geohashEncode` of a point inside the ranges: the cell of `step` bits
/// per coordinate holding it. Every caller has checked the point, or
/// decoded it from a cell.
fn encode(lon_r: Range, lat_r: Range, lon: f64, lat: f64, step: u8) -> Cell {
    let scale = (1u64 << step) as f64;
    let lat_offset = (lat - lat_r.min) / (lat_r.max - lat_r.min) * scale;
    let lon_offset = (lon - lon_r.min) / (lon_r.max - lon_r.min) * scale;
    // C converts each offset to `uint32_t`, dropping the fraction.
    Cell {
        bits: interleave(lat_offset as u32, lon_offset as u32),
        step,
    }
}

/// `geohashDecode` over the score ranges: a cell's edges, each a fused
/// multiply and add. `ilato + 1` is a 32-bit sum in C, so it wraps.
fn decode(cell: Cell) -> Edges {
    let (lat_i, lon_i) = deinterleave(cell.bits);
    let div = (1u64 << cell.step) as f64;
    let edge = |i: u32, r: Range| (f64::from(i) / div).mul_add(r.max - r.min, r.min);
    Edges {
        lat: Range {
            min: edge(lat_i, LAT),
            max: edge(lat_i.wrapping_add(1), LAT),
        },
        lon: Range {
            min: edge(lon_i, LON),
            max: edge(lon_i.wrapping_add(1), LON),
        },
    }
}

/// `geohashDecodeAreaToLongLat`: a cell's middle, kept inside the ranges.
fn middle(e: &Edges) -> [f64; 2] {
    [
        ((e.lon.min + e.lon.max) / 2.0).clamp(LON.min, LON.max),
        ((e.lat.min + e.lat.max) / 2.0).clamp(LAT.min, LAT.max),
    ]
}

/// `geohashAlign52Bits`: a cell as the score of its first point.
fn align52(cell: Cell) -> u64 {
    cell.bits << (52 - u32::from(cell.step) * 2)
}

/// GEOADD's score for a point inside the ranges.
fn score_of(xy: [f64; 2]) -> u64 {
    align52(encode(LON, LAT, xy[0], xy[1], STEP_MAX))
}

/// `decodeGeohash`: the point a score names. C reads the score as
/// `(uint64_t)score`, which on arm64 saturates as `as` does, so a negative
/// score is cell 0 and anything past 2^64 the last.
fn point_of(score: f64) -> [f64; 2] {
    middle(&decode(Cell {
        bits: score as u64,
        step: STEP_MAX,
    }))
}

fn deg_rad(ang: f64) -> f64 {
    ang * D_R
}

fn rad_deg(ang: f64) -> f64 {
    ang / D_R
}

/// `geohashGetLatDistance`.
fn lat_distance(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2) - deg_rad(lat1)).abs()
}

/// `geohashGetDistance`: the haversine on a sphere, in meters, with its
/// `u*u + ...` fused.
fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lon1r = deg_rad(lon1);
    let lon2r = deg_rad(lon2);
    let v = ((lon2r - lon1r) / 2.0).sin();
    // Upstream's shortcut when the longitudes are practically the same.
    if v == 0.0 {
        return lat_distance(lat1, lat2);
    }
    let lat1r = deg_rad(lat1);
    let lat2r = deg_rad(lat2);
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u.mul_add(u, lat1r.cos() * lat2r.cos() * v * v);
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

/// `addReplyDoubleDistance`: four decimals, from the distance times 10^4
/// rounded half to even, as `fixedpoint_d2string` rounds it with `llrint`.
fn fmt_distance(d: f64) -> Vec<u8> {
    let v = (d * 10_000.0).round_ties_even() as i64;
    let sign = if v < 0 { "-" } else { "" };
    let a = v.unsigned_abs();
    format!("{sign}{}.{:04}", a / 10_000, a % 10_000).into_bytes()
}

/// `geohash_move_x`: the cell `d` columns east (or west), wrapping.
fn move_x(c: Cell, d: i8) -> Cell {
    if d == 0 {
        return c;
    }
    let shift = 64 - u32::from(c.step) * 2;
    let mut x = c.bits & 0xAAAA_AAAA_AAAA_AAAA;
    let y = c.bits & 0x5555_5555_5555_5555;
    let zz = 0x5555_5555_5555_5555u64 >> shift;
    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x = (x | zz).wrapping_sub(zz + 1);
    }
    x &= 0xAAAA_AAAA_AAAA_AAAAu64 >> shift;
    Cell {
        bits: x | y,
        step: c.step,
    }
}

/// `geohash_move_y`: the cell `d` rows north (or south), wrapping.
fn move_y(c: Cell, d: i8) -> Cell {
    if d == 0 {
        return c;
    }
    let shift = 64 - u32::from(c.step) * 2;
    let x = c.bits & 0xAAAA_AAAA_AAAA_AAAA;
    let mut y = c.bits & 0x5555_5555_5555_5555;
    let zz = 0xAAAA_AAAA_AAAA_AAAAu64 >> shift;
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y = (y | zz).wrapping_sub(zz + 1);
    }
    y &= 0x5555_5555_5555_5555u64 >> shift;
    Cell {
        bits: x | y,
        step: c.step,
    }
}

/// `geohashEstimateStepsByRadius`: how coarse the nine cells may be.
fn estimate_steps(range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return STEP_MAX;
    }
    let mut range = range_meters;
    let mut step: i32 = 1;
    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    step -= 2;
    // Wider cells towards the poles.
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, i32::from(STEP_MAX)) as u8
}

/// A search's shape, in the unit it was asked in.
#[derive(Clone, Copy, Debug)]
enum Shape {
    Radius(f64),
    Box { width: f64, height: f64 },
}

/// A search (`GeoShape`): a centre, a shape, and the shape's unit in meters.
#[derive(Clone, Copy, Debug)]
struct Search {
    xy: [f64; 2],
    shape: Shape,
    conversion: f64,
}

impl Search {
    /// `geohashBoundingBox`: [min lon, min lat, max lon, max lat].
    fn bounds(&self) -> [f64; 4] {
        let [lon, lat] = self.xy;
        let (height, width) = match self.shape {
            Shape::Radius(r) => (self.conversion * r, self.conversion * r),
            Shape::Box { width, height } => (
                self.conversion * (height / 2.0),
                self.conversion * (width / 2.0),
            ),
        };
        let lat_delta = rad_deg(height / EARTH_RADIUS_IN_METERS);
        let long_delta_top =
            rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(lat + lat_delta).cos());
        let long_delta_bottom =
            rad_deg(width / EARTH_RADIUS_IN_METERS / deg_rad(lat - lat_delta).cos());
        // The hemispheres widen towards opposite edges.
        let delta = if lat < 0.0 {
            long_delta_bottom
        } else {
            long_delta_top
        };
        [lon - delta, lat - lat_delta, lon + delta, lat + lat_delta]
    }

    /// `geohashCalculateAreasByShapeWGS84`: the cells to read, in the
    /// order a search reads them (its own, then north, south, east, west,
    /// north-east, north-west, south-east, south-west), with the ones that
    /// cannot hold an answer zeroed.
    fn cells(&self) -> [Cell; 9] {
        let [min_lon, min_lat, max_lon, max_lat] = self.bounds();
        let [lon, lat] = self.xy;
        let radius_meters = match self.shape {
            Shape::Radius(r) => r,
            // The half diagonal, its sum fused.
            Shape::Box { width, height } => (width / 2.0)
                .mul_add(width / 2.0, (height / 2.0) * (height / 2.0))
                .sqrt(),
        } * self.conversion;
        let mut steps = estimate_steps(radius_meters, lat);
        let mut hash = encode(LON, LAT, lon, lat, steps);
        let mut around = neighbours(hash);
        // Too coarse when a neighbour stops short of the search's edge.
        let [n, s, e, w] = [around[0], around[1], around[2], around[3]].map(decode);
        let decrease = n.lat.max < max_lat
            || s.lat.min > min_lat
            || e.lon.max < max_lon
            || w.lon.min > min_lon;
        if steps > 1 && decrease {
            steps -= 1;
            hash = encode(LON, LAT, lon, lat, steps);
            around = neighbours(hash);
        }
        let area = decode(hash);
        if steps >= 2 {
            // [n, s, e, w, ne, nw, se, sw]
            let mut zero = |at: [usize; 3]| at.iter().for_each(|&i| around[i] = NO_CELL);
            if area.lat.min < min_lat {
                zero([1, 7, 6]);
            }
            if area.lat.max > max_lat {
                zero([0, 4, 5]);
            }
            if area.lon.min < min_lon {
                zero([3, 7, 5]);
            }
            if area.lon.max > max_lon {
                zero([2, 6, 4]);
            }
        }
        let mut cells = [hash; 9];
        cells[1..].copy_from_slice(&around);
        cells
    }

    /// `geoAppendIfWithinShape`: a point's distance from the centre, in
    /// meters, and the point, when it is inside.
    fn admits(&self, score: f64) -> Option<(f64, [f64; 2])> {
        let xy = point_of(score);
        let [x1, y1] = self.xy;
        let [x2, y2] = xy;
        let dist = match self.shape {
            Shape::Radius(r) => {
                let d = distance(x1, y1, x2, y2);
                if d > r * self.conversion {
                    return None;
                }
                d
            }
            // `geohashGetDistanceIfInRectangle`.
            Shape::Box { width, height } => {
                if lat_distance(y2, y1) > height * self.conversion / 2.0 {
                    return None;
                }
                if distance(x2, y2, x1, y2) > width * self.conversion / 2.0 {
                    return None;
                }
                distance(x1, y1, x2, y2)
            }
        };
        Some((dist, xy))
    }
}

/// `geohashNeighbors`: [n, s, e, w, ne, nw, se, sw].
fn neighbours(c: Cell) -> [Cell; 8] {
    [
        move_y(c, 1),
        move_y(c, -1),
        move_x(c, 1),
        move_x(c, -1),
        move_y(move_x(c, 1), 1),
        move_y(move_x(c, -1), 1),
        move_y(move_x(c, 1), -1),
        move_y(move_x(c, -1), -1),
    ]
}

/// `extractLongLatOrReply`: both parse, then the pair is checked.
fn lon_lat(lon: &[u8], lat: &[u8]) -> Result<[f64; 2], Value> {
    let parse = |raw: &[u8]| parse_f64(raw).map_err(|()| err("ERR value is not a valid float"));
    let xy = [parse(lon)?, parse(lat)?];
    if !(LON.min..=LON.max).contains(&xy[0]) || !(LAT.min..=LAT.max).contains(&xy[1]) {
        return Err(err(&format!(
            "ERR invalid longitude,latitude pair {:.6},{:.6}",
            xy[0], xy[1]
        )));
    }
    Ok(xy)
}

/// `extractUnitOrReply`: a unit's length in meters.
fn unit(raw: &[u8]) -> Result<f64, Value> {
    match raw.to_ascii_lowercase().as_slice() {
        b"m" => Ok(1.0),
        b"km" => Ok(1000.0),
        b"ft" => Ok(0.3048),
        b"mi" => Ok(1609.34),
        _ => Err(err(
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        )),
    }
}

/// `extractDistanceOrReply`: a radius and its unit.
fn radius_arg(radius: &[u8], u: &[u8]) -> Result<(Shape, f64), Value> {
    let r = parse_f64(radius).map_err(|()| err("ERR need numeric radius"))?;
    if r < 0.0 {
        return Err(err("ERR radius cannot be negative"));
    }
    Ok((Shape::Radius(r), unit(u)?))
}

/// `extractBoxOrReply`: a width, a height and their unit.
fn box_arg(width: &[u8], height: &[u8], u: &[u8]) -> Result<(Shape, f64), Value> {
    let w = parse_f64(width).map_err(|()| err("ERR need numeric width"))?;
    let h = parse_f64(height).map_err(|()| err("ERR need numeric height"))?;
    if h < 0.0 || w < 0.0 {
        return Err(err("ERR height or width cannot be negative"));
    }
    Ok((
        Shape::Box {
            width: w,
            height: h,
        },
        unit(u)?,
    ))
}

/// Valkey 9.1's refusal of a centre member the set does not hold, where
/// Redis 8.2 says `could not decode requested zset member`. The member is
/// printed with `%s`, so it ends at a NUL.
fn no_member(member: &[u8]) -> Value {
    let shown = member.split(|&b| b == 0).next().unwrap_or_default();
    err(&format!(
        "ERR member {} does not exist",
        String::from_utf8_lossy(shown)
    ))
}

/// The six searches (`georadiusGeneric`'s flags).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GeoSearch {
    Radius,
    RadiusRo,
    ByMember,
    ByMemberRo,
    Search,
    SearchStore,
}

impl GeoSearch {
    fn name(self) -> &'static str {
        match self {
            Self::Radius => "georadius",
            Self::RadiusRo => "georadius_ro",
            Self::ByMember => "georadiusbymember",
            Self::ByMemberRo => "georadiusbymember_ro",
            Self::Search => "geosearch",
            Self::SearchStore => "geosearchstore",
        }
    }

    /// Where the options begin, which is also the fewest arguments but
    /// for GEOSEARCH (`FROM... BY...` are options there, arity -7) and
    /// GEOSEARCHSTORE (-8).
    fn base(self) -> usize {
        match self {
            Self::Radius | Self::RadiusRo => 6,
            Self::ByMember | Self::ByMemberRo => 5,
            Self::Search => 2,
            Self::SearchStore => 3,
        }
    }

    fn min_args(self) -> usize {
        match self {
            Self::Search => 7,
            Self::SearchStore => 8,
            other => other.base(),
        }
    }

    fn source(self) -> usize {
        if self == Self::SearchStore { 2 } else { 1 }
    }

    /// GEORADIUS and GEORADIUSBYMEMBER take STORE and STOREDIST.
    fn may_store(self) -> bool {
        matches!(self, Self::Radius | Self::ByMember)
    }

    fn is_search(self) -> bool {
        matches!(self, Self::Search | Self::SearchStore)
    }
}

/// The key GEORADIUS or GEORADIUSBYMEMBER stores at, read from its options
/// as the parse reads them, for the slot check that comes first.
fn store_key(opts: &[Vec<u8>]) -> Option<&Vec<u8>> {
    let mut key = None;
    let mut i = 0;
    while i < opts.len() {
        let more = i + 1 < opts.len();
        match opts[i].to_ascii_uppercase().as_slice() {
            b"WITHDIST" | b"WITHHASH" | b"WITHCOORD" | b"ANY" | b"ASC" | b"DESC" => {}
            b"COUNT" if more => i += 1,
            b"STORE" | b"STOREDIST" if more => {
                key = Some(&opts[i + 1]);
                i += 1;
            }
            _ => break,
        }
        i += 1;
    }
    key
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sort {
    None,
    Asc,
    Desc,
}

/// One point a search found (`geoPoint`).
struct Hit {
    member: Vec<u8>,
    score: f64,
    dist: f64,
    xy: [f64; 2],
}

impl Dispatcher<'_> {
    /// `GEOADD key [NX|XX] [CH] longitude latitude member ...`: ZADD of
    /// each point's score, as upstream hands it to ZADD.
    pub(super) fn cmd_geoadd(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 5 {
            return arity_err("geoadd");
        }
        let (mut nx, mut xx) = (false, false);
        let mut at = 2;
        while let Some(opt) = args.get(at) {
            match opt.to_ascii_uppercase().as_slice() {
                b"NX" => nx = true,
                b"XX" => xx = true,
                b"CH" => {}
                _ => break,
            }
            at += 1;
        }
        let points = &args[at..];
        if !points.len().is_multiple_of(3) || (nx && xx) {
            return err("ERR syntax error");
        }
        let mut zadd = args[..at].to_vec();
        zadd[0] = b"ZADD".to_vec();
        for p in points.chunks(3) {
            match lon_lat(&p[0], &p[1]) {
                Ok(xy) => {
                    zadd.push(score_of(xy).to_string().into_bytes());
                    zadd.push(p[2].clone());
                }
                Err(e) => return e,
            }
        }
        self.cmd_zadd(&zadd)
    }

    /// `GEOPOS key [member ...]`: each member's point, or a null.
    pub(super) fn cmd_geopos(&self, args: &[Vec<u8>]) -> Value {
        self.each_point(
            args,
            "geopos",
            |xy| {
                Value::Array(Some(vec![
                    Value::HumanDouble(xy[0]),
                    Value::HumanDouble(xy[1]),
                ]))
            },
            Value::Array(None),
        )
    }

    /// `GEOHASH key [member ...]`: each member's standard 11-character
    /// geohash, or a null. A score covers latitudes to ±85.05 only, so the
    /// point is encoded again over ±90; the eleventh character stands for
    /// bits a score does not have, and is always `0`.
    pub(super) fn cmd_geohash(&self, args: &[Vec<u8>]) -> Value {
        self.each_point(
            args,
            "geohash",
            |xy| {
                let std_lat = Range {
                    min: -90.0,
                    max: 90.0,
                };
                let bits = encode(LON, std_lat, xy[0], xy[1], STEP_MAX).bits;
                let mut hash = Vec::with_capacity(11);
                for i in 0..10 {
                    hash.push(GEOALPHABET[((bits >> (52 - (i + 1) * 5)) & 0x1f) as usize]);
                }
                hash.push(GEOALPHABET[0]);
                Value::Bulk(Some(hash))
            },
            Value::Null,
        )
    }

    /// GEOPOS's and GEOHASH's shared walk: the key's type first, then one
    /// answer per member.
    fn each_point(
        &self,
        args: &[Vec<u8>],
        name: &str,
        answer: impl Fn([f64; 2]) -> Value,
        missing: Value,
    ) -> Value {
        if args.len() < 2 {
            return arity_err(name);
        }
        let slot = slot_for_key(&args[1]);
        if let Err(e) = self.zsets.zcard(slot, &args[1]) {
            return store_err(e);
        }
        let mut out = Vec::with_capacity(args.len() - 2);
        for member in &args[2..] {
            match self.zsets.zscore(slot, &args[1], member) {
                Ok(Some(score)) => out.push(answer(point_of(score))),
                Ok(None) => out.push(missing.clone()),
                Err(e) => return store_err(e),
            }
        }
        Value::Array(Some(out))
    }

    /// `GEODIST key member1 member2 [M|KM|FT|MI]`: four decimals, or a null
    /// when either member is missing.
    pub(super) fn cmd_geodist(&self, args: &[Vec<u8>]) -> Value {
        let to_meter = match args.len() {
            0..=3 => return arity_err("geodist"),
            4 => 1.0,
            5 => match unit(&args[4]) {
                Ok(u) => u,
                Err(e) => return e,
            },
            _ => return err("ERR syntax error"),
        };
        let slot = slot_for_key(&args[1]);
        let score = |m: &[u8]| self.zsets.zscore(slot, &args[1], m);
        let (a, b) = match (score(&args[2]), score(&args[3])) {
            (Ok(Some(a)), Ok(Some(b))) => (a, b),
            (Err(e), _) | (_, Err(e)) => return store_err(e),
            _ => return Value::Null,
        };
        let (p, q) = (point_of(a), point_of(b));
        Value::Bulk(Some(fmt_distance(
            distance(p[0], p[1], q[0], q[1]) / to_meter,
        )))
    }

    /// GEORADIUS, GEORADIUSBYMEMBER, their `_RO` forms, GEOSEARCH and
    /// GEOSEARCHSTORE (`georadiusGeneric`).
    pub(super) fn cmd_geosearch(&self, args: &[Vec<u8>], kind: GeoSearch) -> Value {
        if args.len() < kind.min_args() {
            return arity_err(kind.name());
        }
        // Keys in two slots are refused before anything is read, as Redis
        // Cluster refuses them before running the command.
        let other = match kind {
            GeoSearch::SearchStore => Some(&args[2]),
            k if k.may_store() => store_key(&args[k.base()..]),
            _ => None,
        };
        if let Some(e) = other.and_then(|k| Self::crossslot(&args[1], std::slice::from_ref(k))) {
            return e;
        }
        self.geosearch(args, kind).unwrap_or_else(|e| e)
    }

    fn geosearch(&self, args: &[Vec<u8>], kind: GeoSearch) -> Result<Value, Value> {
        let src = &args[kind.source()];
        let slot = slot_for_key(src);
        let exists = self.zsets.zcard(slot, src).map_err(store_err)? > 0;
        let member_point = |m: &[u8]| match self.zsets.zscore(slot, src, m) {
            Ok(Some(score)) => Ok(point_of(score)),
            Ok(None) => Err(no_member(m)),
            Err(e) => Err(store_err(e)),
        };
        let (mut xy, mut shape, mut conversion) = (None, None, 1.0);
        match kind {
            GeoSearch::Radius | GeoSearch::RadiusRo => {
                xy = Some(lon_lat(&args[2], &args[3])?);
                let (s, c) = radius_arg(&args[4], &args[5])?;
                (shape, conversion) = (Some(s), c);
            }
            // A missing key reads nothing, but its options are still
            // checked so the reply is the right empty one.
            GeoSearch::ByMember | GeoSearch::ByMemberRo if exists => {
                xy = Some(member_point(&args[2])?);
                let (s, c) = radius_arg(&args[3], &args[4])?;
                (shape, conversion) = (Some(s), c);
            }
            _ => {}
        }

        let (mut withdist, mut withhash, mut withcoord, mut any) = (false, false, false, false);
        let (mut frommember, mut fromloc, mut byradius, mut bybox) = (false, false, false, false);
        let mut sort = Sort::None;
        let mut count: i64 = 0;
        let mut store = (kind == GeoSearch::SearchStore).then_some(&args[1]);
        let mut storedist = false;
        let opts = &args[kind.base()..];
        let mut i = 0;
        while i < opts.len() {
            let left = opts.len() - i - 1;
            match opts[i].to_ascii_uppercase().as_slice() {
                b"WITHDIST" => withdist = true,
                b"WITHHASH" => withhash = true,
                b"WITHCOORD" => withcoord = true,
                b"ANY" => any = true,
                b"ASC" => sort = Sort::Asc,
                b"DESC" => sort = Sort::Desc,
                b"COUNT" if left >= 1 => {
                    count = parse_i64(&opts[i + 1])
                        .map_err(|()| err("ERR value is not an integer or out of range"))?;
                    if count <= 0 {
                        return Err(err("ERR COUNT must be > 0"));
                    }
                    i += 1;
                }
                b"STORE" if left >= 1 && kind.may_store() => {
                    (store, storedist) = (Some(&opts[i + 1]), false);
                    i += 1;
                }
                b"STOREDIST" if left >= 1 && kind.may_store() => {
                    (store, storedist) = (Some(&opts[i + 1]), true);
                    i += 1;
                }
                b"STOREDIST" if kind == GeoSearch::SearchStore => storedist = true,
                b"FROMMEMBER" if left >= 1 && kind.is_search() && !fromloc => {
                    if exists {
                        xy = Some(member_point(&opts[i + 1])?);
                    }
                    frommember = true;
                    i += 1;
                }
                b"FROMLONLAT" if left >= 2 && kind.is_search() && !frommember => {
                    xy = Some(lon_lat(&opts[i + 1], &opts[i + 2])?);
                    fromloc = true;
                    i += 2;
                }
                b"BYRADIUS" if left >= 2 && kind.is_search() && !bybox => {
                    let (s, c) = radius_arg(&opts[i + 1], &opts[i + 2])?;
                    (shape, conversion, byradius) = (Some(s), c, true);
                    i += 2;
                }
                b"BYBOX" if left >= 3 && kind.is_search() && !byradius => {
                    let (s, c) = box_arg(&opts[i + 1], &opts[i + 2], &opts[i + 3])?;
                    (shape, conversion, bybox) = (Some(s), c, true);
                    i += 3;
                }
                _ => return Err(err("ERR syntax error")),
            }
            i += 1;
        }

        if store.is_some() && (withdist || withhash || withcoord) {
            return Err(err(&format!(
                "ERR {} is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
                if kind == GeoSearch::SearchStore {
                    "GEOSEARCHSTORE"
                } else {
                    "STORE option in GEORADIUS"
                }
            )));
        }
        let name = String::from_utf8_lossy(&args[0]);
        if kind.is_search() && !(frommember || fromloc) {
            return Err(err(&format!(
                "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for {name}"
            )));
        }
        if kind.is_search() && !(byradius || bybox) {
            return Err(err(&format!(
                "ERR exactly one of BYRADIUS and BYBOX can be specified for {name}"
            )));
        }
        if any && count == 0 {
            return Err(err("ERR the ANY argument requires COUNT argument"));
        }
        let store_at = |dst: &Vec<u8>, pairs: &[(f64, Vec<u8>)]| {
            self.zsets
                .zreplace(slot_for_key(dst), dst, pairs)
                .map_err(store_err)
        };
        if !exists {
            return Ok(match store {
                Some(dst) => {
                    store_at(dst, &[])?;
                    Value::Integer(0)
                }
                None => Value::Array(Some(Vec::new())),
            });
        }
        let (Some(xy), Some(shape)) = (xy, shape) else {
            // Every path that reaches here has set both.
            return Err(err("ERR syntax error"));
        };

        // A COUNT without a sort returns the nearest, unless it is ANY's.
        if count != 0 && sort == Sort::None && !any {
            sort = Sort::Asc;
        }
        let search = Search {
            xy,
            shape,
            conversion,
        };
        let limit = if any { count as usize } else { 0 };
        let mut hits = self.points_near(slot, src, &search, limit)?;
        if hits.is_empty() && store.is_none() {
            return Ok(Value::Array(Some(Vec::new())));
        }
        // Ties keep the order the cells were read in; upstream's `qsort`
        // leaves theirs to the C library.
        match sort {
            Sort::Asc => hits.sort_by(|a, b| a.dist.total_cmp(&b.dist)),
            Sort::Desc => hits.sort_by(|a, b| b.dist.total_cmp(&a.dist)),
            Sort::None => {}
        }
        if count > 0 {
            hits.truncate(usize::try_from(count).unwrap_or(usize::MAX));
        }
        for h in &mut hits {
            h.dist /= conversion;
        }

        if let Some(dst) = store {
            let n = hits.len();
            let pairs: Vec<(f64, Vec<u8>)> = hits
                .into_iter()
                .map(|h| (if storedist { h.dist } else { h.score }, h.member))
                .collect();
            store_at(dst, &pairs)?;
            return Ok(Value::Integer(n as i64));
        }
        let plain = !(withdist || withhash || withcoord);
        let items = hits
            .into_iter()
            .map(|h| {
                if plain {
                    return Value::Bulk(Some(h.member));
                }
                let mut item = vec![Value::Bulk(Some(h.member))];
                if withdist {
                    item.push(Value::Bulk(Some(fmt_distance(h.dist))));
                }
                if withhash {
                    item.push(Value::Integer(h.score as i64));
                }
                if withcoord {
                    item.push(Value::Array(Some(vec![
                        Value::HumanDouble(h.xy[0]),
                        Value::HumanDouble(h.xy[1]),
                    ])));
                }
                Value::Array(Some(item))
            })
            .collect();
        Ok(Value::Array(Some(items)))
    }

    /// `membersOfAllNeighbors`: the points inside the search, cell by
    /// cell. A cell that repeats the one read last is skipped, as upstream
    /// skips it, which never compares with the first. `limit` is ANY's
    /// count, or 0.
    fn points_near(
        &self,
        slot: u16,
        key: &[u8],
        search: &Search,
        limit: usize,
    ) -> Result<Vec<Hit>, Value> {
        let cells = search.cells();
        let full = |hits: &Vec<Hit>| !hits.is_empty() && limit != 0 && hits.len() >= limit;
        let mut hits = Vec::new();
        let mut last = 0;
        for (i, &cell) in cells.iter().enumerate() {
            if cell == NO_CELL || (last != 0 && cell == cells[last]) {
                continue;
            }
            if full(&hits) {
                break;
            }
            // `geoGetPointsInRange`: scores from the cell's first point to
            // the next cell's.
            let next = Cell {
                bits: cell.bits.wrapping_add(1),
                step: cell.step,
            };
            let rows = self
                .zsets
                .zrange_by_score(
                    slot,
                    key,
                    ScoreBound {
                        value: align52(cell) as f64,
                        inclusive: true,
                    },
                    ScoreBound {
                        value: align52(next) as f64,
                        inclusive: false,
                    },
                    false,
                    0,
                    -1,
                )
                .map_err(store_err)?;
            for (member, score) in rows {
                if let Some((dist, xy)) = search.admits(score) {
                    hits.push(Hit {
                        member,
                        score,
                        dist,
                        xy,
                    });
                }
                if full(&hits) {
                    break;
                }
            }
            last = i;
        }
        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flint_storage::MemKv;
    use flint_storage::strings::system_clock;

    fn call(kv: &MemKv, parts: &[&str]) -> Value {
        let d = Dispatcher::new(kv, system_clock);
        d.dispatch(
            &parts
                .iter()
                .map(|p| p.as_bytes().to_vec())
                .collect::<Vec<_>>(),
        )
    }

    fn bulk(s: &str) -> Value {
        Value::Bulk(Some(s.as_bytes().to_vec()))
    }

    /// The documentation's Sicily, with the replies Valkey 9.1 gives.
    fn sicily() -> MemKv {
        let kv = MemKv::new();
        assert_eq!(
            call(
                &kv,
                &[
                    "GEOADD",
                    "Sicily",
                    "13.361389",
                    "38.115556",
                    "Palermo",
                    "15.087269",
                    "37.502669",
                    "Catania",
                ],
            ),
            Value::Integer(2)
        );
        kv
    }

    #[test]
    fn points_are_stored_read_and_measured_as_valkey_does() {
        let kv = sicily();
        assert_eq!(
            call(&kv, &["ZSCORE", "Sicily", "Palermo"]),
            Value::Double(3_479_099_956_230_698.0)
        );
        assert_eq!(
            call(&kv, &["GEOPOS", "Sicily", "Palermo", "nope"]),
            Value::Array(Some(vec![
                Value::Array(Some(vec![
                    Value::HumanDouble(13.361_389_338_970_184),
                    Value::HumanDouble(38.115_556_395_496_3),
                ])),
                Value::Array(None),
            ]))
        );
        assert_eq!(
            flint_resp::fmt_human_double(13.361_389_338_970_184),
            b"13.36138933897018433"
        );
        assert_eq!(
            call(&kv, &["GEOHASH", "Sicily", "Palermo", "Catania", "nope"]),
            Value::Array(Some(vec![
                bulk("sqc8b49rny0"),
                bulk("sqdtr74hyu0"),
                Value::Null
            ]))
        );
        assert_eq!(
            call(&kv, &["GEODIST", "Sicily", "Palermo", "Catania"]),
            bulk("166274.1516")
        );
        assert_eq!(
            call(&kv, &["GEODIST", "Sicily", "Palermo", "Catania", "KM"]),
            bulk("166.2742")
        );
        assert_eq!(
            call(&kv, &["GEODIST", "Sicily", "Palermo", "nope"]),
            Value::Null
        );
    }

    #[test]
    fn a_search_answers_in_upstreams_order_with_its_options() {
        let kv = sicily();
        // Unsorted: the order the cells are read in.
        assert_eq!(
            call(
                &kv,
                &[
                    "GEOSEARCH",
                    "Sicily",
                    "FROMLONLAT",
                    "15",
                    "37",
                    "BYRADIUS",
                    "200",
                    "km",
                    "WITHDIST",
                    "WITHHASH"
                ],
            ),
            Value::Array(Some(vec![
                Value::Array(Some(vec![
                    bulk("Palermo"),
                    bulk("190.4424"),
                    Value::Integer(3_479_099_956_230_698),
                ])),
                Value::Array(Some(vec![
                    bulk("Catania"),
                    bulk("56.4413"),
                    Value::Integer(3_479_447_370_796_909),
                ])),
            ]))
        );
        assert_eq!(
            call(
                &kv,
                &["GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "1"]
            ),
            Value::Array(Some(vec![bulk("Catania")]))
        );
        assert_eq!(
            call(&kv, &["GEORADIUSBYMEMBER", "Sicily", "nope", "1", "km"]),
            err("ERR member nope does not exist")
        );
        assert_eq!(
            call(
                &kv,
                &[
                    "GEORADIUS",
                    "Sicily",
                    "15",
                    "37",
                    "200",
                    "km",
                    "STORE",
                    "Sicily"
                ]
            ),
            Value::Integer(2)
        );
        assert_eq!(
            call(
                &kv,
                &[
                    "GEOSEARCHSTORE",
                    "{g}d",
                    "{g}none",
                    "FROMMEMBER",
                    "x",
                    "BYBOX",
                    "1",
                    "1",
                    "m"
                ]
            ),
            Value::Integer(0)
        );
        assert_eq!(
            call(
                &kv,
                &[
                    "GEORADIUS_RO",
                    "Sicily",
                    "15",
                    "37",
                    "200",
                    "km",
                    "STORE",
                    "d"
                ]
            ),
            err("ERR syntax error")
        );
    }

    #[test]
    fn a_store_in_another_slot_is_refused_before_anything_runs() {
        // `a` is slot 15495, `b` 3300.
        let kv = MemKv::new();
        for c in [
            &["GEORADIUS", "a", "0", "0", "1", "km", "STORE", "b"][..],
            &["GEORADIUSBYMEMBER", "a", "m", "1", "km", "STOREDIST", "b"],
            &[
                "GEOSEARCHSTORE",
                "a",
                "b",
                "FROMLONLAT",
                "0",
                "0",
                "BYRADIUS",
                "1",
                "km",
            ],
        ] {
            assert!(
                matches!(call(&kv, c), Value::Error(ref e) if e.starts_with("CROSSSLOT")),
                "{c:?}"
            );
            let args: Vec<Vec<u8>> = c.iter().map(|p| p.as_bytes().to_vec()).collect();
            assert!(
                queue_time_error(&args, false).is_some(),
                "{c:?} at queue time"
            );
        }
    }

    #[test]
    fn cells_and_points_are_upstreams() {
        // A score names its cell's middle, and the middle encodes back to it.
        for xy in [
            [13.361389, 38.115556],
            [-180.0, -85.05112878],
            [180.0, 85.05112878],
            [0.0, 0.0],
        ] {
            let s = score_of(xy) as f64;
            let p = point_of(s);
            assert_eq!(score_of(p) as f64, s, "{xy:?}");
        }
        // The neighbours wrap at the edges, as upstream's carries do.
        let c = encode(LON, LAT, 179.9, 0.0, 2);
        assert_eq!(move_x(move_x(c, 1), -1), c);
        assert_eq!(move_y(move_y(c, -1), 1), c);
        assert_eq!(estimate_steps(0.0, 0.0), 26);
        assert_eq!(estimate_steps(1e9, 0.0), 1);
        assert_eq!(fmt_distance(0.0), b"0.0000");
        // A half rounds to even, as `llrint` rounds it: 0.5 and 2.5 ten
        // thousandths, exactly.
        assert_eq!(fmt_distance(0.000_05), b"0.0000");
        assert_eq!(fmt_distance(0.000_25), b"0.0002");
        assert_eq!(fmt_distance(56.441_25), b"56.4412");
        assert_eq!(fmt_distance(166_274.151_6), b"166274.1516");
    }
}
