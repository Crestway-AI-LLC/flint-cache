// SPDX-License-Identifier: Elastic-2.0
//! GEO points as RediSearch indexes and searches them (ADR-0062 B).
//!
//! A point is stored as a 52-bit geohash: 26 bits of latitude and 26 of
//! longitude, interleaved, latitude in the even bits. Latitude spans
//! ±85.05112878 (the Web Mercator square), so a point nearer a pole than that
//! has no hash and is never matched. A radius query reads up to nine ranges of
//! hashes, a cell sized to the radius and its eight neighbours, and keeps a
//! point when the haversine distance from the query's centre to its CELL'S
//! CENTRE is at most the radius. Doing the same here is what makes an answer
//! agree with RediSearch's at the edge of a radius, where the stored point and
//! the cell centre differ by up to half a cell (about 0.3 m).

/// Bits for each coordinate: 26, so a hash is 52 bits.
pub const STEP_MAX: u32 = 26;
pub const LAT_MIN: f64 = -85.05112878;
pub const LAT_MAX: f64 = 85.05112878;
pub const LON_MIN: f64 = -180.0;
pub const LON_MAX: f64 = 180.0;
/// The half-width of the Web Mercator square, in metres: how big a cell is
/// at each step.
const MERCATOR_MAX: f64 = 20037726.37;
/// Redis's Earth radius for its geo distances, in metres.
pub const EARTH_RADIUS_M: f64 = 6372797.560856;

/// A radius unit's length in metres, by RediSearch's names in any case.
pub fn unit_metres(unit: &[u8]) -> Option<f64> {
    Some(match unit.to_ascii_lowercase().as_slice() {
        b"m" => 1.0,
        b"km" => 1000.0,
        b"ft" => 0.3048,
        b"mi" => 1609.34,
        _ => return None,
    })
}

/// A GEO value: `lon,lat` or `lon lat`, each part trimmed and finite. Not a
/// check of the ranges, which [`hash`] makes.
pub fn parse_point(raw: &[u8]) -> Result<(f64, f64), String> {
    let s = std::str::from_utf8(raw).map_err(|_| "is not UTF-8".to_string())?;
    let at = s
        .find([',', ' '])
        .ok_or_else(|| format!("{s:?} is not lon,lat"))?;
    let num = |part: &str| -> Result<f64, String> {
        part.trim()
            .parse::<f64>()
            .ok()
            .filter(|x| x.is_finite())
            .ok_or_else(|| format!("{s:?} is not lon,lat"))
    };
    Ok((num(&s[..at])?, num(&s[at + 1..])?))
}

/// Whether a point has a hash: inside the Web Mercator square.
pub fn hashable(lon: f64, lat: f64) -> bool {
    (LON_MIN..=LON_MAX).contains(&lon) && (LAT_MIN..=LAT_MAX).contains(&lat)
}

/// A coordinate's cell at `step` bits: its offset in the range, scaled and
/// floored, the top edge folded into the last cell.
fn cell(x: f64, min: f64, max: f64, step: u32) -> u64 {
    let size = (1u64 << step) as f64;
    (((x - min) / (max - min) * size) as u64).min((1u64 << step) - 1)
}

/// `lat` in the even bits, `lon` in the odd, `step` of each.
fn interleave(lat: u64, lon: u64, step: u32) -> u64 {
    let mut h = 0u64;
    for b in 0..step {
        h |= ((lat >> b) & 1) << (2 * b);
        h |= ((lon >> b) & 1) << (2 * b + 1);
    }
    h
}

/// Inverse of [`interleave`]: `(lat, lon)`.
fn deinterleave(h: u64, step: u32) -> (u64, u64) {
    let (mut lat, mut lon) = (0u64, 0u64);
    for b in 0..step {
        lat |= ((h >> (2 * b)) & 1) << b;
        lon |= ((h >> (2 * b + 1)) & 1) << b;
    }
    (lat, lon)
}

/// The cell of `(lon, lat)` at `step`, as interleaved bits.
fn encode(lon: f64, lat: f64, step: u32) -> u64 {
    interleave(
        cell(lat, LAT_MIN, LAT_MAX, step),
        cell(lon, LON_MIN, LON_MAX, step),
        step,
    )
}

/// The stored form of a point: its 52-bit hash. `None` for a point nearer a
/// pole than the square reaches, or past ±180.
pub fn hash(lon: f64, lat: f64) -> Option<u64> {
    hashable(lon, lat).then(|| encode(lon, lat, STEP_MAX))
}

/// The centre of a 52-bit hash's cell, `(lon, lat)`: the point RediSearch
/// measures from.
pub fn centre(h: u64) -> (f64, f64) {
    let (lat, lon) = deinterleave(h, STEP_MAX);
    let size = (1u64 << STEP_MAX) as f64;
    (
        LON_MIN + (lon as f64 + 0.5) / size * (LON_MAX - LON_MIN),
        LAT_MIN + (lat as f64 + 0.5) / size * (LAT_MAX - LAT_MIN),
    )
}

/// The haversine distance in metres, on Redis's sphere.
pub fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (lat1, lat2) = (lat1.to_radians(), lat2.to_radians());
    let u = ((lat2 - lat1) / 2.0).sin();
    let v = ((lon2.to_radians() - lon1.to_radians()) / 2.0).sin();
    2.0 * EARTH_RADIUS_M
        * (u * u + lat1.cos() * lat2.cos() * v * v)
            .sqrt()
            .min(1.0)
            .asin()
}

/// Whether the point stored as `h` is within `radius_m` of `(lon, lat)`: its
/// cell's centre at most that far.
pub fn within(h: u64, lon: f64, lat: f64, radius_m: f64) -> bool {
    let (x, y) = centre(h);
    distance(lon, lat, x, y) <= radius_m
}

/// The step whose cells are about the size of `radius_m` at `lat`, coarser
/// near the poles, where a cell narrows.
fn steps_for(radius_m: f64, lat: f64) -> u32 {
    if radius_m.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
        return STEP_MAX;
    }
    // The fewest doublings of the radius that reach the square's half-width,
    // less one.
    let ratio = (MERCATOR_MAX / radius_m).ceil().min(u32::MAX as f64) as u64;
    let doublings = ratio.max(1).next_power_of_two().ilog2() as i32;
    let polar = match lat.abs() {
        a if a > 80.0 => 2,
        a if a > 66.0 => 1,
        _ => 0,
    };
    (doublings - 1 - polar).clamp(1, STEP_MAX as i32) as u32
}

/// A cell's bounds, `(lon_min, lon_max, lat_min, lat_max)`.
fn bounds(bits: u64, step: u32) -> (f64, f64, f64, f64) {
    let (lat, lon) = deinterleave(bits, step);
    let size = (1u64 << step) as f64;
    let at = |i: u64, min: f64, max: f64| min + i as f64 / size * (max - min);
    (
        at(lon, LON_MIN, LON_MAX),
        at(lon + 1, LON_MIN, LON_MAX),
        at(lat, LAT_MIN, LAT_MAX),
        at(lat + 1, LAT_MIN, LAT_MAX),
    )
}

/// The cell `dlon` cells east and `dlat` north of `bits`, wrapping at each
/// edge of the square as the hash's bits do.
fn moved(bits: u64, step: u32, dlon: i64, dlat: i64) -> u64 {
    let (lat, lon) = deinterleave(bits, step);
    let n = 1i64 << step;
    let wrap = |x: u64, d: i64| (x as i64 + d).rem_euclid(n) as u64;
    interleave(wrap(lat, dlat), wrap(lon, dlon), step)
}

/// The hashes a radius query reads: up to nine half-open ranges of 52-bit
/// hashes, the centre's cell at a step sized to the radius and its eight
/// neighbours, less the neighbours outside the circle's bounding box. Empty
/// for a centre nearer a pole than the square reaches, which no point can be
/// within, as RediSearch answers it.
pub fn ranges(lon: f64, lat: f64, radius_m: f64) -> Vec<(u64, u64)> {
    if !hashable(lon, lat) {
        return Vec::new();
    }
    // The circle's bounding box, by a flat approximation: it only prunes
    // neighbours, and every candidate is measured after.
    let lat_r = lat.to_radians();
    let dlon = (radius_m / EARTH_RADIUS_M / lat_r.cos()).to_degrees();
    let dlat = (radius_m / EARTH_RADIUS_M).to_degrees();
    let (min_lon, max_lon, min_lat, max_lat) = (lon - dlon, lon + dlon, lat - dlat, lat + dlat);

    let mut step = steps_for(radius_m, lat);
    let mut centre_cell = encode(lon, lat, step);
    // A neighbour nearer the centre than the radius cannot cover it: one
    // step coarser.
    let along_meridian = |lat2: f64| {
        2.0 * EARTH_RADIUS_M
            * ((lat2.to_radians() - lat_r) / 2.0)
                .sin()
                .abs()
                .min(1.0)
                .asin()
    };
    let along_parallel = |lon2: f64| {
        2.0 * EARTH_RADIUS_M
            * (lat_r.cos() * ((lon2.to_radians() - lon.to_radians()) / 2.0).sin().abs())
                .min(1.0)
                .asin()
    };
    let too_near = {
        let north = bounds(moved(centre_cell, step, 0, 1), step);
        let south = bounds(moved(centre_cell, step, 0, -1), step);
        let east = bounds(moved(centre_cell, step, 1, 0), step);
        let west = bounds(moved(centre_cell, step, -1, 0), step);
        along_meridian(north.3) < radius_m
            || along_meridian(south.2) < radius_m
            || along_parallel(east.1) < radius_m
            || along_parallel(west.0) < radius_m
    };
    if step > 1 && too_near {
        step -= 1;
        centre_cell = encode(lon, lat, step);
    }
    let (c_lon_min, c_lon_max, c_lat_min, c_lat_max) = bounds(centre_cell, step);
    // (east, north) of each cell read, centre first.
    let mut cells: Vec<(i64, i64)> = vec![
        (0, 0),
        (0, 1),
        (0, -1),
        (1, 0),
        (-1, 0),
        (1, 1),
        (-1, 1),
        (1, -1),
        (-1, -1),
    ];
    if step >= 2 {
        cells.retain(|&(e, n)| {
            !((n == -1 && c_lat_min < min_lat)
                || (n == 1 && c_lat_max > max_lat)
                || (e == -1 && c_lon_min < min_lon)
                || (e == 1 && c_lon_max > max_lon))
        });
    }
    let shift = 2 * (STEP_MAX - step);
    let mut out: Vec<(u64, u64)> = cells
        .into_iter()
        .map(|(e, n)| {
            let b = moved(centre_cell, step, e, n);
            (b << shift, (b + 1) << shift)
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Redis's documented Sicily example (`GEOADD Sicily 13.361389 38.115556
    /// "Palermo" 15.087269 37.502669 "Catania"`): the scores `ZRANGE`
    /// answers, the positions `GEOPOS` answers, and `GEODIST` between them.
    #[test]
    fn hashes_centres_and_distances_agree_with_redis_s_own_example() {
        let palermo = hash(13.361389, 38.115556).expect("hashable");
        let catania = hash(15.087269, 37.502669).expect("hashable");
        assert_eq!(palermo, 3479099956230698);
        assert_eq!(catania, 3479447370796909);
        let (lon, lat) = centre(palermo);
        assert!((lon - 13.361_389_338_970_184).abs() < 1e-12, "{lon}");
        assert!((lat - 38.115_556_395_496_3).abs() < 1e-12, "{lat}");
        let (clon, clat) = centre(catania);
        let d = distance(lon, lat, clon, clat);
        assert!((d - 166274.1516).abs() < 0.001, "{d}");
        // `GEORADIUS Sicily 15 37 200 km`: both, Palermo at 190.4424 km.
        assert!(within(palermo, 15.0, 37.0, 200_000.0));
        assert!(within(catania, 15.0, 37.0, 200_000.0));
        assert!(!within(palermo, 15.0, 37.0, 190_000.0));
        assert!((distance(15.0, 37.0, lon, lat) - 190442.4).abs() < 0.1);
    }

    /// The parts RediSearch reads, and what it does not.
    #[test]
    fn a_value_is_lon_and_lat_by_a_comma_or_a_space() {
        assert_eq!(parse_point(b"13.36,38.11"), Ok((13.36, 38.11)));
        assert_eq!(parse_point(b"13.36 38.11"), Ok((13.36, 38.11)));
        // RediSearch's own examples: each part trimmed after the split.
        assert_eq!(parse_point(b"10.0, 20.0"), Ok((10.0, 20.0)));
        assert_eq!(parse_point(b"10.0,  20.0"), Ok((10.0, 20.0)));
        // It splits at the first comma or space before trimming, so a
        // leading blank, or one before the comma, leaves a part empty.
        for bad in [
            &b" -122.4,37.7"[..],
            b"-122.4 , 37.7",
            b"13.36",
            b"a,b",
            b"1,inf",
            b"1,NaN",
            b"",
            b"\xff,1",
        ] {
            assert!(parse_point(bad).is_err(), "{bad:?}");
        }
        assert!(hash(0.0, 85.05112878).is_some());
        assert!(hash(0.0, 85.06).is_none());
        assert!(hash(180.0, 0.0).is_some());
        assert!(hash(180.1, 0.0).is_none());
        assert_eq!(
            [b"m", &b"KM"[..], b"Ft", b"mi"].map(unit_metres),
            [Some(1.0), Some(1000.0), Some(0.3048), Some(1609.34)]
        );
        assert_eq!(unit_metres(b"yd"), None);
    }

    /// The cells a query reads hold every point within its radius: over
    /// random points and centres at many radii, the antimeridian included,
    /// no point within the radius lies outside the ranges. Not near the
    /// poles with a large radius: there RediSearch's bounding box prunes
    /// neighbours that hold points within it, a limitation its own source
    /// records, and the filter answers as RediSearch does by asking the
    /// ranges too.
    #[test]
    fn the_ranges_hold_every_point_within_the_radius() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed >> 11) as f64 / (1u64 << 53) as f64
        };
        let pts: Vec<(f64, f64, u64)> = (0..4000)
            .map(|_| {
                let (lon, lat) = (rnd() * 360.0 - 180.0, rnd() * 170.0 - 85.0);
                (lon, lat, hash(lon, lat).expect("hashable"))
            })
            .collect();
        let centres = [
            (0.0, 0.0),
            (179.9, 10.0),
            (-179.95, -20.0),
            (12.0, 60.0),
            (-70.0, -55.0),
        ];
        let (mut checked, mut inside) = (0usize, 0usize);
        for &(lon, lat) in centres
            .iter()
            .chain(&[(rnd() * 360.0 - 180.0, rnd() * 120.0 - 60.0)])
        {
            for radius in [1.0, 150.0, 5_000.0, 80_000.0, 600_000.0, 2_500_000.0] {
                let rs = ranges(lon, lat, radius);
                assert!(!rs.is_empty() && rs.len() <= 9);
                for &(_, _, h) in &pts {
                    if within(h, lon, lat, radius) {
                        inside += 1;
                        assert!(
                            rs.iter().any(|&(lo, hi)| (lo..hi).contains(&h)),
                            "({lon}, {lat}) r {radius}: {:?} missed",
                            centre(h)
                        );
                    }
                    checked += 1;
                }
            }
        }
        assert!(
            inside > 100,
            "only {inside} of {checked} inside: the test proves little"
        );
    }

    /// A centre nearer a pole than the square reaches matches nothing, as
    /// RediSearch answers it.
    #[test]
    fn a_centre_past_the_square_reads_nothing() {
        assert!(ranges(0.0, 86.0, 1_000_000.0).is_empty());
        assert!(!ranges(0.0, 85.0, 1_000_000.0).is_empty());
    }
}
