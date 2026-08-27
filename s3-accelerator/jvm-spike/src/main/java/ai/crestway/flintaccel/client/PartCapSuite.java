// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.util.*;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * ADR-0023 D17.5: the cap that decides the payoff is on the PART, not the object.
 *
 * <p>A read whose parts are small enough for a saved round trip to matter is
 * cached; one that pulls so much per request that 30 ms is a rounding error is
 * read through and cached NOT AT ALL — writing its chunks anyway would be the
 * cost without the benefit, which is the trade the gate exists to refuse.
 *
 * <p><b>The controls are the suite.</b> "A big request was not cached" is
 * satisfied by a cache that is simply broken, so the same request is issued
 * twice with nothing different but the cap: once above it and once below. If
 * both outcomes are identical the gate is not the thing deciding, and the suite
 * says so rather than reporting a pass.
 *
 * <p>The request size is driven through {@code flint.aal.request.bytes} rather
 * than by picking an object large enough to force one, so the fixture stays
 * small and the variable under test is set directly instead of inferred.
 */
public final class PartCapSuite {

  static String endpoint;
  static boolean ok = true;
  static final long MiB = 1024 * 1024;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static TierSupport build(String tier, long partCap, long aalReq) {
    Map<String, String> p = new HashMap<>();
    p.put("flint.tier.uri", tier);
    p.put("s3.endpoint", endpoint);
    p.put("s3.path-style-access", "true");
    p.put("s3.access-key-id", "i");
    p.put("s3.secret-access-key", "i");
    p.put("s3.region", "us-east-1");
    p.put("client.region", "us-east-1");
    p.put("flint.max.part.bytes", String.valueOf(partCap));
    p.put("flint.aal.request.bytes", String.valueOf(aalReq));
    return TierSupport.build(TierSupport.from(p));
  }

  /** Fresh AAL factory per read, or AAL's own cache answers instead (D12.5). */
  static byte[] read(TierSupport t, String key, int len) throws Exception {
    try (var f = new S3SeekableInputStreamFactory(t.client,
             S3SeekableInputStreamConfiguration.DEFAULT);
         var in = f.createStream(S3URI.of("bucket", key))) {
      byte[] out = new byte[len];
      int n = 0;
      while (n < len) {
        int r = in.read(out, n, len - n);
        if (r < 0) break;
        n += r;
      }
      return Arrays.copyOf(out, n);
    }
  }

  static int chunkKeys(TierSupport t) {
    // CHUNK keys only. Metadata is still written for a bypassed read by design
    // (D17: the entry is ~50 bytes and saves a HEAD), so counting dbsize would
    // show growth for a read that cached no data and the check would fail while
    // the product was correct.
    return ai.crestway.flintaccel.TierScan.keys(t.liveConnection(), "c2/*").size();
  }

  /** One ranged read through the client, fully drained. */
  static byte[] drain(TierSupport t, S3URI uri, String etag, long lo, long hi)
      throws Exception {
    ObjectContent oc = t.client.getObject(GetRequest.builder()
        .s3Uri(uri).etag(etag).referrer(new Referrer("bytes=" + lo + "-" + hi,
            ReadMode.SYNC))
        .range(new Range(lo, hi)).build()).join();
    try (var in = oc.getStream()) { return in.readAllBytes(); }
  }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String tier = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    final String KEY = "data/000002.bin";
    final int LEN = 4 * (int) MiB;

    // ---- arm A: parts of 2 MiB against a 1 MiB cap -> must not be cached
    TierSupport a = build(tier, 1 * MiB, 2 * MiB);
    a.liveConnection().sync().flushall();
    int before = chunkKeys(a);
    byte[] gotA = read(a, KEY, LEN);
    int afterA = chunkKeys(a);
    long bypassedA = a.client.oversizePartBypassed.get();

    check(gotA.length == LEN, "the read still returns the object, " + gotA.length + " bytes");
    check(afterA == before,
          "and NOTHING was cached: chunk keys " + before + " -> " + afterA);
    check(bypassedA > 0,
          "the gate is what refused it: oversizePartBypassed=" + bypassedA);
    a.close();

    // ---- arm B: the SAME 2 MiB parts against a 4 MiB cap -> must be cached
    TierSupport b = build(tier, 4 * MiB, 2 * MiB);
    b.liveConnection().sync().flushall();
    int beforeB = chunkKeys(b);
    byte[] gotB = read(b, KEY, LEN);
    int afterB = chunkKeys(b);
    long bypassedB = b.client.oversizePartBypassed.get();

    check(afterB > beforeB,
          "POSITIVE CONTROL: the same request under a larger cap IS cached, "
          + beforeB + " -> " + afterB + " chunk keys");
    check(bypassedB == 0,
          "and nothing was refused on that arm: oversizePartBypassed=" + bypassedB);
    check(Arrays.equals(gotA, gotB),
          "both arms return identical bytes, so the gate changes what is STORED "
          + "and not what is returned");
    b.close();

    // Without this the suite would pass for a cache that never stores anything.
    check(afterA != afterB,
          "the cap is the only difference between the two arms, and it decided "
          + "the outcome");

    // ---- arm C: the cap is enforced where parts ENTER the tier, not only
    // where requests are admitted.
    //
    // A fill run is grid-ALIGNED, so a request of exactly the cap that starts
    // mid-chunk becomes a run LARGER than the cap. The request gate admits it
    // -- correctly, it is not over the cap -- and only a check at the write
    // boundary stops the oversize part. Driven through the client rather than
    // through AAL because AAL issues block-aligned requests and so cannot
    // produce this shape; the boundary being tested is the client's, and this
    // is where it lives.
    TierSupport c = build(tier, 1 * MiB, 2 * MiB);
    c.liveConnection().sync().flushall();
    S3URI uri = S3URI.of("bucket", KEY);
    String etag = c.client.headObject(HeadRequest.builder().s3Uri(uri).build())
        .join().getEtag();

    // 64 KiB is the chunk grid (D4). Half a chunk in, a request of exactly the
    // cap spans one chunk more than the cap holds.
    final long HALF = 32 * 1024;
    byte[] unaligned = drain(c, uri, etag, HALF, HALF + 1 * MiB - 1);
    int afterC = chunkKeys(c);
    long missesC = c.client.chunkMisses.get();
    check(unaligned.length == (int) MiB,
          "a part over the cap still READS correctly, " + unaligned.length + " bytes");
    check(afterC == 0, "and NO part over the cap entered the tier: "
          + afterC + " chunk keys");
    check(c.client.oversizePartBypassed.get() > 0,
          "counted, not silent: oversizePartBypassed="
          + c.client.oversizePartBypassed.get());
    // Armed. The request is exactly ON the cap, so the request gate admitted it
    // and the chunks were looked up; a miss exists only because the fill path
    // ran. Without this the checks above pass on the request gate and say
    // nothing about writes.
    check(missesC > 0, "armed: the request was ADMITTED and the fill path ran ("
          + missesC + " chunk misses), so it is the WRITE guard that refused");

    // The control is the alignment, not the size: same cap, same byte count,
    // same object, different offset.
    c.liveConnection().sync().flushall();
    byte[] aligned = drain(c, uri, etag, 0, 1 * MiB - 1);
    int afterD = chunkKeys(c);
    // Oracle without a second client: the two ranges OVERLAP by 1 MiB - 32 KiB,
    // one served past the cap and one from the tier, so disagreeing bytes mean
    // the guard changed what is RETURNED and not only what is stored.
    check(Arrays.equals(Arrays.copyOfRange(aligned, (int) HALF, aligned.length),
                        Arrays.copyOf(unaligned, aligned.length - (int) HALF)),
          "the two arms agree on the bytes they share, so the guard changed "
          + "what is STORED and not what is returned");
    check(afterD > 0, "POSITIVE CONTROL: the same " + MiB
          + " B, grid-ALIGNED, IS cached, " + afterD + " chunk keys");
    c.close();

    System.exit(ok ? 0 : 1);
  }
}
