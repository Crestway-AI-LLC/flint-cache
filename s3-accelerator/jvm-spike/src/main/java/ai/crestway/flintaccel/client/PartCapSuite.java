// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.util.*;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
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
    System.exit(ok ? 0 : 1);
  }
}
