// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;
import java.net.http.*;
import java.security.MessageDigest;
import java.util.*;

import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * The tier may throw our data away at any moment. Does a read notice correctly?
 *
 * <p>ADR-0023 D18 moved capacity policy to the Flint Cache side: the tier owns
 * eviction and this client is not told about it. That makes eviction something
 * we <b>tolerate</b> rather than direct — and tolerance is a property, so it
 * needs a test. Until this suite, <b>nothing in either gate ever evicted
 * anything.</b> The correctness argument existed and was never executed.
 *
 * <h2>Why it should be safe, and why that is not enough</h2>
 *
 * Every path is meant to degrade: a missing chunk is a miss and refetches from
 * origin; a partially-evicted run makes {@code assemble} return null, which the
 * caller turns into a passthrough (D12.29 — a gap must never become a short
 * read); an evicted-then-refilled chunk is byte-identical because keys are
 * content-addressed by ETag, so the D14 seal still verifies; and a follower
 * whose chunks vanished between the leader's write and its own {@code mget}
 * simply fetches them itself.
 *
 * <p>All of that is reasoning. This suite evicts for real, under a live reader,
 * and checks the bytes.
 *
 * <h2>The controls</h2>
 *
 * <ul>
 *   <li><b>Armed:</b> eviction must actually have happened — chunk keys must
 *       FALL. A run where the tier never evicted would pass every correctness
 *       check below while proving nothing, which is this file's whole risk.
 *   <li><b>The origin must be reached again.</b> Correct bytes alone are also
 *       true of a client that ignored the tier entirely, so the origin GET
 *       counter has to move: that is what says the data came back from S3
 *       because it was gone, rather than never having been cached.
 * </ul>
 */
public final class EvictionSuite {

  static final HttpClient HTTP = HttpClient.newHttpClient();
  static String endpoint;
  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static int gets() throws Exception {
    String b = HTTP.send(HttpRequest.newBuilder(URI.create(endpoint + "/__stats")).build(),
        HttpResponse.BodyHandlers.ofString()).body();
    return ai.crestway.flintaccel.OriginStats.parse(b, "gets");
  }

  static byte[] expect(String key, long off, int len) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      long abs = off + i;
      md.reset();
      out[i] = md.digest((key + ":0:" + (abs / 16)).getBytes("UTF-8"))[(int) (abs % 16)];
    }
    return out;
  }

  static TierSupport build(String tier) {
    Map<String, String> p = new HashMap<>();
    p.put("flint.tier.uri", tier);
    p.put("s3.endpoint", endpoint);
    p.put("s3.path-style-access", "true");
    p.put("s3.access-key-id", "e");
    p.put("s3.secret-access-key", "e");
    p.put("s3.region", "us-east-1");
    p.put("client.region", "us-east-1");
    return TierSupport.build(TierSupport.from(p));
  }

  static byte[] read(TierSupport t, S3URI uri, String etag, long off, int len)
      throws Exception {
    ObjectContent oc = t.client.getObject(GetRequest.builder()
        .s3Uri(uri).etag(etag)
        .referrer(new Referrer("bytes=" + off + "-" + (off + len - 1), ReadMode.SYNC))
        .range(new Range(off, off + len - 1)).build()).join();
    try (var in = oc.getStream()) { return in.readAllBytes(); }
  }

  static int chunkKeys(TierSupport t) {
    return ai.crestway.flintaccel.TierScan.keys(t.liveConnection(), "c2/*").size();
  }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String tier = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    final String KEY = "data/000001.bin";
    final int READ = 4096;

    TierSupport t = build(tier);
    S3URI uri = S3URI.of("bucket", KEY);
    String etag = t.client.headObject(HeadRequest.builder().s3Uri(uri).build())
        .join().getEtag();
    var sync = t.liveConnection().sync();
    sync.flushall();

    // Fill a good span so there is something to lose.
    final long SPAN = 4L * 1024 * 1024;
    for (long off = 0; off < SPAN; off += 65536) read(t, uri, etag, off, READ);
    int filled = chunkKeys(t);
    check(filled > 0, "armed: the span is cached (" + filled + " chunk keys)");

    // ---- 1. evict EVERYTHING under a live client, then read again.
    //
    // flushall rather than an LRU policy: the question is whether a read
    // survives its chunks disappearing, and "all of them, now" is the strongest
    // form of that. The LRU case is weaker and is covered by check 2.
    int before = gets();
    sync.flushall();
    check(chunkKeys(t) == 0, "armed: the tier really is empty now (0 chunk keys)");
    byte[] got = read(t, uri, etag, 1_000_000, READ);
    check(Arrays.equals(got, expect(KEY, 1_000_000, READ)),
        "a read after TOTAL eviction returns correct bytes");
    check(gets() > before,
        "  and it went back to the ORIGIN (" + before + " -> " + gets() + " GETs), so the "
            + "bytes came from S3 because they were gone -- not from a client "
            + "that was ignoring the tier");

    // ---- 2. evict HALF a run, mid-object, and read across the hole.
    //
    // The interesting case is not an empty tier, it is a PARTIAL one: assemble
    // must refuse to stitch a gap into a short read (D12.29), because a short
    // read at the wrong moment is indistinguishable from end-of-file.
    sync.flushall();
    for (long off = 0; off < SPAN; off += 65536) read(t, uri, etag, off, READ);
    List<byte[]> keys = ai.crestway.flintaccel.TierScan.keys(t.liveConnection(), "c2/*");
    int half = keys.size() / 2, dropped = 0;
    for (int i = 0; i < half; i++) { sync.del(keys.get(i)); dropped++; }
    check(dropped > 0 && chunkKeys(t) < keys.size(),
        "armed: " + dropped + " of " + keys.size() + " chunks evicted, "
            + chunkKeys(t) + " left");
    int before2 = gets();
    long hits0 = t.client.chunkHits.get(), miss0 = t.client.chunkMisses.get();
    // A read spanning many chunks, so it straddles surviving and evicted ones.
    byte[] wide = read(t, uri, etag, 0, 512 * 1024);
    long hits = t.client.chunkHits.get() - hits0, miss = t.client.chunkMisses.get() - miss0;
    check(wide.length == 512 * 1024,
        "a 512 KiB read across a HALF-EVICTED object returns the full length ("
            + wide.length + ") -- a gap must never become a short read");
    check(Arrays.equals(wide, expect(KEY, 0, 512 * 1024)),
        "  and every byte of it is correct");
    check(gets() > before2, "  and the origin was consulted for what was missing");
    // THE ARM FOR THIS CASE. "Half evicted" is a statement about the tier, not
    // about the read: if the deleted half happened to miss this range entirely
    // the read is an all-hit read, and if it took all of it the read is an
    // all-miss read. Either degenerates into a case already covered above and
    // the interesting one -- stitching survivors together with refetched holes
    // -- would silently not be tested. Both counters must move.
    check(hits > 0 && miss > 0,
        "armed: the read genuinely STRADDLED the hole (" + hits + " chunks hit, "
            + miss + " refetched), so this is the mixed case and not an "
            + "all-present or all-absent read wearing its name");

    // ---- 3. refill after eviction must produce IDENTICAL bytes.
    //
    // Keys are content-addressed by ETag, so a refilled chunk is the same chunk.
    // If eviction could change what a key means, the D14 seal would be the only
    // thing standing between that and silently wrong data.
    int nowKeys = chunkKeys(t);
    byte[] again = read(t, uri, etag, 0, 512 * 1024);
    check(Arrays.equals(again, wide),
        "a re-read after the refill is byte-identical (" + nowKeys + " -> "
            + chunkKeys(t) + " chunk keys)");
    check(t.client.integrityFailures.get() == 0,
        "and the seal never rejected a refilled chunk (integrityFailures="
            + t.client.integrityFailures.get() + ")");

    t.close();
    System.out.println("\nEVICTION SUITE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
