// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;
import java.net.http.*;
import java.util.*;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Declaring an object immutable should stop the revalidation HEADs.
 *
 * D3 keeps metadata under a short TTL because an object at a path can be
 * replaced, and a stale length is worse than a stale value -- D12.29 showed it
 * makes reads hit EOF early, which presents as truncation rather than
 * staleness. For a format whose files are never rewritten, that revalidation
 * buys protection against something the format forbids, at a HEAD per object
 * per TTL.
 *
 * The measurement is origin HEAD count across a read taken AFTER the mutable
 * TTL has expired. Anything else -- correct bytes, a hit counter -- would look
 * identical whether or not the declaration did anything.
 */
public final class ImmutableSuite {

  static final HttpClient HTTP = HttpClient.newHttpClient();
  static String endpoint;
  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static int heads() throws Exception {
    String b = HTTP.send(HttpRequest.newBuilder(URI.create(endpoint + "/__stats")).build(),
        HttpResponse.BodyHandlers.ofString()).body();
    return ai.crestway.flintaccel.OriginStats.parse(b, "heads");
  }

  static TierSupport build(String tier, boolean immutable) {
    Map<String, String> p = new HashMap<>();
    p.put("flint.tier.uri", tier);
    p.put("s3.endpoint", endpoint);
    p.put("s3.path-style-access", "true");
    p.put("s3.access-key-id", "i");
    p.put("s3.secret-access-key", "i");
    p.put("s3.region", "us-east-1");
    p.put("client.region", "us-east-1");
    p.put("flint.meta.ttl.seconds", String.valueOf(MUTABLE_TTL_SEC));  // mutable: expires fast
    p.put("flint.meta.ttl.immutable.seconds", "3600");
    if (immutable) p.put("flint.immutable", "true");
    return TierSupport.build(TierSupport.from(p));
  }

  /** Fresh AAL factory per read, or AAL's own cache answers instead (D12.5). */
  static byte[] read(TierSupport t, String key, int len) throws Exception {
    try (var f = new S3SeekableInputStreamFactory(t.client,
             S3SeekableInputStreamConfiguration.DEFAULT);
         var in = f.createStream(S3URI.of("bucket", key))) {
      byte[] b = new byte[len];
      in.seek(0);
      in.read(b, 0, len);
      return b;
    }
  }

  static final long MUTABLE_TTL_SEC = 1;

  /** HEADs charged to the origin by one read taken after the mutable TTL. */
  static int headsAcrossExpiredRead(TierSupport t, String key) throws Exception {
    read(t, key, 100_000);          // populate metadata
    // A SLEEP IS CORRECT HERE, and it is the only one left in these suites.
    // What both arms wait for is real time passing the MUTABLE ttl -- and the
    // immutable arm's entry is supposed to survive it, so there is no event to
    // wait for. Polling for the key's absence turned that non-event into a
    // hard failure: the immutable entry carries a 3600s ttl and correctly
    // never went away. You cannot signal a thing that is defined by not
    // happening. What is fixed is the magic number: the wait now derives from
    // the configured ttl instead of a 2,000 that described neither arm.
    Thread.sleep(MUTABLE_TTL_SEC * 1000L + 500);
    int before = heads();
    read(t, key, 100_000);
    return heads() - before;
  }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String tier = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    final String KEY = "data/000001.bin";

    // -- the control first. Without it, "0 HEADs" from the immutable client
    //    could mean the declaration worked, or that nothing ever revalidates.
    TierSupport mut = build(tier, false);
    mut.conn.sync().flushall();
    int mutableHeads = headsAcrossExpiredRead(mut, KEY);
    check(mutableHeads > 0,
        "control: a MUTABLE object revalidates after its TTL (" + mutableHeads + " HEADs)");

    // -- the claim
    TierSupport imm = build(tier, true);
    imm.conn.sync().flushall();
    int immutableHeads = headsAcrossExpiredRead(imm, KEY);
    check(immutableHeads < mutableHeads,
        "AN IMMUTABLE DECLARATION SKIPS THE REVALIDATION (" + immutableHeads
        + " vs " + mutableHeads + " HEADs past the same TTL)");

    // -- and it is still correct, which a saving alone does not show
    byte[] got = read(imm, KEY, 100_000);
    byte[] want = new byte[100_000];
    java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
    for (int i = 0; i < want.length; i++) {
      md.reset();
      want[i] = md.digest((KEY + ":0:" + (i / 16)).getBytes("UTF-8"))[i % 16];
    }
    check(Arrays.equals(got, want), "and the bytes are still correct");

    mut.close();
    imm.close();
    System.out.println(ok ? "IMMUTABLE SUITE PASSED" : "IMMUTABLE SUITE FAILED");
    System.exit(ok ? 0 : 1);
  }
}
