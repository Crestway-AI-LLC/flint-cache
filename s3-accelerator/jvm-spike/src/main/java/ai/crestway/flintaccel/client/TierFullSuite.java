// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;
import java.net.http.*;
import java.util.*;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * A tier that is FULL is not a tier that is BROKEN.
 *
 * <p>A never-evict namespace is the DEFAULT configuration, and when it fills,
 * Flint sheds writes with {@code -QUOTA storage quota exceeded; writes rejected
 * until usage drops (reads still served)} and goes on answering reads. Every
 * other tier this harness builds is either healthy or dead — and those are
 * exactly the two states that cannot tell a full tier from a broken one. So
 * the state that the default configuration ends up in had never been exercised.
 *
 * <p><b>Two things are being defended, and the second is the load-bearing
 * one.</b> The first is reporting: folding {@code -QUOTA} into
 * {@code tierFailures} sends an operator hunting a fault in a tier that is
 * doing exactly what they configured it to do, and hides the one signal that
 * would tell them to add capacity or enable eviction. The second is behaviour:
 * a refusal must not open the circuit breaker. The breaker exists because a
 * SICK tier is worse than no tier, but a full tier is not sick — it is still
 * serving every read — and opening the breaker on it would throw away a
 * working read cache at exactly the moment the cache is most valuable.
 *
 * <p>The dead-tier control is not optional here. "tierFull moved and
 * tierFailures did not" is also true of a client that reports everything as
 * full, so the same reads are run against a tier that is genuinely absent and
 * required to produce the opposite pair.
 */
public final class TierFullSuite {

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

  static TierSupport build(String tier) {
    Map<String, String> p = new HashMap<>();
    p.put("flint.tier.uri", tier);
    p.put("s3.endpoint", endpoint);
    p.put("s3.path-style-access", "true");
    p.put("s3.access-key-id", "i");
    p.put("s3.secret-access-key", "i");
    p.put("s3.region", "us-east-1");
    p.put("client.region", "us-east-1");
    return TierSupport.build(TierSupport.from(p));
  }

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

  /**
   * Wait for the asynchronous fill to have ATTEMPTED its write.
   *
   * <p>The fill is off the read path on purpose (D17.5.1), so "the tier was
   * written to" is true eventually rather than on return. Bounded, and the
   * caller asserts afterwards: a poll that times out leaves the counter at
   * zero and fails the check, rather than passing quietly the way a sleep
   * would on a fast machine and failing on a loaded one.
   */
  static void settle(TierSupport t, long budgetMs) throws Exception {
    long end = System.nanoTime() + budgetMs * 1_000_000L;
    while (System.nanoTime() < end) {
      if (t.client.tierFull.get() > 0 || t.client.tierFailures.get() > 0) return;
      Thread.sleep(20);
    }
  }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    // args[1] is the gate's HEALTHY tier, and this suite deliberately ignores
    // it: its subject is a tier in a state the harness tier is never in. Both
    // ports are declared in tools/port_exclusivity_check.sh -- 9316 is bound by
    // the quota fixture, 9498 is declared precisely so that nothing binds it.
    final String full = "redis://127.0.0.1:9316";
    final String dead = "redis://127.0.0.1:9498";

    final String KEY = "data/000001.bin";
    final int LEN = 64 * 1024;

    // ---- arm: the tier is full -------------------------------------------
    TierSupport tf = build(full);
    check(true, "TierSupport.build survives a tier that refuses every write");

    int before = gets();
    byte[] got = read(tf, KEY, LEN);
    settle(tf, 5000);
    check(got.length == LEN,
          "a read against a FULL tier returns the object (" + got.length + " bytes)");
    check(gets() > before, "and the bytes came from the ORIGIN (origin GETs moved)");

    long tFull = tf.client.tierFull.get();
    long tFail = tf.client.tierFailures.get();
    check(tFull > 0, "the refusal is COUNTED as full (tierFull=" + tFull + ")");
    check(tFail == 0,
          "and NOT as breakage -- a full tier must not read as a broken one "
          + "(tierFailures=" + tFail + ")");
    check(tf.client.breakerOpens.get() == 0,
          "the breaker stayed CLOSED (" + tf.client.breakerOpens.get() + " opens) -- "
          + "opening it would abandon a read cache that is still serving");
    check(!tf.client.isBreakerOpen(),
          "and is not open now, so the next read still consults the tier");

    // Reads keep being served by a full tier: that is the property the -QUOTA
    // message itself promises, and the reason the breaker must stay shut.
    for (int i = 0; i < 4; i++) read(tf, KEY, LEN);
    check(tf.client.tierFailures.get() == 0,
          "four more reads, still zero tierFailures (" + tf.client.tierFailures.get() + ")");
    tf.close();

    // ---- control: the tier is genuinely absent ---------------------------
    TierSupport td = build(dead);
    byte[] got2 = read(td, KEY, LEN);
    settle(td, 5000);
    check(got2.length == LEN, "control: a DEAD tier also returns the object");
    check(td.client.tierFailures.get() > 0,
          "control: a dead tier DOES register as broken (tierFailures="
          + td.client.tierFailures.get() + ")");
    check(td.client.tierFull.get() == 0,
          "control: and does NOT register as full (tierFull="
          + td.client.tierFull.get() + ") -- the counters discriminate");
    td.close();

    System.out.println(ok ? "TIER FULL SUITE PASSED" : "TIER FULL SUITE FAILED");
    System.exit(ok ? 0 : 1);
  }
}
