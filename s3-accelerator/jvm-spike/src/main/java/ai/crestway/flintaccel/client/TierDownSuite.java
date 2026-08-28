// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;
import java.net.http.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * BUG-0058: a tier that is ALREADY down must not fail the job.
 *
 * <p>ADR-0023 D12.9 states the property this defends: S3 is authoritative and
 * always reachable, so every tier interaction is an optimisation and any
 * failure falls through to the origin. Its protections were all on the read
 * path, which covers a tier that dies after a working connection exists.
 * Nothing covered a tier that was never reachable, because
 * {@code RedisClient.connect()} threw during construction and
 * {@code FlintS3AFileSystem.initialize} handed that exception to Hadoop.
 *
 * <p>Measured under Spark 4.0.4 with flint-server stopped, before the fix:
 * {@code Failed to initialize filesystem s3a://...: RedisConnectionException},
 * repeating per table, and not one query ran.
 *
 * <p><b>The order of the checks is the point.</b> "Reads still work" is the
 * weakest possible evidence on its own, because it also holds for a client
 * that gave up on the tier permanently, and for one that never tried. So the
 * suite also requires that the client TRIED (tierFailures moved), that it did
 * not try too hard (the reconnect is rate-limited, or a dead tier costs a TCP
 * handshake per read and the cache is worse than no cache), and that it
 * RECOVERS once the tier returns — which is what separates this from simply
 * disabling the tier on a failed connect.
 */
public final class TierDownSuite {

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
    p.put("flint.tier.reconnect.ms", "400");   // short, so recovery is testable
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

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String tier = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    Suite.redisUrl = tier;

    // The origin fixture serves any key; 64 KiB spans more than one chunk so
    // the read exercises the chunk path rather than a metadata-only lookup.
    final String KEY = "data/000001.bin";   // the fixture generates this
    final int LEN = 64 * 1024;

    Suite.killTier();
    check(!Suite.tierListening(), "precondition: the tier port refuses connections");

    TierSupport t = null;
    try {
      t = build(tier);
      check(true, "TierSupport.build SURVIVES a tier that is already down"
                  + " -- this is the check that fails before the fix");
    } catch (RuntimeException e) {
      check(false, "TierSupport.build threw with the tier down: " + e);
      System.exit(1);
    }

    int before = gets();
    byte[] got = read(t, KEY, LEN);
    check(got.length == LEN, "a read with no tier returns the object, " + got.length + " bytes");
    check(gets() > before, "and it came from the ORIGIN (origin GETs moved)");

    // Without this, "reads work" is equally true of a client that silently
    // stopped using the tier at build time and never intended to try again.
    check(t.client.tierFailures.get() > 0,
          "the client TRIED the tier and degraded: tierFailures="
          + t.client.tierFailures.get());

    long attemptsAfterFirst = t.lazy == null ? -1 : t.lazy.connectAttempts.get();
    check(t.lazy != null, "the lazy handle is installed when the tier was down at build");

    for (int i = 0; i < 6; i++) read(t, KEY, LEN);
    long attempts = t.lazy.connectAttempts.get();
    // A connect per read against a dead endpoint is a TCP handshake per read.
    check(attempts < attemptsAfterFirst + 6,
          "reconnects are RATE-LIMITED across 6 more reads: attempts went "
          + attemptsAfterFirst + " -> " + attempts);

    // Recovery is what separates a lazy connect from giving up. Without it the
    // fix would be indistinguishable from disabling the cache on first failure.
    Suite.startTier();
    check(Suite.tierAnswering(), "the tier is back and answering PING");
    Thread.sleep(600);                       // outlast flint.tier.reconnect.ms
    for (int i = 0; i < 3; i++) read(t, KEY, LEN);
    check(t.lazy.connection() != null,
          "the lazy handle CONNECTED once the tier returned");
    byte[] after = read(t, KEY, LEN);
    check(Arrays.equals(after, got), "and the bytes are still correct after recovery");

    t.close();

    // ---- the same property, entered through S3A PATH 1 (BUG-0067).
    //
    // Everything above builds through TierSupport, which is where BUG-0058's
    // fix landed -- so this suite followed the fix around and never tested the
    // path that was missed. Path 1 builds its client DIRECTLY, and its
    // bind-time behaviour was broken for exactly as long as this suite was
    // green.
    //
    // Bind-time on path 1 is covered by ConfigReachSuite's dead-tier probe.
    // This is the MID-JOB half, and it was untested anywhere on this path. A
    // spike named for the property looked like it covered it and did not: it
    // defined its own ObjectClient rather than using the product's, so it
    // proved a reimplementation degraded correctly. It has been deleted
    // (BUG-0067); the client suite covers the same property against the real
    // client, and this arm covers it through path 1.
    Configuration c = new Configuration();
    c.set("fs.s3a.endpoint", endpoint);
    c.set("fs.s3a.endpoint.region", "us-east-1");
    c.setBoolean("fs.s3a.path.style.access", true);
    c.set("fs.s3a.access.key", "s"); c.set("fs.s3a.secret.key", "s");
    c.setInt("fs.s3a.bucket.probe", 0);
    c.set("fs.s3a.change.detection.mode", "none");
    c.set("fs.s3a.input.stream.type", "custom");
    c.set("fs.s3a.input.stream.custom.factory",
        ai.crestway.flintaccel.s3a.FlintStreamFactory.class.getName());
    c.set(ai.crestway.flintaccel.s3a.FlintStreamFactory.TIER_URI, tier);
    c.set("fs.s3a.impl.disable.cache", "true");

    try (FileSystem fs = FileSystem.get(URI.create("s3a://bucket/"), c)) {
      byte[] b1 = new byte[LEN];
      try (FSDataInputStream in = fs.open(new Path("s3a://bucket/" + KEY))) {
        in.readFully(0, b1, 0, LEN);
      }
      check(b1.length == LEN,
          "path 1: a read with a HEALTHY tier returns the object");
      // Armed. Without this the survival check below is satisfied by a path
      // that was never using the tier in the first place.
      check(Suite.tierAnswering(),
          "  armed: the tier was up and answering for that read");

      int beforeKill = gets();
      Suite.killTier();
      check(!Suite.tierListening(),
          "precondition: the tier is DOWN, mid-job, with the filesystem open");

      byte[] b2 = new byte[LEN];
      try (FSDataInputStream in = fs.open(new Path("s3a://bucket/" + KEY))) {
        in.readFully(LEN, b2, 0, LEN);       // a DIFFERENT range: must be fetched
      }
      check(b2.length == LEN,
          "path 1: a read SURVIVES the tier dying mid-job, " + b2.length + " bytes");
      check(gets() > beforeKill,
          "  and it came from the ORIGIN (origin GETs moved), so it degraded "
              + "rather than being served from something stale");
    } catch (Exception e) {
      check(false, "path 1: a read SURVIVES the tier dying mid-job -- threw " + e);
    }

    Suite.startTier();            // leave the box as this suite found it
    check(Suite.tierAnswering(), "the tier is back for the stages after this one");

    System.exit(ok ? 0 : 1);
  }
}
