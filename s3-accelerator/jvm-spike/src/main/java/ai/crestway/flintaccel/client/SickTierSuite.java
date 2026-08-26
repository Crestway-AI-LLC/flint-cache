// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;

/**
 * A SICK tier, which is the failure mode a bounded client does not survive.
 *
 * A dead tier is cheap and already covered: the connection refuses inline and
 * the read falls through to S3 at once. The dangerous state is a tier that
 * still answers, slowly. Every request then burns the full tier budget, fails,
 * and goes to the origin anyway -- so the cache ADDS its budget to every read
 * rather than removing anything, and the fleet is slower than it would be with
 * no cache installed at all.
 *
 * That is not a hypothetical: it is what a saturated, GC-thrashing or
 * cross-AZ-failed-over tier looks like from the client, and it is the state a
 * cache most needs to detect, because the operator's instinct -- "the cache is
 * up, so that is not the problem" -- is exactly wrong.
 *
 * The reference this measures against is NOT the healthy tier. It is the
 * client with no tier at all: a cache is allowed to be slower than a fast
 * cache, and is not allowed to be slower than no cache.
 */
public final class SickTierSuite {

  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static void note(String s) { System.out.println("       " + s); }

  static StatefulRedisConnection<byte[], byte[]> connect(String uri) {
    RedisClient rc = RedisClient.create(uri);
    rc.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .cancelCommandsOnReconnectFailure(true).build());
    return rc.connect(new ByteArrayCodec());
  }

  /** Median of N reads, so one GC pause does not decide the verdict. */
  static double medianMs(FlintObjectClient c, String key, int rounds) throws Exception {
    double[] t = new double[rounds];
    for (int i = 0; i < rounds; i++) {
      long t0 = System.nanoTime();
      Suite.read(c, key, (i % 4) * 65_536L, 65_536);
      t[i] = (System.nanoTime() - t0) / 1e6;
    }
    java.util.Arrays.sort(t);
    return t[rounds / 2];
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String healthy  = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    String sick     = args.length > 2 ? args[2] : "redis://127.0.0.1:9398";
    Suite.endpoint = endpoint;
    final String KEY = "data/000001.bin";
    final int ROUNDS = 9;

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("s", "s")))
        .forcePathStyle(true).build();

    var hc = connect(healthy);
    var sc = connect(sick);

    try (var o1 = new S3SdkObjectClient(s3, false);
         var o2 = new S3SdkObjectClient(s3, false);
         var o3 = new S3SdkObjectClient(s3, false)) {

      // Reference 1: no tier at all. This is what the customer has today.
      var none = new FlintObjectClient(o1, hc.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2, true);
      double noTier = medianMs(none, KEY, ROUNDS);

      // Reference 2: a healthy tier, warm. This is what we are selling.
      hc.sync().flushall();
      var good = new FlintObjectClient(o2, hc.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2);
      medianMs(good, KEY, 3);                       // warm it
      double warm = medianMs(good, KEY, ROUNDS);
      check(warm < noTier,
          String.format("control: a HEALTHY tier is faster than no tier (%.0f vs %.0f ms)",
              warm, noTier));

      // The subject: a tier that answers, slowly.
      var bad = new FlintObjectClient(o3, sc.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2);
      long failBefore = bad.tierFailures.get();
      double sickMs = medianMs(bad, KEY, ROUNDS);
      check(bad.tierFailures.get() > failBefore,
          "armed: the sick tier really did fail/time out ("
          + (bad.tierFailures.get() - failBefore) + " times)");

      note(String.format("no tier %.0f ms | healthy %.0f ms | SICK %.0f ms",
          noTier, warm, sickMs));

      // The claim. A cache may be slower than a fast cache. It may not be
      // slower than no cache -- that turns an outage in the tier into an
      // outage in the application, which is the opposite of a look-aside
      // design's whole promise.
      check(sickMs <= noTier * 1.25,
          String.format("A SICK TIER IS NOT SLOWER THAN NO TIER AT ALL "
              + "(%.0f ms vs %.0f ms, +%.0f%%)",
              sickMs, noTier, 100 * (sickMs / noTier - 1)));
    }

    hc.close(); sc.close(); s3.close();
    System.out.println(ok ? "SICK-TIER SUITE PASSED" : "SICK-TIER SUITE FAILED");
    System.exit(ok ? 0 : 1);
  }
}
