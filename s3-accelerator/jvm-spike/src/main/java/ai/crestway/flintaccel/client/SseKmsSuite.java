// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;
import java.util.Arrays;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;

/**
 * SSE-KMS bypasses the cache unless the customer opts in (ADR-0023 D13.3).
 *
 * Unlike SSE-C, this is not a key-handling question -- S3 decrypts SSE-KMS
 * server-side and hands us plaintext lawfully. It is a question about two
 * properties the customer paid for and would silently lose:
 *
 *   the KMS grant stops being the access gate, because anyone who can read the
 *   tier reads the plaintext without holding kms:Decrypt; and
 *
 *   every cache hit is a decrypt that never reaches CloudTrail. For many
 *   customers that audit trail IS the compliance requirement, and no
 *   mitigation restores it -- it is a property of not calling KMS at all.
 *
 * Default off. Opt-in after the customer's own review. Never a surprise.
 *
 * The checks below are paired throughout: every "it did not cache" is
 * accompanied by proof that the same client DOES cache a non-KMS object, since
 * a client that cached nothing at all would pass every bypass assertion.
 */
public final class SseKmsSuite {

  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static S3AsyncClient sdk(String endpoint) {
    return S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("k", "k")))
        .forcePathStyle(true).build();
  }

  public static void main(String[] args) throws Exception {
    // arg0 = a KMS-reporting origin, arg1 = a plain one, arg2 = tier
    String kmsEp   = args.length > 0 ? args[0] : "http://127.0.0.1:9530";
    String plainEp = args.length > 1 ? args[1] : "http://127.0.0.1:9531";
    String tierUri = args.length > 2 ? args[2] : "redis://127.0.0.1:9399";
    Suite.endpoint = kmsEp;

    RedisClient rc = RedisClient.create(tierUri);
    StatefulRedisConnection<byte[], byte[]> conn = rc.connect(new ByteArrayCodec());
    final String KEY = "data/000001.bin";
    final int LEN = 200_000;

    S3AsyncClient kmsS3 = sdk(kmsEp), plainS3 = sdk(plainEp);

    // ------------------------------------------------------------------ 1
    // Default: KMS object, cacheKms=false. Nothing may reach the tier.
    conn.sync().flushall();
    try (var sdkc = new S3SdkObjectClient(kmsS3, false)) {
      var c = new FlintObjectClient(sdkc, conn.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2,
          false, kmsS3, false);
      byte[] got = Suite.read(c, KEY, 0, LEN);
      check(Suite.genOf(KEY, 0, got) == 0, "KMS object still reads CORRECTLY (bypass is not a failure)");
      long keys = conn.sync().keys("*".getBytes()).size();
      check(keys == 0, "and NOTHING reached the tier -- no chunks, no metadata (" + keys + " keys)");
      check(c.kmsBypassed.get() > 0,
          "armed: it was the KMS path that bypassed (" + c.kmsBypassed.get() + "), not some other bypass");
      check(c.kmsUndetectable.get() == 0,
          "armed: detection actually worked -- 0 undetectable objects");
    }

    // ------------------------------------------------------------------ 2
    // The control that carries check 1. A client that cached nothing at all
    // would pass every assertion above, so the SAME configuration must cache a
    // non-KMS object from a non-KMS origin.
    conn.sync().flushall();
    try (var sdkc = new S3SdkObjectClient(plainS3, false)) {
      var c = new FlintObjectClient(sdkc, conn.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2,
          false, plainS3, false);
      byte[] got = Suite.read(c, KEY, 0, LEN);
      check(Suite.genOf(KEY, 0, got) == 0, "control: a PLAIN object reads correctly");
      long keys = conn.sync().keys("c2/*".getBytes()).size();
      check(keys > 0,
          "control: and the same client DOES cache it (" + keys + " chunks) -- so check 1 "
          + "measured the KMS rule, not a broken cache");
      check(c.kmsBypassed.get() == 0, "control: and nothing was KMS-bypassed");
    }

    // ------------------------------------------------------------------ 3
    // Opt-in. The customer has decided; the same KMS object must now cache.
    conn.sync().flushall();
    try (var sdkc = new S3SdkObjectClient(kmsS3, false)) {
      var c = new FlintObjectClient(sdkc, conn.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2,
          false, kmsS3, true);
      byte[] got = Suite.read(c, KEY, 0, LEN);
      check(Suite.genOf(KEY, 0, got) == 0, "opt-in: the KMS object reads correctly");
      long keys = conn.sync().keys("c2/*".getBytes()).size();
      check(keys > 0, "OPT-IN WORKS: flint.cache.sse-kms=true caches the KMS object ("
          + keys + " chunks)");
      check(c.kmsBypassed.get() == 0, "armed: and nothing was bypassed once opted in");

      // and it is genuinely the cache, not a coincidence
      int before = Suite.gets();
      byte[] warm = Suite.read(c, KEY, 0, LEN);
      check(Arrays.equals(got, warm) && Suite.gets() == before,
          "opt-in: the second read is served entirely from the tier (0 origin GETs)");
    }

    // ------------------------------------------------------------------ 4
    // Detection unavailable must not look like detection succeeding. Built
    // without an SDK client, the client cannot check, and says so.
    conn.sync().flushall();
    try (var sdkc = new S3SdkObjectClient(kmsS3, false)) {
      var c = new FlintObjectClient(sdkc, conn.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2);
      Suite.read(c, KEY, 0, LEN);
      check(c.kmsUndetectable.get() > 0,
          "no SDK client: the client reports it COULD NOT check (" + c.kmsUndetectable.get()
          + ") rather than reporting all-clear");
    }

    conn.close(); rc.shutdown(); kmsS3.close(); plainS3.close();
    System.out.println(ok ? "SSE-KMS SUITE PASSED" : "SSE-KMS SUITE FAILED");
    System.exit(ok ? 0 : 1);
  }
}
