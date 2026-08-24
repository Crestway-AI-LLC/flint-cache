// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;
import java.util.*;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;

/**
 * What happens when the TIER lies.
 *
 * Every other suite we have assumes the cache returns what was put into it.
 * That is the assumption a cache cannot make about itself: ADR-0023 D1 promises
 * "the same bytes, sooner", and the only failure that breaks the promise
 * outright is returning bytes that are WRONG rather than merely late.
 *
 * So this suite corrupts the tier on purpose and asks what the client does.
 * Four ways a cached chunk can lie, in increasing order of nastiness:
 *
 *   ABSENT     the chunk is gone            -> a hole
 *   SHORT      the chunk is truncated       -> a hole, disguised as data
 *   CORRUPT    right length, wrong bytes    -> undetectable by length checks
 *   MISPLACED  a real chunk, wrong offset   -> valid data in the wrong place
 *
 * The last two are the dangerous ones, because every structural check the
 * assemble path can make -- count, length, contiguity -- passes. They are also
 * the ones a distributed tier actually produces: a key collision, a namespace
 * bug, or a 16-bit TCP checksum that missed. Nothing here needs an attacker.
 *
 * Each check is ARMED: it asserts the fault was really injected before it
 * asserts the client survived it, because "no corruption detected" and "no
 * corruption present" print the same way.
 */
public final class IntegritySuite {

  static boolean ok = true;
  static final String BUCKET = "bucket";
  static final int CHUNK = 64 * 1024;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static void note(String s) { System.out.println("       " + s); }

  /** Chunk ids covering [off, off+len). */
  static List<Long> ids(long off, int len) {
    List<Long> l = new ArrayList<>();
    for (long i = off / CHUNK; i <= (off + len - 1) / CHUNK; i++) l.add(i);
    return l;
  }

  static List<byte[]> chunkKeys(RedisCommands<byte[], byte[]> t) {
    List<byte[]> out = new ArrayList<>();
    for (byte[] k : t.keys("c1/*".getBytes())) out.add(k);
    out.sort(Comparator.comparingLong(a -> {
      String s = new String(a);
      return Long.parseLong(s.substring(s.lastIndexOf('/') + 1));
    }));
    return out;
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String redisUrl = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    Suite.endpoint = endpoint;

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("suite", "suite")))
        .forcePathStyle(true).build();

    RedisClient rc = RedisClient.create(redisUrl);
    rc.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .cancelCommandsOnReconnectFailure(true).build());
    StatefulRedisConnection<byte[], byte[]> conn = rc.connect(new ByteArrayCodec());
    RedisCommands<byte[], byte[]> tier = conn.sync();

    final String KEY = "data/000002.bin";
    final long OFF = 0;
    final int LEN = 200_000;            // spans chunks 0..3

    try (var sdk = new S3SdkObjectClient(s3, false)) {
      var c = new FlintObjectClient(sdk, conn.async(), CHUNK, 50, 2, false, s3, false);

      // ---------------------------------------------------------------- 0
      // Positive control. Without this, every check below could pass because
      // the cache is simply never used, and the suite would be measuring
      // nothing at all.
      tier.flushall();
      byte[] cold = Suite.read(c, KEY, OFF, LEN);
      check(Suite.genOf(KEY, OFF, cold) == 0, "control: cold read is correct");
      int keysAfter = chunkKeys(tier).size();
      check(keysAfter >= ids(OFF, LEN).size(),
          "control: the tier was actually populated (" + keysAfter + " chunks)");

      int g0 = Suite.gets();
      byte[] warm = Suite.read(c, KEY, OFF, LEN);
      check(Arrays.equals(cold, warm) && Suite.gets() == g0,
          "control: the warm read is served entirely from the tier (0 origin GETs)");

      // ---------------------------------------------------------------- 1
      // ABSENT. D12.29 hardened assemble() to fall back on a hole rather than
      // return a short read. Nothing held that fix; this does.
      List<byte[]> keys = chunkKeys(tier);
      byte[] victim = keys.get(1);
      byte[] saved = tier.get(victim);
      check(saved != null && saved.length > 0, "armed: victim chunk exists before deletion");
      tier.del(victim);
      check(tier.get(victim) == null, "armed: victim chunk is gone");

      int g1 = Suite.gets();
      byte[] holed = Suite.read(c, KEY, OFF, LEN);
      check(Arrays.equals(cold, holed), "ABSENT chunk: bytes are still correct");
      check(Suite.gets() > g1, "armed: and the origin was consulted, so it really fell back");

      // ---------------------------------------------------------------- 2
      // SHORT. A truncated chunk is a hole wearing data's clothes: the key is
      // present, so a presence check passes.
      tier.flushall();
      Suite.read(c, KEY, OFF, LEN);
      keys = chunkKeys(tier);
      victim = keys.get(1);
      byte[] full = tier.get(victim);
      tier.set(victim, Arrays.copyOf(full, full.length / 2));
      check(tier.get(victim).length == full.length / 2, "armed: victim chunk is truncated");

      int g2 = Suite.gets();
      byte[] shortened = Suite.read(c, KEY, OFF, LEN);
      check(Arrays.equals(cold, shortened), "SHORT chunk: bytes are still correct");
      check(Suite.gets() > g2, "armed: and the origin was consulted");

      // ---------------------------------------------------------------- 3
      // CORRUPT. Right length, wrong bytes. Every structural check passes.
      tier.flushall();
      Suite.read(c, KEY, OFF, LEN);
      keys = chunkKeys(tier);
      victim = keys.get(1);
      full = tier.get(victim);
      byte[] garbage = new byte[full.length];
      Arrays.fill(garbage, (byte) 0x5A);
      tier.set(victim, garbage);
      check(Arrays.equals(tier.get(victim), garbage), "armed: victim chunk holds wrong bytes");

      long if0 = c.integrityFailures.get();
      int g3 = Suite.gets();
      byte[] corrupted = Suite.read(c, KEY, OFF, LEN);
      boolean corruptSurvived = Arrays.equals(cold, corrupted);
      check(corruptSurvived, "CORRUPT chunk: bytes are still correct");
      check(c.integrityFailures.get() > if0,
          "armed: the seal REJECTED it (not merely a coincidental miss)");
      check(Suite.gets() > g3, "armed: and the origin was consulted");
      if (!corruptSurvived) {
        int bad = 0;
        for (int i = 0; i < cold.length; i++) if (cold[i] != corrupted[i]) bad++;
        note("returned " + bad + " wrong bytes out of " + cold.length
            + " -- the client served the tier's lie as truth");
      }

      // ---------------------------------------------------------------- 4
      // MISPLACED. Real data from this very object, at the wrong offset. This
      // is the one a checksum over content alone cannot catch, and the reason
      // ADR-0023's verification plan borrows flint-chaos's self-describing
      // values: the bytes must say WHERE they belong, not merely what they are.
      tier.flushall();
      Suite.read(c, KEY, OFF, LEN);
      keys = chunkKeys(tier);
      byte[] a = tier.get(keys.get(1)), b = tier.get(keys.get(2));
      check(!Arrays.equals(a, b), "armed: the two chunks about to be swapped differ");
      tier.set(keys.get(1), b);
      tier.set(keys.get(2), a);

      long if1 = c.integrityFailures.get();
      byte[] swapped = Suite.read(c, KEY, OFF, LEN);
      boolean swapSurvived = Arrays.equals(cold, swapped);
      check(swapSurvived, "MISPLACED chunk: bytes are still correct");
      check(c.integrityFailures.get() > if1,
          "armed: the seal rejected data that was VALID but in the wrong place");
      if (!swapSurvived) {
        int bad = 0;
        for (int i = 0; i < cold.length; i++) if (cold[i] != swapped[i]) bad++;
        note("returned " + bad + " wrong bytes out of " + cold.length
            + " -- valid data, wrong offset, served as truth");
      }

      // ---------------------------------------------------------------- 5
      // The negative control the other four need. A seal that rejected
      // EVERYTHING would pass all four checks above -- every read would fall
      // through to the origin and return correct bytes -- while destroying the
      // cache entirely. So: an untouched tier must reject nothing.
      tier.flushall();
      Suite.read(c, KEY, OFF, LEN);
      long if2 = c.integrityFailures.get();
      int g5 = Suite.gets();
      byte[] clean = Suite.read(c, KEY, OFF, LEN);
      check(Arrays.equals(cold, clean), "negative control: untouched tier reads correctly");
      check(c.integrityFailures.get() == if2,
          "negative control: and the seal rejected NOTHING (no false positives)");
      check(Suite.gets() == g5,
          "negative control: 0 origin GETs -- the cache still works, it was not "
          + "silently disabled by the check meant to protect it");

      // ---------------------------------------------------------------- 6
      // What the seal COSTS. Claiming a check is cheap without measuring it is
      // how cheap checks become expensive ones. Warm reads only, so the number
      // is the tier path and not the origin.
      tier.flushall();
      Suite.read(c, KEY, OFF, LEN);                       // populate
      for (int w = 0; w < 3; w++) Suite.read(c, KEY, OFF, LEN);   // let JIT settle
      int ROUNDS = 200;
      long t0 = System.nanoTime();
      for (int i = 0; i < ROUNDS; i++) Suite.read(c, KEY, OFF, LEN);
      double perRead = (System.nanoTime() - t0) / 1e3 / ROUNDS;

      byte[] probe = new byte[CHUNK];
      new Random(7).nextBytes(probe);
      byte[] sealed = null;
      for (int w = 0; w < 20_000; w++) sealed = FlintObjectClient.sealForBench("e", 1, probe);
      long t1 = System.nanoTime();
      for (int i = 0; i < 20_000; i++) c.unsealForBench("e", 1, sealed);
      double perChunk = (System.nanoTime() - t1) / 1e3 / 20_000;

      int chunks = ids(OFF, LEN).size();
      double share = 100.0 * (perChunk * chunks) / perRead;
      note(String.format("warm read of %d KiB (%d chunks): %.1f us", LEN / 1024, chunks, perRead));
      note(String.format("unseal per 64 KiB chunk: %.2f us  ->  %.1f%% of the read", perChunk, share));
      check(share < 25.0, String.format(
          "the integrity check costs under 25%% of a warm read (measured %.1f%%)", share));
    }

    conn.close(); rc.shutdown(); s3.close();
    System.out.println(ok ? "INTEGRITY SUITE PASSED" : "INTEGRITY SUITE FAILED");
    System.exit(ok ? 0 : 1);
  }
}
