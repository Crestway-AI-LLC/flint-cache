// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.*;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Every scenario the scattered spikes proved, against ONE client, in one run. */
public final class Suite {

  static final HttpClient HTTP = HttpClient.newHttpClient();
  static String endpoint, redisUrl;
  static final String BUCKET = "bucket";
  static boolean ok = true;
  static S3SeekableInputStreamConfiguration CFG = S3SeekableInputStreamConfiguration.DEFAULT;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static byte[] expect(String key, int gen, long off, int len) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      long abs = off + i;
      md.reset();
      out[i] = md.digest((key + ":" + gen + ":" + (abs / 16)).getBytes("UTF-8"))[(int) (abs % 16)];
    }
    return out;
  }

  static int genOf(String key, long off, byte[] got) throws Exception {
    for (int g = 0; g <= 3; g++) if (Arrays.equals(got, expect(key, g, off, got.length))) return g;
    return -1;
  }

  static byte[] read(ObjectClient c, String key, long off, int len) throws IOException {
    // Fresh factory every read: AAL's own cache must never answer for us (D12.5).
    try (var f = new S3SeekableInputStreamFactory(c, CFG);
         var in = f.createStream(S3URI.of(BUCKET, key))) {
      byte[] b = new byte[len];
      in.seek(off);
      in.read(b, 0, len);
      return b;
    }
  }

  static String stat(String path) throws Exception {
    return HTTP.send(HttpRequest.newBuilder(URI.create(endpoint + path)).build(),
        HttpResponse.BodyHandlers.ofString()).body();
  }

  static int gets() throws Exception {
    String b = stat("/__stats");
    int i = b.indexOf("\"gets\":");
    return Integer.parseInt(b.substring(i + 7, b.indexOf(',', i)).trim());
  }

  static void startTier() throws Exception {
    new ProcessBuilder("valkey-server", "--port", "6399", "--save", "",
        "--appendonly", "no", "--daemonize", "yes").start().waitFor();
    Thread.sleep(700);
  }

  static void killTier() throws Exception {
    new ProcessBuilder("valkey-cli", "-p", "6399", "shutdown", "nosave")
        .redirectErrorStream(true).start().waitFor();
    Thread.sleep(600);
  }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    redisUrl = args.length > 1 ? args[1] : "redis://127.0.0.1:6399";

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
    conn.sync().flushall();

    try (var sdk = new S3SdkObjectClient(s3, false)) {
      var c = new FlintObjectClient(sdk, conn.async(), 64 * 1024, 50, 2);

      // 1. cold + warm, bytes verified
      String k = "data/000001.bin";
      byte[] cold = read(c, k, 100_000, 8192);
      check(genOf(k, 100_000, cold) == 0, "cold read verifies against the oracle");
      long hitsBefore = c.chunkHits.get();
      byte[] warm = read(c, k, 100_000, 8192);
      check(Arrays.equals(cold, warm) && c.chunkHits.get() > hitsBefore,
          "warm read is served from the tier and matches");
      check(c.metaHits.get() > 0, "metadata served from the tier, so no repeat HEAD");

      // 2. cross-pattern sharing
      stat("/__reset");
      String k2 = "data/000002.bin";
      for (long off : new long[]{0, 65536, 131072, 196608}) read(c, k2, off, 65536);
      long shareBefore = c.chunkHits.get();
      for (long off : new long[]{88_000, 202_000, 3_100_000}) read(c, k2, off, 4096);
      check(c.chunkHits.get() > shareBefore,
          "a different access pattern reuses the first reader's chunks");

      // 3. single-flight under a genuine race
      //
      // This check USED to gate on `joined > 0` and failed about one loaded run
      // in three. A warm-up round to cut thread-start skew did not fix it
      // (4 failures in 8 runs under load), because the check was asking the
      // wrong question.
      //
      // `joined` is only ONE of two correct outcomes. A reader that arrives
      // after the leader has filled the tier HITS instead of joining, and that
      // is single-flight working, not failing. Which of the two a given reader
      // takes is pure timing. So the old check demanded a specific race
      // outcome and called the other one a bug.
      //
      // What is actually invariant -- and is the claim the product sells -- is
      // that 24 concurrent cold readers of ONE chunk cost about ONE origin
      // GET. That held in EVERY run, including all the failing ones. So gate
      // on that, tightened from `< 24` (which 23 GETs would have passed) to a
      // small constant, arm it on the machinery having run at all, and REPORT
      // the join count instead of gating on it.
      stat("/__reset");
      String k3 = "data/000003.bin";
      final int N = 24;
      long claimedBefore = c.claimed.get(), joinedBefore = c.joined.get();
      ExecutorService pool = Executors.newFixedThreadPool(N);
      CountDownLatch ready = new CountDownLatch(N), go = new CountDownLatch(1);
      List<Future<Boolean>> fs = new ArrayList<>();
      for (int i = 0; i < N; i++) {
        final long off = 262_144 + (i % 3) * 4096;
        fs.add(pool.submit(() -> {
          ready.countDown(); go.await();
          return genOf(k3, off, read(c, k3, off, 4096)) == 0;
        }));
      }
      ready.await(); go.countDown();
      boolean allOk = true;
      for (var f : fs) allOk &= f.get(60, TimeUnit.SECONDS);
      pool.shutdown();
      check(allOk, N + " concurrent readers all got correct bytes");
      int g = gets();
      long claimedNow = c.claimed.get() - claimedBefore, joinedNow = c.joined.get() - joinedBefore;
      // <= 3 rather than == 1: the leader publishes to the tier asynchronously,
      // so a reader arriving between the SET being issued and it landing can
      // legitimately miss and claim a second time. That window is real and
      // narrow; 23 duplicate fetches would not fit in it.
      check(g <= 3, N + " concurrent cold readers of one chunk caused " + g
          + " origin GETs (<= 3), a " + (100 - 100 * g / N) + "% saving");
      check(claimedNow >= 1,
          "armed-check: the single-flight path ran (" + claimedNow + " claimed)");
      System.out.println("       of " + N + " readers: " + claimedNow + " claimed, "
          + joinedNow + " joined in-flight, " + (N - claimedNow - joinedNow)
          + " arrived after the fill and hit -- all three are single-flight working");

      // 4. mutation contract
      stat("/__reset");
      String k4 = "data/000004.bin";
      byte[] g0 = read(c, k4, 100_000, 8192);
      stat("/__mutate?key=" + k4);
      byte[] during = read(c, k4, 100_000, 8192);
      int gDur = genOf(k4, 100_000, during);
      check(gDur == 0, "within the metadata TTL the read is stale (gen " + gDur + "), per contract");
      Thread.sleep(c.metaTtlSec * 1000 + 800);
      byte[] after = read(c, k4, 100_000, 8192);
      int gAft = genOf(k4, 100_000, after);
      check(gAft == 1, "after the TTL the read is the new object (gen " + gAft + ")");
      check(genOf(k4, 100_000, g0) >= 0 && gDur >= 0 && gAft >= 0,
          "no read was ever TORN across generations");

      // 5. D13: an SSE-C-shaped read must cache NOTHING, with a control.
      stat("/__reset");
      conn.sync().flushall();
      String k6 = "data/000006.bin";
      var bypassing = new FlintObjectClient(sdk, conn.async(), 64 * 1024, 50, 2, true);
      byte[] enc = read(bypassing, k6, 300_000, 8192);
      check(genOf(k6, 300_000, enc) == 0, "bypassing client still returns correct bytes");
      long keysAfterBypass = conn.sync().dbsize();
      check(keysAfterBypass == 0,
          "D13: a bypassing client wrote NOTHING to the tier (" + keysAfterBypass + " keys)");
      check(bypassing.bypassed.get() > 0, "armed-check: the bypass path was actually taken");

      // control: the SAME read through the caching client DOES populate the tier,
      // so the zero above is the bypass working rather than the read failing.
      byte[] plain = read(c, k6, 300_000, 8192);
      long keysAfterCaching = conn.sync().dbsize();
      check(Arrays.equals(enc, plain), "both clients return identical bytes");
      check(keysAfterCaching > 0,
          "negative control -- the CACHING client does populate the tier ("
              + keysAfterCaching + " keys)");

      // 6. THE INTERACTION (LAST -- it kills the tier and does not restart it): tier dies while readers are joined in flight.
      //    Neither the resilience spike nor the concurrency spike covers this;
      //    a follower joined to a leader that dies with the tier could wait
      //    forever, which is the hang D12.9 exists to prevent.
      System.out.println("     -- killing the tier mid-flight --");
      String k5 = "data/000005.bin";
      ExecutorService pool2 = Executors.newFixedThreadPool(8);
      CountDownLatch ready2 = new CountDownLatch(8), go2 = new CountDownLatch(1);
      List<Future<Boolean>> fs2 = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        final long off = 400_000 + (i % 2) * 4096;
        fs2.add(pool2.submit(() -> {
          ready2.countDown(); go2.await();
          try { return genOf(k5, off, read(c, k5, off, 4096)) == 0; }
          catch (Exception e) {
            System.out.println("        threw: " + e.getClass().getSimpleName());
            return false;
          }
        }));
      }
      ready2.await();
      long t0 = System.nanoTime();
      go2.countDown();
      Thread.sleep(30);
      killTier();
      boolean survived = true;
      for (var f : fs2) survived &= f.get(90, TimeUnit.SECONDS);
      double ms = (System.nanoTime() - t0) / 1e6;
      pool2.shutdown();
      check(survived, "readers joined in-flight SURVIVE the tier dying under them");
      check(ms < 30_000, String.format("and they finish promptly, not eventually (%.0f ms)", ms));
      check(c.tierFailures.get() > 0, "armed-check: tier failures were observed");

      System.out.println("\ncounters: " + c.counters());
      System.out.println("bypass:   " + bypassing.counters());
    } finally {
      try { conn.close(); } catch (Exception ignored) {}
      rc.shutdown();
      startTier();   // leave the box as we found it
    }

    System.out.println("\nFLINT-ACCEL SUITE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
