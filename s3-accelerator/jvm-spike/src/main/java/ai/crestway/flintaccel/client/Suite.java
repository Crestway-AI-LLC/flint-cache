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

  /**
   * Valkey or Redis, whichever this machine has.
   *
   * The suites need a Redis-protocol server and do not care which
   * implementation provides one; hardcoding `valkey-server` made this
   * unrunnable on any CI image that does not package it, which is most of
   * them. FLINT_TIER_SERVER / FLINT_TIER_CLI override, so the gate can pass
   * down whatever it resolved rather than each layer guessing separately.
   */
  static Process runTier(String env, String[] candidates, String... args) throws Exception {
    java.util.List<String> tries = new ArrayList<>();
    String override = System.getenv(env);
    if (override != null && !override.isEmpty()) tries.add(override);
    tries.addAll(Arrays.asList(candidates));
    IOException last = null;
    for (String bin : tries) {
      List<String> cmd = new ArrayList<>();
      cmd.add(bin);
      cmd.addAll(Arrays.asList(args));
      try {
        return new ProcessBuilder(cmd).redirectErrorStream(true).start();
      } catch (IOException e) {
        last = e;               // not on PATH; try the next
      }
    }
    throw new IllegalStateException(
        "no Redis-protocol server found (tried " + tries + ")", last);
  }

  /** The port of the tier this suite was GIVEN, not a constant.
   *
   * It was "9399" in both methods while the client dialled whatever redisUrl
   * named. When the gate moved its tier to another port, killTier() shut down
   * port 9399 -- a stranger's server, on a shared machine -- while the client's
   * own tier stayed up, so "tier failures were observed" observed none and the
   * suite failed pointing at the product. startTier() then left a NEW
   * daemonized server on 9399 behind it. One hardcode, three wrong outcomes:
   * a false failure, a neighbour's process killed, and a leak.
   */
  static int tierPort() {
    int i = redisUrl.lastIndexOf(':');
    if (i < 0) return 9399;
    String tail = redisUrl.substring(i + 1).replaceAll("[^0-9].*$", "");
    return tail.isEmpty() ? 9399 : Integer.parseInt(tail);
  }

  /** Poll a condition to a deadline. Returns false if it never became true.
   *
   *  Every fixed sleep this replaces was waiting for a state change that
   *  announces itself. A duration is only ever a GUESS at how long the change
   *  takes, and the guess is calibrated on an idle machine -- which is the one
   *  machine where waiting was never needed. */
  static boolean awaitTrue(java.util.function.BooleanSupplier cond, long budgetMs)
      throws Exception {
    long deadline = System.nanoTime() + budgetMs * 1_000_000L;
    while (System.nanoTime() < deadline) {
      if (cond.getAsBoolean()) return true;
      Thread.sleep(2);
    }
    return cond.getAsBoolean();
  }

  /** Does a TIER answer on the tier port -- not merely a listener?
   *
   *  `tierListening` below proves a socket accepted, which a stale process on
   *  a recycled port also does, and which a server still loading a dataset
   *  does while refusing every command with -LOADING. The protocol is the
   *  discrimination: +PONG comes only from something speaking RESP and ready
   *  to serve. Inline command, so no client library is needed in a static
   *  helper that runs before the connection exists and again after it closes.
   *
   *  The asymmetry is the point and is worth not smoothing over: liveness
   *  needs the PROTOCOL, death needs the SOCKET. A failed PING is a bad death
   *  signal -- it fails for a hung server, a full backlog, or a dropped packet,
   *  all of which leave the process alive -- so killTier still waits on
   *  connect-refused. Same port, opposite questions, different right answer. */
  static boolean tierAnswering() {
    try (java.net.Socket sk = new java.net.Socket()) {
      sk.connect(new java.net.InetSocketAddress("127.0.0.1", tierPort()), 200);
      sk.setSoTimeout(200);
      sk.getOutputStream().write("PING\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
      sk.getOutputStream().flush();
      byte[] buf = new byte[7];
      int n = sk.getInputStream().read(buf);
      return n >= 5 && new String(buf, 0, n, java.nio.charset.StandardCharsets.US_ASCII)
          .startsWith("+PONG");
    } catch (IOException e) {
      return false;
    }
  }

  /** Is anything accepting connections on the tier port? */
  static boolean tierListening() {
    try (java.net.Socket sk = new java.net.Socket()) {
      sk.connect(new java.net.InetSocketAddress("127.0.0.1", tierPort()), 200);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  static void startTier() throws Exception {
    runTier("FLINT_TIER_SERVER", new String[] {"valkey-server", "redis-server"},
        "--port", String.valueOf(tierPort()), "--save", "", "--appendonly", "no",
        "--daemonize", "yes").waitFor();
    // `--daemonize yes` EXITS 0 before the server listens, so waitFor() proves
    // nothing at all. This used to be `sleep(700)`, which is the same
    // non-signal with a longer fuse: fine idle, wrong under load.
    if (!awaitTrue(Suite::tierAnswering, 15_000))
      throw new IllegalStateException("tier never answered PING on port " + tierPort());
  }

  /** Kill the tier and return only once the port refuses.
   *
   *  It used to fork a valkey-cli and then sleep 600ms. Both are load-sensitive
   *  and neither is the signal -- and the fork sat inside a WALL-CLOCK
   *  ASSERTION further down, where a JVM ProcessBuilder under load is the most
   *  expensive thing in the window and has nothing to do with the property
   *  being measured. Shutting down over a connection we already hold costs
   *  microseconds and cannot be starved by the scheduler. */
  static void killTier(StatefulRedisConnection<byte[], byte[]> live) throws Exception {
    if (live != null) {
      // async, not sync: SHUTDOWN gets no reply because the server dies
      // answering it, and a sync call would sit on the command timeout inside
      // the one window this whole rewrite exists to keep clean.
      try { live.async().shutdown(false); }
      catch (RuntimeException expected) { /* the server dies mid-command */ }
    } else {
      runTier("FLINT_TIER_CLI", new String[] {"valkey-cli", "redis-cli"},
          "-p", String.valueOf(tierPort()), "shutdown", "nosave").waitFor();
    }
    if (!awaitTrue(() -> !tierListening(), 15_000))
      throw new IllegalStateException("tier still listening after shutdown");
  }

  static void killTier() throws Exception { killTier(null); }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    redisUrl = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";

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
      var c = new FlintObjectClient(sdk, conn.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2, false, s3, false);

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
      long degradedBefore = c.degraded.get();
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
      long degradedNow = c.degraded.get() - degradedBefore;
      // <= 3 rather than == 1: the leader publishes to the tier asynchronously,
      // so a reader arriving between the SET being issued and it landing can
      // legitimately miss and claim a second time. That window is real and
      // narrow; 23 duplicate fetches would not fit in it.
      //
      // PLUS one per degraded reader, and that term is not slack. A reader
      // whose tier operation exceeds its budget takes passthrough() -- it
      // skips the tier AND the single-flight, and fetches from the origin on
      // its own. That is D12.36 working as designed: a cache may never make a
      // read slower than no cache. So origin GETs are claims plus degraded
      // passthroughs, and on a contended two-core runner the second term is
      // routinely nonzero.
      //
      // Without the term this check failed every CI run -- 6 GETs, then 4 --
      // while single-flight was working correctly and only two readers ever
      // claimed. It was reporting the runner's CPU contention as a product
      // defect. The check stays armed because a genuine single-flight
      // regression raises CLAIMS, which this bound does not forgive.
      check(g <= 3 + degradedNow, N + " concurrent cold readers of one chunk caused "
          + g + " origin GETs (<= 3 + " + degradedNow + " degraded), a "
          + (100 - 100 * g / N) + "% saving");
      check(claimedNow >= 1,
          "armed-check: the single-flight path ran (" + claimedNow + " claimed)");
      System.out.println("       of " + N + " readers: " + claimedNow + " claimed, "
          + joinedNow + " joined in-flight, " + (N - claimedNow - joinedNow)
          + " arrived after the fill and hit -- all three are single-flight working"
          + (degradedNow > 0 ? "; " + degradedNow + " degraded past the tier on budget"
                             + " (D12.36), each paying its own origin GET" : ""));

      // 4. mutation contract
      stat("/__reset");
      String k4 = "data/000004.bin";
      byte[] g0 = read(c, k4, 100_000, 8192);
      stat("/__mutate?key=" + k4);
      byte[] during = read(c, k4, 100_000, 8192);
      int gDur = genOf(k4, 100_000, during);
      check(gDur == 0, "within the metadata TTL the read is stale (gen " + gDur + "), per contract");
      // Wait for the metadata entry to be GONE, not for a duration we believe
      // covers its TTL. The `+ 800` was a fudge on top of a guess, and neither
      // number describes anything: what the next read depends on is the key's
      // absence, which the tier will tell us about if asked. Not tautological
      // -- the assertion below is about what the read RETURNS once the cached
      // metadata is gone, which is a different claim from "it is gone".
      if (!awaitTrue(() -> conn.sync().keys(("m1/*" + k4).getBytes(java.nio.charset.StandardCharsets.UTF_8)).isEmpty(),
                     c.metaTtlSec * 1000 + 10_000))
        throw new IllegalStateException("metadata for " + k4 + " never expired");
      byte[] after = read(c, k4, 100_000, 8192);
      int gAft = genOf(k4, 100_000, after);
      check(gAft == 1, "after the TTL the read is the new object (gen " + gAft + ")");
      check(genOf(k4, 100_000, g0) >= 0 && gDur >= 0 && gAft >= 0,
          "no read was ever TORN across generations");

      // 5. D13: an SSE-C-shaped read must cache NOTHING, with a control.
      stat("/__reset");
      conn.sync().flushall();
      String k6 = "data/000006.bin";
      var bypassing = new FlintObjectClient(sdk, conn.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2, true, s3, false);
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

      // 5b. D17: an object above the cap is READ, and never cached.
      //
      // The bound is on the KEYSPACE as much as the bytes -- an object of size
      // S occupies S/chunkBytes keys, so one very large object can cost more
      // in per-key overhead than its data is worth to anybody sharing the
      // tier. The corpus objects here are 8 MiB, so a 1 MiB cap makes them
      // oversize without needing a special fixture.
      stat("/__reset");
      conn.sync().flushall();
      String k7 = "data/000007.bin";
      var capped = new FlintObjectClient(sdk, conn.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 2,
          false, s3, false, false, 86_400, 1024 * 1024);
      byte[] big = read(capped, k7, 300_000, 8192);
      check(genOf(k7, 300_000, big) == 0, "an oversize object still READS correctly");
      // CHUNK keys, not dbsize. The cap is a CAPACITY rule and metadata is
      // ~50 bytes that saves a HEAD, so an oversize object still caches its
      // metadata. Only D13's SSE-C bypass suppresses both, because that one
      // is a SECURITY rule and the length and etag of bytes we must not see
      // are themselves something we must not store. Asserting dbsize()==0
      // here conflated the two policies and failed on the metadata entry.
      long chunksAfterCap = conn.sync().keys("c1/*".getBytes(java.nio.charset.StandardCharsets.UTF_8)).size();
      check(chunksAfterCap == 0,
          "D17: above the cap the tier got NO CHUNKS (" + chunksAfterCap + ")");
      check(conn.sync().keys("m1/*".getBytes(java.nio.charset.StandardCharsets.UTF_8)).size() > 0,
          "and its METADATA is still cached -- a capacity cap, not a security bypass");
      check(capped.oversizeBypassed.get() > 0,
          "armed-check: counted as oversize (" + capped.oversizeBypassed.get()
              + "), not silently missing");
      // The control carries the weight: without it, a client that cached
      // nothing for ANY reason would pass both checks above.
      byte[] uncapped = read(c, k7, 300_000, 8192);
      check(Arrays.equals(big, uncapped), "capped and uncapped return identical bytes");
      check(conn.sync().dbsize() > 0,
          "negative control -- UNDER the cap the same object IS cached");

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
      // Warm the METADATA and leave chunk 6 COLD. Without this the eight
      // readers each do their own HEAD first, which staggers them by however
      // long the origin takes, and whether anyone is still joined when the
      // tier dies becomes a coin flip -- runs of this suite reported 0 joined
      // and 1 joined on the same code. With metadata cached they race straight
      // to one chunk key: one leads, seven join, every time.
      read(c, k5, 0, 4096);
      final long claim0 = c.claimed.get(), join0 = c.joined.get();

      ready2.await();
      go2.countDown();
      // WAIT FOR THE SIGNAL, not for a duration. `sleep(30)` was a guess at how
      // long eight threads take to reach one chunk key, and a guess calibrated
      // on an idle machine is wrong on the machine where it matters.
      boolean armed = awaitTrue(() -> c.joined.get() > join0, 2_000);
      check(armed, String.format(
          "armed: readers are JOINED to an in-flight fetch when the tier dies (%d joined, %d claimed)",
          c.joined.get() - join0, c.claimed.get() - claim0));
      // Kill over the connection we already hold: microseconds, and nothing
      // the scheduler can starve. The clock starts AFTER it, because the
      // property is the readers' unwind -- the old window also contained a
      // 30 ms sleep, a JVM fork/exec of a CLI, and a 600 ms sleep, and under
      // load the fork was the largest term in a number reported as "readers
      // finish promptly".
      killTier(conn);
      long t0 = System.nanoTime();
      boolean survived = true;
      for (var f : fs2) survived &= f.get(90, TimeUnit.SECONDS);
      double ms = (System.nanoTime() - t0) / 1e6;
      pool2.shutdown();
      check(survived, "readers joined in-flight SURVIVE the tier dying under them");
      // NOT `degraded > 0`. I asserted that and it failed, correctly: the
      // leader's origin fetch completes and serves all fourteen joiners from
      // its own result, so nobody needs to fall back. Degradation is the
      // LEADER's failure path, and asserting it here demanded a fallback the
      // scenario does not call for.
      //
      // Worth recording that the racy version DID see `degraded 8`, because
      // with no joiners the readers were mid-tier-read when it died. Making
      // this deterministic on the joined path gave that up -- but only here:
      // SickTierSuite drives a client at a deliberately sick tier and asserts
      // the degradation path against a measured no-tier baseline, which is a
      // better home for it than a race that happened to land there.
      //
      // What IS load-immune, and what makes the clock below mean something:
      // the armed check above proves readers were joined, tierFailures proves
      // the tier really died under them, and `survived` proves every one came
      // back with correct bytes.
      check(c.tierFailures.get() > 0, "armed-check: the tier really did fail under them");
      // The clock is now a BACKSTOP behind that counter rather than the only
      // guard, and it is expressed as a multiple of the configured budget --
      // the joiner's own deadline is budget x 40, so budget x 600 leaves an
      // order of magnitude and still catches a hang.
      long bound = c.tierBudgetMs * 600;
      check(ms < bound, String.format(
          "and they finish promptly, not eventually (%.0f ms, bound %d = budget x 600)", ms, bound));

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
