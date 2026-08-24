// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.*;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * The property that decides whether this is deployable: what happens when the
 * tier is gone?
 *
 * A look-aside cache sits in the data path of someone's production Spark job.
 * If a tier outage FAILS the read rather than falling through to S3, the cache
 * can take down the thing it was bought to accelerate — strictly worse than
 * not deploying it. The naive composition does exactly that: a failed
 * `tier.get()` propagates up through AAL and out to the application.
 *
 * S3 is authoritative and always reachable. Every tier interaction is
 * therefore an OPTIMISATION and must be written as one: any failure, on any
 * path, degrades to the origin and the read still succeeds.
 */
public final class ResilienceSpike {

  static final int CHUNK = 64 * 1024;

  /**
   * Bound on any single tier interaction.
   *
   * This exists because of a failure that a naive .handle() cannot catch.
   * Lettuce's DEFAULT disconnectedBehavior QUEUES commands until reconnect,
   * so killing the tier does not fail the future -- it makes it never
   * complete. The first version of this spike hung for five minutes on a dead
   * tier, which in a Spark executor is worse than an error: the task slot is
   * held indefinitely rather than released.
   *
   * An error path that the error never reaches is the same shape as a check
   * that cannot fail. So resilience needs BOTH: reject-when-disconnected, and
   * a hard timeout as the backstop for anything slow rather than broken.
   */
  static final java.time.Duration TIER_BUDGET = java.time.Duration.ofMillis(50);

  static final class ResilientObjectClient implements ObjectClient {
    private final ObjectClient origin;
    private final RedisAsyncCommands<byte[], byte[]> tier;
    final AtomicInteger tierFailures = new AtomicInteger();
    final AtomicInteger degradedReads = new AtomicInteger();
    final AtomicInteger tierHits = new AtomicInteger();

    ResilientObjectClient(ObjectClient origin, RedisAsyncCommands<byte[], byte[]> tier) {
      this.origin = origin; this.tier = tier;
    }

    private static byte[] ck(String etag, long id) {
      return ("c/" + etag + "/" + id).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override public CompletableFuture<ObjectMetadata> headObject(HeadRequest r) {
      return origin.headObject(r);
    }
    @Override public CompletableFuture<ObjectContent> getObject(GetRequest r) { return get(r, null); }
    @Override public CompletableFuture<ObjectContent> getObject(GetRequest r, StreamContext c) { return get(r, c); }

    /** Straight to the origin, exactly as if no cache existed. */
    private CompletableFuture<ObjectContent> passthrough(GetRequest r, StreamContext ctx) {
      degradedReads.incrementAndGet();
      return (ctx == null) ? origin.getObject(r) : origin.getObject(r, ctx);
    }

    private CompletableFuture<ObjectContent> get(GetRequest r, StreamContext ctx) {
      final long start = r.getRange().getStart(), end = r.getRange().getEnd();
      final long first = start / CHUNK, last = end / CHUNK;
      final int n = (int) (last - first + 1);
      final String etag = r.getEtag();

      byte[][] keys = new byte[n][];
      for (int i = 0; i < n; i++) keys[i] = ck(etag, first + i);

      CompletableFuture<java.util.List<io.lettuce.core.KeyValue<byte[], byte[]>>> lookup;
      try {
        lookup = tier.mget(keys).toCompletableFuture();
      } catch (RuntimeException e) {
        // Lettuce can throw synchronously when the connection is already dead.
        tierFailures.incrementAndGet();
        return passthrough(r, ctx);
      }

      return lookup
          .orTimeout(TIER_BUDGET.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
          // ANY tier failure OR timeout -> null, read as "no cache" downstream.
          .handle((vals, err) -> {
            if (err != null) { tierFailures.incrementAndGet(); return null; }
            return vals;
          })
          .thenCompose(vals -> {
            if (vals == null) return passthrough(r, ctx);

            byte[][] have = new byte[n][];
            boolean allHit = true;
            for (int i = 0; i < n; i++) {
              var kv = vals.get(i);
              have[i] = kv.hasValue() ? kv.getValue() : null;
              if (have[i] == null) allHit = false;
            }
            if (allHit) {
              tierHits.incrementAndGet();
              java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
              for (int i = 0; i < n; i++) {
                long cStart = (first + i) * CHUNK;
                int from = (int) Math.max(0, start - cStart);
                int to = (int) Math.min(have[i].length, end - cStart + 1);
                if (to > from) out.write(have[i], from, to - from);
              }
              return CompletableFuture.completedFuture(ObjectContent.builder()
                  .stream(new ByteArrayInputStream(out.toByteArray())).build());
            }

            // Partial or total miss: serve from the origin and fill what we can.
            CompletableFuture<ObjectContent> up =
                (ctx == null) ? origin.getObject(r) : origin.getObject(r, ctx);
            return up.thenApply(oc -> {
              byte[] all;
              try (InputStream in = oc.getStream()) { all = in.readAllBytes(); }
              catch (IOException e) { throw new RuntimeException(e); }
              // NOTE (found by MutationSpike): this fill only fires when AAL's
              // range happens to be 64 KiB-aligned, because it slices AAL's
              // own range rather than fetching on the chunk grid. D12.7 showed
              // AAL anchors ranges to the read offset, so that is the
              // exception. This spike reads at an aligned offset, so it works
              // here; the correct pattern is ChunkedTierSpike's grid-aligned
              // fetch. Left as-is deliberately -- this file's subject is
              // degradation, not the fill path -- but it is NOT the shape to
              // copy.
              //
              // The FILL is best-effort too: a dead tier must not fail a read
              // whose bytes we already hold.
              try {
                long fillFrom = first * CHUNK;
                for (int i = 0; (long) i * CHUNK < all.length; i++) {
                  if (start != fillFrom) break;      // only grid-aligned fills
                  int off = i * CHUNK, len = Math.min(CHUNK, all.length - off);
                  if (len == CHUNK) {
                    tier.set(ck(etag, first + i), Arrays.copyOfRange(all, off, off + len));
                  }
                }
              } catch (RuntimeException e) {
                tierFailures.incrementAndGet();
              }
              return ObjectContent.builder().stream(new ByteArrayInputStream(all)).build();
            });
          })
          .toCompletableFuture();
    }

    @Override public void close() throws IOException { origin.close(); }
  }

  static byte[] expected(String key, long start, int len) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      long abs = start + i, block = abs / 16;
      md.reset();
      out[i] = md.digest((key + ":" + block).getBytes("UTF-8"))[(int) (abs % 16)];
    }
    return out;
  }

  static boolean readAndVerify(S3SeekableInputStreamFactory f, String bucket,
                               String key, long off, int len) {
    try (var in = f.createStream(S3URI.of(bucket, key))) {
      byte[] b = new byte[len];
      in.seek(off);
      int n = in.read(b, 0, len);
      return n == len && Arrays.equals(b, expected(key, off, len));
    } catch (Exception e) {
      System.out.println("      read threw: " + e.getClass().getSimpleName()
          + ": " + String.valueOf(e.getMessage()).split("\n")[0]);
      return false;
    }
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String redis = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    String bucket = "bucket", key = "data/000006.bin";
    boolean ok = true;

    RedisClient rc = RedisClient.create(redis);
    // Reject rather than queue while disconnected, so a dead tier produces a
    // FAILURE the code above can see instead of a future that never settles.
    rc.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .cancelCommandsOnReconnectFailure(true)
        .autoReconnect(true)
        .build());
    StatefulRedisConnection<byte[], byte[]> conn = rc.connect(new ByteArrayCodec());
    conn.sync().flushall();

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("spike", "spike")))
        .forcePathStyle(true).build();
    var cfg = S3SeekableInputStreamConfiguration.DEFAULT;

    try (var sdk = new S3SdkObjectClient(s3, false)) {
      var c = new ResilientObjectClient(sdk, conn.async());

      // Phase 1: healthy tier — cold then warm.
      boolean cold, warm;
      try (var f = new S3SeekableInputStreamFactory(c, cfg)) {
        cold = readAndVerify(f, bucket, key, 131072, 4096);
      }
      try (var f = new S3SeekableInputStreamFactory(c, cfg)) {
        warm = readAndVerify(f, bucket, key, 131072, 4096);
      }
      ok &= cold && warm;
      System.out.printf("[%s] healthy tier: cold and warm reads both correct "
          + "(%d tier hits)%n", (cold && warm) ? "ok" : "FAIL", c.tierHits.get());

      // Phase 2: KILL the tier, then keep reading.
      System.out.println("     -- killing the tier --");
      new ProcessBuilder("valkey-cli", "-p", "9399", "shutdown", "nosave")
          .redirectErrorStream(true).start().waitFor();
      Thread.sleep(600);

      int failuresBefore = c.tierFailures.get();
      boolean survived = true;
      long t0 = System.nanoTime();
      for (int i = 0; i < 4; i++) {
        try (var f = new S3SeekableInputStreamFactory(c, cfg)) {
          survived &= readAndVerify(f, bucket, key, 200000 + i * 4096, 4096);
        }
      }
      double elapsedMs = (System.nanoTime() - t0) / 1e6;
      ok &= survived;
      System.out.printf("[%s] TIER DOWN: reads still succeed and verify "
          + "(%d tier failures observed, %d degraded to origin)%n",
          survived ? "ok" : "FAIL",
          c.tierFailures.get() - failuresBefore, c.degradedReads.get());
      boolean fast = elapsedMs < 4 * TIER_BUDGET.toMillis() + 3000;
      ok &= fast;
      System.out.printf("[%s] and they degrade FAST, not merely eventually "
          + "(%.0f ms for 4 reads)%n", fast ? "ok" : "FAIL", elapsedMs);

      boolean noticed = c.tierFailures.get() > failuresBefore;
      ok &= noticed;
      System.out.printf("[%s] armed-check: the failures were actually OBSERVED, "
          + "not silently absent%n", noticed ? "ok" : "FAIL");
    }

    try { conn.close(); } catch (Exception ignored) {}
    rc.shutdown();
    System.out.println("\nRESILIENCE SPIKE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
