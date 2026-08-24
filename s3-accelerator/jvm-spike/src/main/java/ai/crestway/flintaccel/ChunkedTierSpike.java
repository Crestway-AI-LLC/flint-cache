// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.RedisClient;
import io.lettuce.core.KeyValue;
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
 * Does the sharing D12.7 PROJECTED actually happen?
 *
 * D12.7 recorded AAL's ranges and computed, on paper, that 64 KiB grid-aligned
 * chunks would share 5 chunks between a sequential and a random reader while
 * whole-range caching shares zero. That is arithmetic over a trace, not a
 * measurement. This implements the chunking and counts real hits.
 *
 * The chunk layer is the actual product mechanism: split each AAL range onto
 * an absolute 64 KiB grid, MGET what we have, fetch contiguous runs of what we
 * do not, reassemble, return exactly the bytes asked for.
 */
public final class ChunkedTierSpike {

  static final int CHUNK = 64 * 1024;

  static final class ChunkedObjectClient implements ObjectClient {
    private final ObjectClient origin;
    private final RedisAsyncCommands<byte[], byte[]> tier;
    final AtomicInteger chunkHits = new AtomicInteger();
    final AtomicInteger chunkMisses = new AtomicInteger();
    final AtomicInteger originGets = new AtomicInteger();
    final AtomicLong originBytes = new AtomicLong();

    ChunkedObjectClient(ObjectClient origin, RedisAsyncCommands<byte[], byte[]> tier) {
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

    private CompletableFuture<ObjectContent> get(GetRequest r, StreamContext ctx) {
      final long start = r.getRange().getStart(), end = r.getRange().getEnd();
      final long first = start / CHUNK, last = end / CHUNK;
      final int n = (int) (last - first + 1);
      final String etag = r.getEtag();

      byte[][] keys = new byte[n][];
      for (int i = 0; i < n; i++) keys[i] = ck(etag, first + i);

      return tier.mget(keys).toCompletableFuture().thenCompose(vals -> {
        byte[][] have = new byte[n][];
        List<int[]> runs = new ArrayList<>();      // contiguous runs of missing chunks
        int runStart = -1;
        for (int i = 0; i < n; i++) {
          KeyValue<byte[], byte[]> kv = vals.get(i);
          byte[] v = kv.hasValue() ? kv.getValue() : null;
          have[i] = v;
          if (v == null) {
            chunkMisses.incrementAndGet();
            if (runStart < 0) runStart = i;
          } else {
            chunkHits.incrementAndGet();
            if (runStart >= 0) { runs.add(new int[]{runStart, i - 1}); runStart = -1; }
          }
        }
        if (runStart >= 0) runs.add(new int[]{runStart, n - 1});

        // Fetch each missing run as ONE ranged GET on the chunk grid.
        List<CompletableFuture<Void>> fills = new ArrayList<>();
        for (int[] run : runs) {
          long lo = (first + run[0]) * CHUNK;
          long hi = (first + run[1]) * CHUNK + CHUNK - 1;
          GetRequest sub = GetRequest.builder()
              .s3Uri(r.getS3Uri()).etag(etag)
              .referrer(r.getReferrer())
              .range(new Range(lo, hi)).build();
          originGets.incrementAndGet();
          final int rs = run[0];
          CompletableFuture<ObjectContent> up =
              (ctx == null) ? origin.getObject(sub) : origin.getObject(sub, ctx);
          fills.add(up.thenAccept(oc -> {
            try (InputStream in = oc.getStream()) {
              byte[] all = in.readAllBytes();
              originBytes.addAndGet(all.length);
              for (int i = 0; i * CHUNK < all.length; i++) {
                int off = i * CHUNK, len = Math.min(CHUNK, all.length - off);
                byte[] piece = Arrays.copyOfRange(all, off, off + len);
                have[rs + i] = piece;
                tier.set(ck(etag, first + rs + i), piece);
              }
            } catch (IOException e) { throw new RuntimeException(e); }
          }));
        }

        return CompletableFuture.allOf(fills.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
              // Reassemble exactly [start, end].
              java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
              for (int i = 0; i < n; i++) {
                byte[] c = have[i];
                if (c == null) continue;
                long cStart = (first + i) * CHUNK;
                int from = (int) Math.max(0, start - cStart);
                int to = (int) Math.min(c.length, end - cStart + 1);
                if (to > from) out.write(c, from, to - from);
              }
              return ObjectContent.builder()
                  .stream(new ByteArrayInputStream(out.toByteArray())).build();
            });
      }).toCompletableFuture();
    }

    @Override public void close() throws IOException { origin.close(); }
  }

  static byte[] expected(String key, long start, int len) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      long abs = start + i, block = abs / 16;
      md.reset();
      byte[] b = md.digest((key + ":" + block).getBytes("UTF-8"));
      out[i] = b[(int) (abs % 16)];
    }
    return out;
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String redis = args.length > 1 ? args[1] : "redis://127.0.0.1:6399";
    String bucket = "bucket", key = "data/000004.bin";
    boolean ok = true;

    RedisClient rc = RedisClient.create(redis);
    StatefulRedisConnection<byte[], byte[]> conn = rc.connect(new ByteArrayCodec());
    conn.sync().flushall();

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("spike", "spike")))
        .forcePathStyle(true).build();
    var cfg = S3SeekableInputStreamConfiguration.DEFAULT;

    try (var sdk = new S3SdkObjectClient(s3, false)) {
      var c = new ChunkedObjectClient(sdk, conn.async());

      // Reader A: sequential, cold.
      long[] seq = {0, 65536, 131072, 196608, 262144, 327680, 393216, 458752};
      for (long off : seq) {
        try (var f = new S3SeekableInputStreamFactory(c, cfg);
             var in = f.createStream(S3URI.of(bucket, key))) {
          byte[] b = new byte[65536]; in.seek(off); in.read(b, 0, 65536);
        }
      }
      int hitsAfterA = c.chunkHits.get(), missAfterA = c.chunkMisses.get();
      System.out.printf("  reader A (sequential, cold): %d chunk hits, %d misses, "
          + "%d origin GETs%n", hitsAfterA, missAfterA, c.originGets.get());

      // Reader B: random offsets that OVERLAP A's region — different ranges entirely.
      long[] rnd = {3_100_000, 511_000, 7_900_000, 1_250_000,
                    6_020_000, 202_000, 4_400_000, 88_000};
      for (long off : rnd) {
        try (var f = new S3SeekableInputStreamFactory(c, cfg);
             var in = f.createStream(S3URI.of(bucket, key))) {
          byte[] b = new byte[4096]; in.seek(off); in.read(b, 0, 4096);
          byte[] want = expected(key, off, 4096);
          if (!Arrays.equals(b, want)) {
            ok = false;
            System.out.printf("  [FAIL] reassembly wrong at offset %d%n", off);
          }
        }
      }
      int sharedHits = c.chunkHits.get() - hitsAfterA;
      System.out.printf("  reader B (random, warm):     %d chunk hits from A's chunks%n",
          sharedHits);

      System.out.printf("%n[%s] reassembled bytes verify against the oracle at every "
          + "random offset%n", ok ? "ok" : "FAIL");

      boolean shared = sharedHits > 0;
      ok &= shared;
      System.out.printf("[%s] a DIFFERENT access pattern reused chunks the first "
          + "reader fetched (%d)%n", shared ? "ok" : "FAIL", sharedHits);
      System.out.printf("     D12.7 projected 5 shared chunks from the recorded trace; "
          + "measured %d%n", sharedHits);

      System.out.printf("[--] origin: %d GETs, %d bytes for %d chunk misses%n",
          c.originGets.get(), c.originBytes.get(), c.chunkMisses.get());
      System.out.printf("[--] tier holds %d chunk keys%n", conn.sync().dbsize());
    }

    conn.close(); rc.shutdown();
    System.out.println("\nCHUNKED TIER SPIKE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
