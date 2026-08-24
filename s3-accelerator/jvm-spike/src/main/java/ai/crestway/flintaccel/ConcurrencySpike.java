// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
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
 * D4 and D5 together, which is where the real implementation lives.
 *
 * They have only been tested apart. The economic spike proved single-flight
 * over whole ranges; adding chunking (D12.8) SILENTLY DROPPED it, so N
 * concurrent readers would have fetched the same chunks N times — a
 * regression that no correctness test would notice, since the bytes stay
 * right and only the bill moves.
 *
 * Single-flight is per CHUNK, not per request. Two readers whose ranges
 * overlap partially must share the overlap and fetch only their own
 * remainder; keying the claim on the whole request would miss that, which is
 * exactly the case D12.7 showed is normal rather than rare.
 */
public final class ConcurrencySpike {

  static final int CHUNK = 64 * 1024;
  static final long TIER_BUDGET_MS = 50;

  static final class SingleFlightChunkClient implements ObjectClient {
    private final ObjectClient origin;
    private final RedisAsyncCommands<byte[], byte[]> tier;
    private final ConcurrentMap<String, CompletableFuture<byte[]>> inflight =
        new ConcurrentHashMap<>();
    final AtomicInteger originGets = new AtomicInteger();
    final AtomicInteger chunkHits = new AtomicInteger();
    final AtomicInteger claimed = new AtomicInteger();
    final AtomicInteger joined = new AtomicInteger();

    SingleFlightChunkClient(ObjectClient origin, RedisAsyncCommands<byte[], byte[]> tier) {
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

      return tier.mget(keys).toCompletableFuture()
          .orTimeout(TIER_BUDGET_MS, TimeUnit.MILLISECONDS)
          .handle((vals, err) -> err != null ? null : vals)
          .thenCompose(vals -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<byte[]>[] slots = new CompletableFuture[n];
            List<Integer> mine = new ArrayList<>();

            for (int i = 0; i < n; i++) {
              byte[] cached = null;
              if (vals != null) {
                var kv = vals.get(i);
                cached = kv.hasValue() ? kv.getValue() : null;
              }
              if (cached != null) {
                chunkHits.incrementAndGet();
                slots[i] = CompletableFuture.completedFuture(cached);
                continue;
              }
              // D5, per chunk: claim it, or join whoever already has.
              final long id = first + i;
              final String ik = etag + "/" + id;
              final int idx = i;
              boolean[] leader = {false};
              slots[i] = inflight.computeIfAbsent(ik, k -> {
                leader[0] = true;
                return new CompletableFuture<byte[]>();
              });
              if (leader[0]) { claimed.incrementAndGet(); mine.add(idx); }
              else { joined.incrementAndGet(); }
            }

            // Fetch only the chunks THIS caller claimed, coalesced into runs.
            for (int[] run : runs(mine)) {
              long lo = (first + run[0]) * CHUNK, hi = (first + run[1]) * CHUNK + CHUNK - 1;
              GetRequest sub = GetRequest.builder()
                  .s3Uri(r.getS3Uri()).etag(etag).referrer(r.getReferrer())
                  .range(new Range(lo, hi)).build();
              originGets.incrementAndGet();
              final int rs = run[0], re = run[1];
              CompletableFuture<ObjectContent> up =
                  (ctx == null) ? origin.getObject(sub) : origin.getObject(sub, ctx);
              up.whenComplete((oc, err) -> {
                for (int i = rs; i <= re; i++) {
                  String ik = etag + "/" + (first + i);
                  CompletableFuture<byte[]> slot = inflight.remove(ik);
                  if (slot == null) continue;
                  if (err != null) { slot.completeExceptionally(err); continue; }
                  try (InputStream in = oc.getStream()) {
                    // one stream per run: read once, split on the grid
                    byte[] all = in.readAllBytes();
                    for (int j = rs; j <= re; j++) {
                      int off = (j - rs) * CHUNK;
                      if (off >= all.length) break;
                      int len = Math.min(CHUNK, all.length - off);
                      byte[] piece = Arrays.copyOfRange(all, off, off + len);
                      CompletableFuture<byte[]> s2 = (j == i) ? slot
                          : inflight.remove(etag + "/" + (first + j));
                      tier.set(ck(etag, first + j), piece);
                      if (s2 != null) s2.complete(piece);
                    }
                    return;   // the whole run was completed by this pass
                  } catch (IOException e) { slot.completeExceptionally(e); }
                }
              });
            }

            return CompletableFuture.allOf(slots)
                .thenApply(v -> {
                  java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                  for (int i = 0; i < n; i++) {
                    byte[] c = slots[i].join();
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

    /** Contiguous runs within a sorted index list. */
    private static List<int[]> runs(List<Integer> idx) {
      List<int[]> out = new ArrayList<>();
      if (idx.isEmpty()) return out;
      Collections.sort(idx);
      int s = idx.get(0), p = s;
      for (int k = 1; k < idx.size(); k++) {
        int v = idx.get(k);
        if (v != p + 1) { out.add(new int[]{s, p}); s = v; }
        p = v;
      }
      out.add(new int[]{s, p});
      return out;
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

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String redis = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    String bucket = "bucket", key = "data/000007.bin";
    final int N = 32;
    boolean ok = true;

    RedisClient rc = RedisClient.create(redis);
    rc.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .cancelCommandsOnReconnectFailure(true).build());
    StatefulRedisConnection<byte[], byte[]> conn = rc.connect(new ByteArrayCodec());
    conn.sync().flushall();

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("spike", "spike")))
        .forcePathStyle(true).build();
    var cfg = S3SeekableInputStreamConfiguration.DEFAULT;

    try (var sdk = new S3SdkObjectClient(s3, false)) {
      var c = new SingleFlightChunkClient(sdk, conn.async());
      ExecutorService pool = Executors.newFixedThreadPool(N);
      List<Future<Boolean>> fs = new ArrayList<>();
      // A latch, because the first version of this test did not test anything.
      // Without it the readers trickled in behind their own HEADs, the first
      // fill landed before the rest arrived, and every later reader took a
      // TIER HIT. Dedup still read 32 -> 2, which looks exactly like
      // single-flight working while single-flight never ran. The joined
      // counter is what exposed it; the latch is what fixes it.
      final CountDownLatch go = new CountDownLatch(1);
      final CountDownLatch ready = new CountDownLatch(N);
      for (int i = 0; i < N; i++) {
        final long off = 262144 + (i % 4) * 4096;   // 4 offsets, same chunks
        fs.add(pool.submit(() -> {
          ready.countDown();
          go.await();
          try (var f = new S3SeekableInputStreamFactory(c, cfg);
               var in = f.createStream(S3URI.of(bucket, key))) {
            byte[] b = new byte[4096];
            in.seek(off);
            int n = in.read(b, 0, 4096);
            return n == 4096 && Arrays.equals(b, expected(key, off, 4096));
          }
        }));
      }
      ready.await();
      go.countDown();
      boolean allCorrect = true;
      for (var f : fs) allCorrect &= f.get(60, TimeUnit.SECONDS);
      pool.shutdown();

      ok &= allCorrect;
      System.out.printf("[%s] all %d concurrent readers got correct bytes%n",
          allCorrect ? "ok" : "FAIL", N);

      boolean dedup = c.originGets.get() < N;
      ok &= dedup;
      System.out.printf("[%s] %d concurrent cold readers -> %d origin GET(s) "
          + "(claimed %d chunks, joined %d in-flight)%n",
          dedup ? "ok" : "FAIL", N, c.originGets.get(), c.claimed.get(), c.joined.get());

      boolean shared = c.joined.get() > 0;
      ok &= shared;
      System.out.printf("[%s] armed-check: readers actually JOINED in-flight fetches "
          + "rather than all arriving after the fill%n", shared ? "ok" : "FAIL");
    }

    conn.close(); rc.shutdown();
    System.out.println("\nCONCURRENCY SPIKE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
