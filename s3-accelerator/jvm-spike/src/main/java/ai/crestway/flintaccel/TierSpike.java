// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * The last unproven link: a REAL network tier inside AAL's async path.
 *
 * The economic spike proved the shape with a HashMap. This replaces it with a
 * Redis-protocol backend over a socket, which is what the product actually
 * does, and asks two questions a map cannot answer:
 *
 *   1. Does the round trip compose with AAL's CompletableFuture chain without
 *      blocking a caller thread? Lettuce's async API returns a
 *      CompletionStage, so the whole path stays non-blocking -- that is why
 *      it is the right client rather than a sync one.
 *   2. Is a tier hit actually faster than S3, measured through AAL rather
 *      than in isolation?
 *
 * Runs against Valkey here, which is the point of ADR-0023 D2: the library is
 * backend-agnostic on purpose, and anything it does on Valkey it does on Flint.
 */
public final class TierSpike {

  static final class TieredObjectClient implements ObjectClient {
    private final ObjectClient origin;
    private final RedisAsyncCommands<byte[], byte[]> tier;
    private final java.util.Map<String, CompletableFuture<byte[]>> inflight =
        new ConcurrentHashMap<>();
    final AtomicInteger hits = new AtomicInteger();
    final AtomicInteger misses = new AtomicInteger();
    final AtomicLong hitNanos = new AtomicLong();
    final AtomicLong missNanos = new AtomicLong();

    TieredObjectClient(ObjectClient origin, RedisAsyncCommands<byte[], byte[]> tier) {
      this.origin = origin;
      this.tier = tier;
    }

    /** D3 + D4: content-addressed by ETag, keyed by absolute range. */
    private static byte[] key(GetRequest r) {
      return ("d/" + r.getEtag() + "/" + r.getRange()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    final AtomicInteger metaHits = new AtomicInteger();
    final AtomicInteger metaMisses = new AtomicInteger();

    /**
     * D3's SECOND level, and it is not an optimization.
     *
     * A first pass without this showed 21 reads producing 1 GET and 21 HEADs:
     * caching data alone left ~95% of the request COUNT on the table, because
     * every fresh reader must still ask what it is reading. HEAD costs the
     * same as GET on S3 ($0.40/M), and short-lived readers -- one factory per
     * task, i.e. the Spark executor pattern -- make HEADs the dominant term.
     *
     * Mutable, so unlike the data keys it carries a bounded TTL rather than
     * being content-addressed.
     */
    @Override
    public CompletableFuture<ObjectMetadata> headObject(HeadRequest r) {
      byte[] mk = ("m/" + r.getS3Uri()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
      return tier.get(mk).toCompletableFuture().thenCompose(v -> {
        if (v != null) {
          metaHits.incrementAndGet();
          String[] parts = new String(v).split("\\|", 2);
          return CompletableFuture.completedFuture(ObjectMetadata.builder()
              .contentLength(Long.parseLong(parts[0])).etag(parts[1]).build());
        }
        metaMisses.incrementAndGet();
        return origin.headObject(r).thenApply(m -> {
          tier.setex(mk, 60, (m.getContentLength() + "|" + m.getEtag())
              .getBytes(java.nio.charset.StandardCharsets.UTF_8));
          return m;
        });
      }).toCompletableFuture();
    }

    @Override
    public CompletableFuture<ObjectContent> getObject(GetRequest r) { return get(r, null); }

    @Override
    public CompletableFuture<ObjectContent> getObject(GetRequest r, StreamContext c) { return get(r, c); }

    private CompletableFuture<ObjectContent> get(GetRequest r, StreamContext ctx) {
      byte[] k = key(r);
      String ks = new String(k);
      long t0 = System.nanoTime();

      // Non-blocking all the way down: Lettuce hands back a CompletionStage,
      // so nothing here ever waits on a thread AAL owns.
      return tier.get(k).toCompletableFuture().thenCompose(cached -> {
        if (cached != null) {
          hits.incrementAndGet();
          hitNanos.addAndGet(System.nanoTime() - t0);
          return CompletableFuture.completedFuture(
              ObjectContent.builder().stream(new ByteArrayInputStream(cached)).build());
        }
        CompletableFuture<byte[]> fetch = inflight.computeIfAbsent(ks, kk -> {
          misses.incrementAndGet();
          CompletableFuture<ObjectContent> up =
              (ctx == null) ? origin.getObject(r) : origin.getObject(r, ctx);
          return up.thenApply(oc -> {
            try (InputStream in = oc.getStream()) {
              byte[] all = in.readAllBytes();
              tier.set(k, all);           // fire-and-forget fill
              return all;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }).whenComplete((v, t) -> inflight.remove(kk));
        });
        return fetch.thenApply(b -> {
          missNanos.addAndGet(System.nanoTime() - t0);
          return ObjectContent.builder().stream(new ByteArrayInputStream(b)).build();
        });
      }).toCompletableFuture();
    }

    @Override
    public void close() throws IOException { origin.close(); }
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String redis = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    String bucket = "bucket", key = "data/000005.bin";
    boolean ok = true;

    RedisClient rc = RedisClient.create(redis);
    StatefulRedisConnection<byte[], byte[]> conn = rc.connect(new ByteArrayCodec());
    conn.sync().flushall();
    System.out.println("[ok] connected to the tier: " + redis);

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("spike", "spike")))
        .forcePathStyle(true).build();

    var cfg = S3SeekableInputStreamConfiguration.DEFAULT;
    try (var sdk = new S3SdkObjectClient(s3)) {
      var client = new TieredObjectClient(sdk, conn.async());

      // cold
      try (var f = new S3SeekableInputStreamFactory(client, cfg)) {
        try (S3SeekableInputStream in = f.createStream(S3URI.of(bucket, key))) {
          byte[] b = new byte[512]; in.seek(2048); in.read(b, 0, 512);
        }
      }
      // warm, fresh factory so AAL's own L1 cannot answer (D12.5)
      int warmReads = 20;
      for (int i = 0; i < warmReads; i++) {
        try (var f = new S3SeekableInputStreamFactory(client, cfg)) {
          try (S3SeekableInputStream in = f.createStream(S3URI.of(bucket, key))) {
            byte[] b = new byte[512]; in.seek(2048); in.read(b, 0, 512);
          }
        }
      }

      boolean served = client.hits.get() >= warmReads;
      ok &= served;
      System.out.printf("[%s] tier served the warm reads (%d hits, %d misses)%n",
          served ? "ok" : "FAIL", client.hits.get(), client.misses.get());

      double hitMs = client.hits.get() == 0 ? -1
          : client.hitNanos.get() / 1e6 / client.hits.get();
      double missMs = client.misses.get() == 0 ? -1
          : client.missNanos.get() / 1e6 / client.misses.get();
      System.out.printf("     mean tier hit  %.3f ms   (over %d)%n", hitMs, client.hits.get());
      System.out.printf("     mean origin miss %.3f ms (over %d)%n", missMs, client.misses.get());

      boolean faster = hitMs > 0 && missMs > 0 && hitMs < missMs;
      ok &= faster;
      System.out.printf("[%s] a tier hit beats going to the origin (%.1fx)%n",
          faster ? "ok" : "FAIL", missMs / Math.max(hitMs, 1e-9));

      long n = conn.sync().dbsize();
      boolean stored = n > 0;
      ok &= stored;
      System.out.printf("[%s] the tier actually holds the bytes (%d key(s))%n",
          stored ? "ok" : "FAIL", n);

      boolean metaServed = client.metaHits.get() >= warmReads;
      ok &= metaServed;
      System.out.printf("[%s] metadata served from the tier too, so warm readers "
          + "issue no HEAD (%d hits, %d misses)%n",
          metaServed ? "ok" : "FAIL", client.metaHits.get(), client.metaMisses.get());
    }

    conn.close(); rc.shutdown();
    System.out.println("\nTIER SPIKE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
