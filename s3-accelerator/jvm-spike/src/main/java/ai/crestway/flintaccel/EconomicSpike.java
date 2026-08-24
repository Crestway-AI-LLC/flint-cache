// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
 * The economic asserts, proven structurally at the layer the product runs at.
 *
 * The cache here is a HashMap, not Flint. That is deliberate: what is under
 * test is whether the SHAPE holds — whether sitting at ObjectClient actually
 * deduplicates misses, whether a warm pass costs nothing, whether one shared
 * tier serves two clients from one dataset's worth of transfer. Those are
 * properties of where we sit, not of what we store in.
 *
 * The trap this exists to expose: AAL has its OWN in-process cache. A warm
 * pass measured through one factory may show zero S3 traffic because AAL
 * answered it, not because we did. Phase 0 demonstrates that directly, so no
 * later measurement can be read the wrong way.
 */
public final class EconomicSpike {

  static String endpoint;
  static final HttpClient HTTP = HttpClient.newHttpClient();

  /** Toy shared tier: content-addressed by (etag, range), with single-flight. */
  static final class CachingObjectClient implements ObjectClient {
    private final ObjectClient delegate;
    private final Map<String, byte[]> cache;
    private final Map<String, CompletableFuture<byte[]>> inflight = new ConcurrentHashMap<>();
    final AtomicInteger hits = new AtomicInteger();
    final AtomicInteger misses = new AtomicInteger();
    final AtomicInteger coalesced = new AtomicInteger();

    CachingObjectClient(ObjectClient delegate, Map<String, byte[]> sharedCache) {
      this.delegate = delegate;
      this.cache = sharedCache;
    }

    private static String key(GetRequest r) {
      return r.getEtag() + ":" + r.getRange();   // D3 + D4, in one line
    }

    @Override
    public CompletableFuture<ObjectMetadata> headObject(HeadRequest r) {
      return delegate.headObject(r);
    }

    @Override
    public CompletableFuture<ObjectContent> getObject(GetRequest r) {
      return get(r, null);
    }

    @Override
    public CompletableFuture<ObjectContent> getObject(GetRequest r, StreamContext ctx) {
      return get(r, ctx);
    }

    private CompletableFuture<ObjectContent> get(GetRequest r, StreamContext ctx) {
      String k = key(r);
      byte[] cached = cache.get(k);
      if (cached != null) {
        hits.incrementAndGet();
        return CompletableFuture.completedFuture(
            ObjectContent.builder().stream(new ByteArrayInputStream(cached)).build());
      }
      // D5 single-flight: concurrent misses on one key produce ONE fetch.
      boolean[] iAmLeader = {false};
      CompletableFuture<byte[]> fetch = inflight.computeIfAbsent(k, kk -> {
        iAmLeader[0] = true;
        misses.incrementAndGet();
        CompletableFuture<ObjectContent> up =
            (ctx == null) ? delegate.getObject(r) : delegate.getObject(r, ctx);
        return up.thenApply(oc -> {
          try (InputStream in = oc.getStream()) {
            byte[] all = in.readAllBytes();
            cache.put(kk, all);
            return all;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).whenComplete((v, t) -> inflight.remove(kk));
      });
      if (!iAmLeader[0]) {
        coalesced.incrementAndGet();
      }
      return fetch.thenApply(
          b -> ObjectContent.builder().stream(new ByteArrayInputStream(b)).build());
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  static S3AsyncClient s3() {
    return S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("spike", "spike")))
        .forcePathStyle(true)
        .build();
  }

  static int endpointGets() throws Exception {
    HttpResponse<String> r = HTTP.send(
        HttpRequest.newBuilder(URI.create(endpoint + "/__stats")).build(),
        HttpResponse.BodyHandlers.ofString());
    String b = r.body();
    int i = b.indexOf("\"gets\":");
    return Integer.parseInt(b.substring(i + 7, b.indexOf(',', i)).trim());
  }

  static void reset() throws Exception {
    HTTP.send(HttpRequest.newBuilder(URI.create(endpoint + "/__reset")).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  static void readOnce(S3SeekableInputStreamFactory f, String bucket, String key) throws IOException {
    try (S3SeekableInputStream in = f.createStream(S3URI.of(bucket, key))) {
      byte[] buf = new byte[512];
      in.seek(2048);
      in.read(buf, 0, 512);
    }
  }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String bucket = "bucket", key = "data/000002.bin";
    boolean ok = true;

    Map<String, byte[]> shared = new ConcurrentHashMap<>();
    var cfg = S3SeekableInputStreamConfiguration.DEFAULT;

    // ---- Phase 0: the trap. AAL's own L1 can absorb a warm pass. ----
    reset();
    try (var sdk = new S3SdkObjectClient(s3())) {
      var counting = new SeamSpike.CountingObjectClient(sdk);
      try (var f = new S3SeekableInputStreamFactory(counting, cfg)) {
        readOnce(f, bucket, key);
        int afterFirst = counting.gets.get();
        readOnce(f, bucket, key);
        int afterSecond = counting.gets.get();
        boolean absorbed = afterSecond == afterFirst;
        System.out.printf("[%s] TRAP: through ONE factory, AAL's own cache absorbed the "
            + "second read (our client called %d then %d)%n",
            absorbed ? "!!" : "ok", afterFirst, afterSecond);
        System.out.println("     -> a warm-pass measurement on a shared factory proves "
            + "nothing about OUR cache. Every phase below uses a fresh factory.");
      }
    }

    // ---- Phase 1: warm second pass, fresh factory -> 0 new S3 GETs ----
    reset();
    int g0, g1, g2;
    try (var sdk = new S3SdkObjectClient(s3())) {
      var c = new CachingObjectClient(sdk, shared);
      try (var f = new S3SeekableInputStreamFactory(c, cfg)) { readOnce(f, bucket, key); }
      g1 = endpointGets();
      try (var f = new S3SeekableInputStreamFactory(c, cfg)) { readOnce(f, bucket, key); }
      g2 = endpointGets();
      boolean warmFree = g2 == g1 && g1 > 0 && c.hits.get() > 0;
      ok &= warmFree;
      System.out.printf("[%s] warm second pass costs 0 S3 GETs (endpoint %d -> %d, "
          + "cache hits %d)%n", warmFree ? "ok" : "FAIL", g1, g2, c.hits.get());
    }

    // ---- Phase 2: two independent clients, one shared tier -> 1x dataset ----
    reset();
    try (var sdkA = new S3SdkObjectClient(s3()); var sdkB = new S3SdkObjectClient(s3())) {
      var a = new CachingObjectClient(sdkA, shared);
      var b = new CachingObjectClient(sdkB, shared);   // SAME shared map
      shared.clear();
      try (var f = new S3SeekableInputStreamFactory(a, cfg)) { readOnce(f, bucket, key); }
      int afterA = endpointGets();
      try (var f = new S3SeekableInputStreamFactory(b, cfg)) { readOnce(f, bucket, key); }
      int afterB = endpointGets();
      boolean shared1x = afterB == afterA && b.hits.get() > 0;
      ok &= shared1x;
      System.out.printf("[%s] second CLIENT transfers 1x, not 2x (endpoint %d -> %d, "
          + "client B hits %d)%n", shared1x ? "ok" : "FAIL", afterA, afterB, b.hits.get());
    }

    // ---- Phase 3: N concurrent cold readers -> single-flight ----
    reset();
    shared.clear();
    final int N = 16;
    try (var sdk = new S3SdkObjectClient(s3())) {
      var c = new CachingObjectClient(sdk, shared);
      ExecutorService pool = Executors.newFixedThreadPool(N);
      var tasks = new java.util.ArrayList<CompletableFuture<Void>>();
      for (int i = 0; i < N; i++) {
        tasks.add(CompletableFuture.runAsync(() -> {
          try (var f = new S3SeekableInputStreamFactory(c, cfg)) { readOnce(f, bucket, key); }
          catch (Exception e) { throw new RuntimeException(e); }
        }, pool));
      }
      CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
      pool.shutdown();
      int gets = endpointGets();
      boolean dedup = gets < N;
      ok &= dedup;
      System.out.printf("[%s] %d concurrent cold readers caused %d S3 GET(s) "
          + "(misses %d, coalesced %d)%n", dedup ? "ok" : "FAIL", N, gets,
          c.misses.get(), c.coalesced.get());
    }

    // ---- Negative control: no cache at all must NOT show these properties ----
    reset();
    try (var sdk = new S3SdkObjectClient(s3())) {
      var counting = new SeamSpike.CountingObjectClient(sdk);
      try (var f = new S3SeekableInputStreamFactory(counting, cfg)) { readOnce(f, bucket, key); }
      int n1 = endpointGets();
      try (var f = new S3SeekableInputStreamFactory(counting, cfg)) { readOnce(f, bucket, key); }
      int n2 = endpointGets();
      boolean control = n2 > n1;
      ok &= control;
      System.out.printf("[%s] negative control -- WITHOUT a cache the second pass "
          + "DOES hit S3 (%d -> %d)%n", control ? "ok" : "FAIL", n1, n2);
    }

    System.out.println("\nECONOMIC SPIKE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
