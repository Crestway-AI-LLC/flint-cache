// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.*;
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
 * D3's staleness contract, which has never been checked.
 *
 * The claim: data keys are content-addressed by ETag, so a mutated object is
 * self-invalidating -- its new ETag simply misses. Metadata is mutable and
 * therefore carries a bounded TTL, and THAT TTL is the entire staleness
 * window we promise.
 *
 * Two things need proving, and the second matters more:
 *
 *   1. Within the TTL a reader sees the old object; after it, the new one.
 *      Stale, but that is the declared contract.
 *   2. A read is NEVER TORN. Content-addressing is supposed to make it
 *      impossible to assemble one response from a mix of generations. A torn
 *      read would be silent corruption -- bytes that never existed as an
 *      object -- which is far worse than staleness and would not be caught by
 *      any hit-rate or latency measurement.
 */
public final class MutationSpike {

  static final int CHUNK = 64 * 1024;
  static final long META_TTL_SEC = 2;      // short, so the test is not slow
  static final HttpClient HTTP = HttpClient.newHttpClient();
  static String endpoint;

  static final class VersionedClient implements ObjectClient {
    private final ObjectClient origin;
    private final RedisAsyncCommands<byte[], byte[]> tier;
    final AtomicInteger metaHits = new AtomicInteger();
    final Set<String> etagsSeen = ConcurrentHashMap.newKeySet();

    VersionedClient(ObjectClient o, RedisAsyncCommands<byte[], byte[]> t) { origin = o; tier = t; }

    private static byte[] ck(String etag, long id) {
      return ("c/" + etag + "/" + id).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public CompletableFuture<ObjectMetadata> headObject(HeadRequest r) {
      byte[] mk = ("m/" + r.getS3Uri()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
      return tier.get(mk).toCompletableFuture().thenCompose(v -> {
        if (v != null) {
          metaHits.incrementAndGet();
          String[] p = new String(v).split("\\|", 2);
          return CompletableFuture.completedFuture(ObjectMetadata.builder()
              .contentLength(Long.parseLong(p[0])).etag(p[1]).build());
        }
        return origin.headObject(r).thenApply(m -> {
          tier.setex(mk, META_TTL_SEC,
              (m.getContentLength() + "|" + m.getEtag()).getBytes());
          return m;
        });
      }).toCompletableFuture();
    }

    @Override public CompletableFuture<ObjectContent> getObject(GetRequest r) { return get(r, null); }
    @Override public CompletableFuture<ObjectContent> getObject(GetRequest r, StreamContext c) { return get(r, c); }

    private CompletableFuture<ObjectContent> get(GetRequest r, StreamContext ctx) {
      etagsSeen.add(r.getEtag());
      final long start = r.getRange().getStart(), end = r.getRange().getEnd();
      final long first = start / CHUNK, last = end / CHUNK;
      final int n = (int) (last - first + 1);
      final String etag = r.getEtag();
      byte[][] keys = new byte[n][];
      for (int i = 0; i < n; i++) keys[i] = ck(etag, first + i);

      return tier.mget(keys).toCompletableFuture().thenCompose(vals -> {
        byte[][] have = new byte[n][];
        boolean all = true;
        for (int i = 0; i < n; i++) {
          var kv = vals.get(i);
          have[i] = kv.hasValue() ? kv.getValue() : null;
          if (have[i] == null) all = false;
        }
        if (all) return CompletableFuture.completedFuture(assemble(have, first, start, end, n));
        // Fetch on the CHUNK GRID, not AAL's range.
        //
        // The first version fetched AAL's own range and tried to slice it into
        // chunks, guarded by `start == first * CHUNK`. D12.7 established AAL's
        // ranges are anchored to the read offset, so that guard almost never
        // passes and the cache was silently never populated -- it held the
        // metadata key and nothing else. It appeared to work in the resilience
        // spike only because that test happened to read at a 64 KiB-aligned
        // offset. A fill that quietly does nothing is invisible to every
        // correctness assertion.
        long lo = first * CHUNK, hi = last * CHUNK + CHUNK - 1;
        GetRequest aligned = GetRequest.builder()
            .s3Uri(r.getS3Uri()).etag(etag).referrer(r.getReferrer())
            .range(new Range(lo, hi)).build();
        CompletableFuture<ObjectContent> up =
            (ctx == null) ? origin.getObject(aligned) : origin.getObject(aligned, ctx);
        return up.thenApply(oc -> {
          byte[] bytes;
          try (InputStream in = oc.getStream()) { bytes = in.readAllBytes(); }
          catch (IOException e) { throw new RuntimeException(e); }
          byte[][] got = new byte[n][];
          for (int i = 0; i < n; i++) {
            int off = i * CHUNK;
            if (off >= bytes.length) break;
            int len = Math.min(CHUNK, bytes.length - off);
            got[i] = Arrays.copyOfRange(bytes, off, off + len);
            tier.set(ck(etag, first + i), got[i]);
          }
          return assemble(got, first, start, end, n);
        });
      }).toCompletableFuture();
    }

    private static ObjectContent assemble(byte[][] have, long first, long start, long end, int n) {
      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
      for (int i = 0; i < n; i++) {
        if (have[i] == null) continue;
        long cs = (first + i) * CHUNK;
        int from = (int) Math.max(0, start - cs);
        int to = (int) Math.min(have[i].length, end - cs + 1);
        if (to > from) out.write(have[i], from, to - from);
      }
      return ObjectContent.builder().stream(new ByteArrayInputStream(out.toByteArray())).build();
    }

    @Override public void close() throws IOException { origin.close(); }
  }

  static byte[] expected(String key, int gen, long start, int len) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      long abs = start + i, block = abs / 16;
      md.reset();
      out[i] = md.digest((key + ":" + gen + ":" + block).getBytes("UTF-8"))[(int) (abs % 16)];
    }
    return out;
  }

  static int mutate(String key) throws Exception {
    var r = HTTP.send(HttpRequest.newBuilder(
        URI.create(endpoint + "/__mutate?key=" + key)).build(),
        HttpResponse.BodyHandlers.ofString());
    String b = r.body();
    int i = b.indexOf("\"generation\":");
    return Integer.parseInt(b.substring(i + 13, b.indexOf(',', i)).trim());
  }

  /** Which generation, if any, does this buffer consistently belong to? -1 = TORN. */
  static int generationOf(String key, long off, byte[] got) throws Exception {
    for (int g = 0; g <= 3; g++) {
      if (Arrays.equals(got, expected(key, g, off, got.length))) return g;
    }
    return -1;
  }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String redis = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    String bucket = "bucket", key = "data/000003.bin";
    boolean ok = true;

    RedisClient rc = RedisClient.create(redis);
    rc.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS).build());
    var conn = rc.connect(new ByteArrayCodec());
    conn.sync().flushall();

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("spike", "spike")))
        .forcePathStyle(true).build();
    var cfg = S3SeekableInputStreamConfiguration.DEFAULT;

    try (var sdk = new S3SdkObjectClient(s3, false)) {
      var c = new VersionedClient(sdk, conn.async());
      long off = 100_000; int len = 8192;

      byte[] g0 = read(c, cfg, bucket, key, off, len);
      int gen0 = generationOf(key, off, g0);
      ok &= gen0 == 0;
      System.out.printf("[%s] before mutation the read is generation %d%n",
          gen0 == 0 ? "ok" : "FAIL", gen0);

      int newGen = mutate(key);
      System.out.printf("     -- object mutated at the origin, now generation %d --%n", newGen);

      int metaBefore = c.metaHits.get();
      byte[] during = read(c, cfg, bucket, key, off, len);
      System.out.printf("     [diag] metadata cache hits during that read: %d "
          + "(total %d); ETags seen so far: %d%n",
          c.metaHits.get() - metaBefore, c.metaHits.get(), c.etagsSeen.size());
      int genDuring = generationOf(key, off, during);
      boolean notTorn1 = genDuring >= 0;
      ok &= notTorn1;
      System.out.printf("[%s] within the metadata TTL the read is generation %d "
          + "(stale, per contract) and NOT torn%n",
          notTorn1 ? "ok" : "FAIL", genDuring);

      System.out.printf("     -- waiting out the %ds metadata TTL --%n", META_TTL_SEC);
      Thread.sleep(META_TTL_SEC * 1000 + 800);

      byte[] after = read(c, cfg, bucket, key, off, len);
      int genAfter = generationOf(key, off, after);
      boolean fresh = genAfter == newGen;
      ok &= fresh;
      System.out.printf("[%s] after the TTL lapses the read is generation %d "
          + "(the new object)%n", fresh ? "ok" : "FAIL", genAfter);

      boolean everTorn = gen0 < 0 || genDuring < 0 || genAfter < 0;
      ok &= !everTorn;
      System.out.printf("[%s] NO read was ever torn -- every response belonged "
          + "entirely to one generation%n", !everTorn ? "ok" : "FAIL");

      boolean twoEtags = c.etagsSeen.size() >= 2;
      ok &= twoEtags;
      System.out.printf("[%s] armed-check: the client actually saw %d distinct "
          + "ETags, so the mutation really reached it%n",
          twoEtags ? "ok" : "FAIL", c.etagsSeen.size());
    }

    conn.close(); rc.shutdown();
    System.out.println("\nMUTATION SPIKE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }

  static byte[] read(ObjectClient c, S3SeekableInputStreamConfiguration cfg,
                     String bucket, String key, long off, int len) throws IOException {
    try (var f = new S3SeekableInputStreamFactory(c, cfg);
         var in = f.createStream(S3URI.of(bucket, key))) {
      byte[] b = new byte[len];
      in.seek(off);
      in.read(b, 0, len);
      return b;
    }
  }
}
