// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;
import java.util.Map;
import java.util.function.Function;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;

/**
 * Builds the tier stack once, for all three adoption paths.
 *
 * ADR-0023 D12.22 leaves us with three entry points into three different
 * ecosystems -- S3A's custom stream type, S3A's fs.s3a.impl, and Iceberg's
 * io-impl -- and exactly one thing to do behind each. Sharing the construction
 * is not tidying: three copies of this would drift, and the D12.12 lesson was
 * that a fill path which quietly does nothing passes every correctness test.
 */
public final class TierSupport {

  public final RedisClient redis;
  /**
   * NULL when the tier was unreachable at build time (BUG-0058). Production
   * paths go through {@link #caching}, {@link #bypass} and {@link #client} and
   * never touch this; the suites that do read it run against a live tier. Ask
   * {@link #liveConnection()} when you need the connection whatever the state.
   */
  public final StatefulRedisConnection<byte[], byte[]> conn;
  public final S3AsyncClient s3;
  public final FlintObjectClient client;
  public final S3SeekableInputStreamFactory caching;
  public final S3SeekableInputStreamFactory bypass;

  /**
   * Registered for the CACHING client only.
   *
   * The bypass client exists to serve SSE-C reads straight from S3 and has no
   * cache behaviour to report; a second bean beside the first would double
   * every number an operator reads at a glance. Null when JMX is unavailable,
   * which is a reason to have no metrics and never a reason to fail a read.
   */
  public FlintCacheMetrics metrics;

  /** Non-null only when the tier was down at build time. Package-visible for
   *  the resilience suite, which asserts the retry is rate-limited. */
  public LazyTierCommands lazy;

  private TierSupport(RedisClient r, StatefulRedisConnection<byte[], byte[]> c,
                      S3AsyncClient s3, FlintObjectClient cl,
                      S3SeekableInputStreamFactory ca, S3SeekableInputStreamFactory by) {
    this.redis = r; this.conn = c; this.s3 = s3; this.client = cl;
    this.caching = ca; this.bypass = by;
  }

  /** `get` reads a config key; `def` supplies fallbacks. Works for Hadoop
   *  Configuration and for Iceberg's property Map alike. */
  public static TierSupport build(Function<String, String> get) {
    String uri = or(get.apply("flint.tier.uri"), "redis://127.0.0.1:6379");
    // 64 KiB - 128, not a power of two. 65,536 is one byte past jemalloc's
    // 64 KiB size class, so a bare chunk takes the 80 KiB class and costs
    // 1.25x its own bytes; the seal is not the cause. Measured end to end:
    // -19.5% tier memory on a full object, -17.7% on scattered reads. MUST
    // match the python client's CHUNK -- the two share one grid.
    int chunk = (int) num(get.apply("flint.chunk.bytes"), FlintObjectClient.DEFAULT_CHUNK_BYTES);
    long budget = num(get.apply("flint.tier.budget.ms"), 50);
    long ttl = num(get.apply("flint.meta.ttl.seconds"), 60);

    RedisClient redis = RedisClient.create(uri);
    redis.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .cancelCommandsOnReconnectFailure(true).autoReconnect(true).build());

    // BUG-0058: a tier that is ALREADY down must not stop the client being
    // built. `connect()` performs the initial connect and throws when the
    // endpoint refuses; that exception used to escape all the way out of
    // FlintS3AFileSystem.initialize, leaving Hadoop with no FileSystem to
    // return, and a Spark job whose tier was down at submission time failed
    // outright rather than reading from S3. ADR-0023 D12.9 calls the opposite
    // of that the property that decides deployability, and its protections all
    // sit on the read path -- they cover a tier that dies after a working
    // connection exists, not one that was never reachable.
    //
    // On failure we hand FlintObjectClient a handle of the same type whose
    // every call throws until the tier answers. That is a RuntimeException
    // arriving where `guard` already catches one inline and degrades to origin,
    // so the outage takes the path that was already tested rather than a new one.
    long retryMs = num(get.apply("flint.tier.reconnect.ms"), 5_000);
    StatefulRedisConnection<byte[], byte[]> conn;
    RedisAsyncCommands<byte[], byte[]> async;
    LazyTierCommands lazy = null;
    try {
      conn = redis.connect(new ByteArrayCodec());
      async = conn.async();
    } catch (RuntimeException down) {
      lazy = LazyTierCommands.install(redis, retryMs);
      async = lazy.commands();
      conn = null;
    }

    S3AsyncClient s3 = asyncClient(get);
    // ADR-0023 D13.3: SSE-KMS bypasses the cache unless the customer turns it
    // on, having decided that losing the KMS grant as the access gate and
    // losing the CloudTrail decrypt record are acceptable for their data.
    // Default false, and it must stay false -- a default that silently caches
    // KMS plaintext is the version of this that ends a security review.
    boolean cacheKms = "true".equalsIgnoreCase(
        or(get.apply("flint.cache.sse-kms"), "false"));
    // The ENGINE knows things the cache cannot infer. Iceberg's format
    // guarantees every file it reads is write-once, so revalidating its
    // metadata every 60s buys protection against a change the format forbids.
    // Off by default: an arbitrary s3a:// path carries no such guarantee.
    boolean immutable = "true".equalsIgnoreCase(
        or(get.apply("flint.immutable"), "false"));
    long immTtl = num(get.apply("flint.meta.ttl.immutable.seconds"), 86_400);
    // Capacity admission (D17). Objects above this are read from the origin
    // and never chunk-cached, bounding the keyspace one object can occupy as
    // much as the bytes.
    long maxObj = num(get.apply("flint.max.object.bytes"),
        FlintObjectClient.DEFAULT_MAX_OBJECT_BYTES);
    var cfg = S3SeekableInputStreamConfiguration.DEFAULT;
    FlintObjectClient cl =
        new FlintObjectClient(new S3SdkObjectClient(s3, false), async,
            chunk, budget, ttl, false, s3, cacheKms, immutable, immTtl, maxObj);
    TierSupport t = new TierSupport(redis, conn, s3, cl,
        new S3SeekableInputStreamFactory(cl, cfg),
        new S3SeekableInputStreamFactory(
            new FlintObjectClient(new S3SdkObjectClient(s3, false), async,
                chunk, budget, ttl, true, s3, cacheKms, immutable, immTtl, maxObj), cfg));
    t.lazy = lazy;
    t.metrics = FlintCacheMetrics.register(cl, uri);
    return t;
  }

  private static S3AsyncClient asyncClient(Function<String, String> get) {
    var b = S3AsyncClient.builder()
        .region(Region.of(or(get.apply("s3.region"), "us-east-1")));
    String ep = or(get.apply("s3.endpoint"), "");
    if (!ep.isEmpty()) {
      b.endpointOverride(URI.create(ep.startsWith("http") ? ep : "https://" + ep));
    }
    if ("true".equalsIgnoreCase(or(get.apply("s3.path-style-access"), "false"))) {
      b.forcePathStyle(true);
    }
    String ak = or(get.apply("s3.access-key-id"), "");
    String sk = or(get.apply("s3.secret-access-key"), "");
    b.credentialsProvider(ak.isEmpty()
        ? DefaultCredentialsProvider.create()
        : StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)));
    return b.build();
  }

  /** Iceberg hands properties as a Map; Hadoop as a Configuration. */
  public static Function<String, String> from(Map<String, String> props) {
    return props::get;
  }

  private static String or(String v, String d) { return v == null || v.isEmpty() ? d : v; }
  private static long num(String v, long d) {
    try { return v == null || v.isEmpty() ? d : Long.parseLong(v.trim()); }
    catch (NumberFormatException e) { return d; }
  }

  /** The live connection whichever way it was obtained, or null if never up. */
  public StatefulRedisConnection<byte[], byte[]> liveConnection() {
    return conn != null ? conn : (lazy != null ? lazy.connection() : null);
  }

  public void close() {
    if (metrics != null) metrics.unregister();
    try { caching.close(); } catch (Exception ignored) { }
    try { bypass.close(); } catch (Exception ignored) { }
    // conn is null when the tier was never reachable; the lazy handle may have
    // connected since, and that connection is the one that needs closing.
    try { if (conn != null) conn.close(); } catch (Exception ignored) { }
    try {
      if (lazy != null && lazy.connection() != null) lazy.connection().close();
    } catch (Exception ignored) { }
    try { redis.shutdown(); } catch (Exception ignored) { }
    try { s3.close(); } catch (Exception ignored) { }
  }
}
