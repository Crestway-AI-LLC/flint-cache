// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

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

  /** Pool keys, so close() releases exactly what build() took a reference on. */
  String tierKey, s3Key, sig;

  private TierSupport(RedisClient r, StatefulRedisConnection<byte[], byte[]> c,
                      S3AsyncClient s3, FlintObjectClient cl,
                      S3SeekableInputStreamFactory ca, S3SeekableInputStreamFactory by) {
    this.redis = r; this.conn = c; this.s3 = s3; this.client = cl;
    this.caching = ca; this.bypass = by;
  }

  /** `get` reads a config key; `def` supplies fallbacks. Works for Hadoop
   *  Configuration and for Iceberg's property Map alike. */
  /** Every key {@link #create} consults. The signature is built from these, so
   *  a key missing here would let two DIFFERENTLY configured mounts share one
   *  instance -- silently, which is the same shape as the allowlist-by-case-
   *  label bug this file already carried once. {@link #create} records every
   *  key it actually reads and throws if one is not listed, so the list cannot
   *  drift away from the code without saying so. */
  private static final String[] CONFIG_KEYS = {
    "flint.tier.uri", "flint.chunk.bytes", "flint.tier.budget.ms",
    "flint.meta.ttl.seconds", "flint.tier.reconnect.ms", "flint.cache.sse-kms",
    "flint.immutable", "flint.meta.ttl.immutable.seconds",
    "flint.max.object.bytes", "s3.endpoint", "s3.region",
    "s3.path-style-access", "s3.access-key-id", "s3.secret-access-key",
  };

  private static final class Ref { final TierSupport v; int n; Ref(TierSupport t) { v = t; } }
  private static final Map<String, Ref> POOL = new HashMap<>();

  /** Observable so a test can prove reuse rather than infer it from threads. */
  public static final java.util.concurrent.atomic.AtomicLong created =
      new java.util.concurrent.atomic.AtomicLong();
  public static final java.util.concurrent.atomic.AtomicLong reused =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * BUG-0057. Under {@code fs.s3a.impl.disable.cache=true} Hadoop builds a
   * FileSystem per {@code get()} and Spark never closes the ones it opens for
   * {@code read.parquet}, so this ran per read rather than per bucket. Measured
   * before pooling: <b>+4 threads per instance, +48 for twelve</b>, until the
   * JVM could not create another Netty event loop group.
   *
   * <p>Identically configured mounts now share one instance, reference-counted;
   * differently configured ones do not, because the settings below change what
   * a read DOES and quietly merging two of them would be a correctness bug
   * rather than a saving.
   */
  public static synchronized TierSupport build(Function<String, String> get) {
    String sig = signature(get);
    Ref r = POOL.get(sig);
    if (r == null) {
      r = new Ref(create(get, sig));
      POOL.put(sig, r);
      created.incrementAndGet();
    } else {
      reused.incrementAndGet();
    }
    r.n++;
    return r.v;
  }

  private static String signature(Function<String, String> get) {
    StringBuilder b = new StringBuilder();
    for (String k : CONFIG_KEYS) {
      String v = get.apply(k);
      b.append(k).append('=').append(v == null ? "" : v).append('\n');
    }
    return b.toString();
  }

  private static TierSupport create(Function<String, String> outer, String sig) {
    java.util.Set<String> seen = new java.util.HashSet<>();
    Function<String, String> get = k -> { seen.add(k); return outer.apply(k); };
    TierSupport t = build0(get);
    java.util.Set<String> known = new java.util.HashSet<>(java.util.Arrays.asList(CONFIG_KEYS));
    seen.removeAll(known);
    if (!seen.isEmpty()) {
      throw new IllegalStateException(
          "TierSupport.create read configuration keys that are not in "
          + "CONFIG_KEYS: " + seen + ". They would not be part of the pool key, "
          + "so two mounts differing only in those would silently share one "
          + "instance. Add them to CONFIG_KEYS.");
    }
    t.sig = sig;
    return t;
  }

  private static TierSupport build0(Function<String, String> get) {
    String uri = or(get.apply("flint.tier.uri"), "redis://127.0.0.1:6379");
    // 64 KiB - 128, not a power of two. 65,536 is one byte past jemalloc's
    // 64 KiB size class, so a bare chunk takes the 80 KiB class and costs
    // 1.25x its own bytes; the seal is not the cause. Measured end to end:
    // -19.5% tier memory on a full object, -17.7% on scattered reads. MUST
    // match the python client's CHUNK -- the two share one grid.
    int chunk = (int) num(get.apply("flint.chunk.bytes"), FlintObjectClient.DEFAULT_CHUNK_BYTES);
    long budget = num(get.apply("flint.tier.budget.ms"), 50);
    long ttl = num(get.apply("flint.meta.ttl.seconds"), 60);

    // BUG-0057 and BUG-0058 both live in TierConnections now. The connection
    // objects are POOLED and reference-counted, because under
    // fs.s3a.impl.disable.cache=true Hadoop builds a FileSystem per get() and
    // Spark never closes them -- measured at +4 threads per client, +48 for
    // twelve, until the JVM could not create another event loop. And a tier
    // that is already down installs a lazy handle rather than throwing, so a
    // cache outage does not take the job with it.
    long retryMs = num(get.apply("flint.tier.reconnect.ms"), 5_000);
    TierConnections.Redis tier = TierConnections.acquireRedis(uri, retryMs);
    StatefulRedisConnection<byte[], byte[]> conn = tier.conn;
    RedisAsyncCommands<byte[], byte[]> async = tier.commands;
    RedisClient redis = tier.client;

    String s3key = TierConnections.s3Key(get.apply("s3.endpoint"),
        or(get.apply("s3.region"), "us-east-1"), get.apply("s3.path-style-access"),
        get.apply("s3.access-key-id"), get.apply("s3.secret-access-key"));
    S3AsyncClient s3 = TierConnections.acquireS3(s3key, () -> asyncClient(get));

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
    // ONE origin client, not two. The caching and bypass paths were each
    // building `new S3SdkObjectClient(s3, false)` from identical arguments, and
    // AAL starts a scheduled executor behind each -- so every mount paid for two
    // where one will do. `false` means it does not own the underlying
    // S3AsyncClient, which is now pooled and must outlive both.
    S3SdkObjectClient origin = new S3SdkObjectClient(s3, false);
    FlintObjectClient cl =
        new FlintObjectClient(origin, async,
            chunk, budget, ttl, false, s3, cacheKms, immutable, immTtl, maxObj);
    TierSupport t = new TierSupport(redis, conn, s3, cl,
        new S3SeekableInputStreamFactory(cl, cfg),
        new S3SeekableInputStreamFactory(
            new FlintObjectClient(origin, async,
                chunk, budget, ttl, true, s3, cacheKms, immutable, immTtl, maxObj), cfg));
    t.lazy = tier.lazy;
    t.tierKey = uri;
    t.s3Key = s3key;
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

  /**
   * Drops one reference. The last one out closes the factories and releases the
   * pooled connections; earlier ones must not, or a mount that finished would
   * take the tier away from the mounts still reading through it.
   */
  public void close() {
    synchronized (TierSupport.class) {
      Ref r = sig == null ? null : POOL.get(sig);
      if (r != null) {
        if (--r.n > 0) return;
        POOL.remove(sig);
      }
    }
    if (metrics != null) metrics.unregister();
    try { caching.close(); } catch (Exception ignored) { }
    try { bypass.close(); } catch (Exception ignored) { }
    if (tierKey != null) TierConnections.releaseRedis(tierKey);
    if (s3Key != null) TierConnections.releaseS3(s3Key);
  }
}
