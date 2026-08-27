// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Reference-counted connections, shared across FileSystem instances.
 *
 * <p>BUG-0057. Hadoop keys {@code FileSystem.CACHE} on scheme+authority+UGI, so
 * {@code initialize()} normally runs once per bucket. Under
 * {@code fs.s3a.impl.disable.cache=true} it runs per {@code FileSystem.get},
 * the caller owns the instance, and Spark does not close the ones it opens for
 * {@code read.parquet}. Every instance built its own {@code RedisClient} — a
 * Netty event loop group with a thread per core — and its own
 * {@code S3AsyncClient}. Measured under Spark 4.0.4: after about 30 table
 * reads, {@code IllegalStateException: failed to create a child event loop}.
 *
 * <p>Measured directly, before this class existed: <b>+4 threads per client,
 * +48 for twelve</b>, split about evenly between Lettuce and the AWS SDK.
 * Sharing only the tier client would have halved the slope and left it linear,
 * which is why both are pooled here and not just the one the stack trace named.
 *
 * <p><b>What is NOT shared, deliberately.</b> Only the two connection objects.
 * {@code FlintObjectClient} and the AAL factories stay per-instance, because
 * they carry per-mount settings — chunk size, TTLs, the immutable and SSE-KMS
 * opt-ins, the object cap — and two mounts configured differently must not
 * quietly become one. Sharing the whole stack would be smaller code and a
 * correctness bug.
 *
 * <p><b>Why credentials are in the S3 key.</b> An {@code S3AsyncClient} carries
 * its credentials provider. Keying on endpoint and region alone would hand one
 * tenant's client to another tenant's mount — a cache that returns the wrong
 * identity is worse than a cache that returns nothing. The secret is hashed
 * into the key rather than stored in it.
 *
 * <p>Reference counts, not weak references: a caller that closes must be able
 * to rely on the connection surviving for the callers that have not, and the
 * last one out has to actually shut the event loops down or this fixes nothing.
 */
public final class TierConnections {

  private TierConnections() {}

  /** Observable so a test can prove reuse happened rather than infer it. */
  public static final AtomicLong redisCreated = new AtomicLong();
  public static final AtomicLong redisReused = new AtomicLong();
  public static final AtomicLong s3Created = new AtomicLong();
  public static final AtomicLong s3Reused = new AtomicLong();

  /** The tier connection, however it was obtained. */
  public static final class Redis {
    public final RedisClient client;
    /** Null when the tier was unreachable at build time (BUG-0058). */
    public final StatefulRedisConnection<byte[], byte[]> conn;
    /** Non-null only in that case. */
    public final LazyTierCommands lazy;
    public final RedisAsyncCommands<byte[], byte[]> commands;

    Redis(RedisClient c, StatefulRedisConnection<byte[], byte[]> s,
          LazyTierCommands l, RedisAsyncCommands<byte[], byte[]> a) {
      this.client = c; this.conn = s; this.lazy = l; this.commands = a;
    }

    /** The live connection whichever way it was obtained, or null if never up. */
    public StatefulRedisConnection<byte[], byte[]> live() {
      return conn != null ? conn : (lazy != null ? lazy.connection() : null);
    }
  }

  private static final class Ref<T> {
    final T value; int count;
    Ref(T v) { this.value = v; }
  }

  private static final Map<String, Ref<Redis>> REDIS = new HashMap<>();
  private static final Map<String, Ref<S3AsyncClient>> S3 = new HashMap<>();

  public static synchronized Redis acquireRedis(String uri, long retryMs) {
    Ref<Redis> r = REDIS.get(uri);
    if (r == null) {
      r = new Ref<>(openRedis(uri, retryMs));
      REDIS.put(uri, r);
      redisCreated.incrementAndGet();
    } else {
      // The first acquirer's reconnect interval wins. Two mounts pointing at
      // one tier with different intervals is not worth a second event loop.
      redisReused.incrementAndGet();
    }
    r.count++;
    return r.value;
  }

  public static synchronized void releaseRedis(String uri) {
    Ref<Redis> r = REDIS.get(uri);
    if (r == null || --r.count > 0) return;
    REDIS.remove(uri);
    Redis v = r.value;
    try { if (v.live() != null) v.live().close(); } catch (Exception ignored) { }
    try { v.client.shutdown(); } catch (Exception ignored) { }
  }

  public static synchronized S3AsyncClient acquireS3(String key,
                                                     Supplier<S3AsyncClient> make) {
    Ref<S3AsyncClient> r = S3.get(key);
    if (r == null) {
      r = new Ref<>(make.get());
      S3.put(key, r);
      s3Created.incrementAndGet();
    } else {
      s3Reused.incrementAndGet();
    }
    r.count++;
    return r.value;
  }

  public static synchronized void releaseS3(String key) {
    Ref<S3AsyncClient> r = S3.get(key);
    if (r == null || --r.count > 0) return;
    S3.remove(key);
    try { r.value.close(); } catch (Exception ignored) { }
  }

  private static Redis openRedis(String uri, long retryMs) {
    RedisClient redis = RedisClient.create(uri);
    redis.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .cancelCommandsOnReconnectFailure(true).autoReconnect(true).build());
    try {
      StatefulRedisConnection<byte[], byte[]> c = redis.connect(new ByteArrayCodec());
      return new Redis(redis, c, null, c.async());
    } catch (RuntimeException down) {
      // BUG-0058: a tier already down must not stop the client being built.
      LazyTierCommands lazy = LazyTierCommands.install(redis, retryMs);
      return new Redis(redis, null, lazy, lazy.commands());
    }
  }

  /** Everything that makes two S3 clients genuinely different. */
  public static String s3Key(String endpoint, String region, String pathStyle,
                             String accessKey, String secretKey) {
    return String.join("|", n(endpoint), n(region), n(pathStyle), n(accessKey),
        secretKey == null || secretKey.isEmpty() ? "-" : sha256(secretKey));
  }

  private static String n(String s) { return s == null ? "" : s; }

  private static String sha256(String s) {
    try {
      byte[] d = MessageDigest.getInstance("SHA-256")
          .digest(s.getBytes(StandardCharsets.UTF_8));
      StringBuilder b = new StringBuilder(16);
      for (int i = 0; i < 8; i++) b.append(String.format("%02x", d[i]));
      return b.toString();
    } catch (Exception e) {
      // Never fall back to the plaintext secret as a key.
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  /** Test seam: how many pooled entries are live right now. */
  public static synchronized int pooled() { return REDIS.size() + S3.size(); }
}
