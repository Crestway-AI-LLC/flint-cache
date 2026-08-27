// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

/**
 * A tier handle that survives a tier which is down when the client is built.
 *
 * <p>BUG-0058. {@code RedisClient.connect()} performs the initial connect and
 * throws when the endpoint refuses, so {@code TierSupport.build} could not
 * complete and {@code FlintS3AFileSystem.initialize} threw
 * {@code RedisConnectionException} out to Hadoop, which then had no FileSystem
 * to return. Measured under Spark with the tier stopped: not one query ran.
 * ADR-0023 D12.9 calls the opposite of that "the property that decides
 * deployability" — <b>a cache that can take down the job it accelerates is
 * strictly worse than no cache.</b>
 *
 * <p>D12.9's existing protections all sit on the read path — {@code
 * REJECT_COMMANDS} so a disconnected client fails instead of queueing, and a
 * latency budget around each lookup. Those cover a tier that dies <i>after</i>
 * a working connection exists. Nothing covered a tier that was never reachable,
 * because the connect happened before there was a read to fall through from.
 *
 * <p>So: hand {@code FlintObjectClient} a handle that is already the shape it
 * expects, and let the failure arrive where the fall-through already lives.
 * Every call while disconnected throws {@link RedisConnectionException}, which
 * is a {@code RuntimeException}, which is precisely what
 * {@code FlintObjectClient.guard} already catches inline and degrades to origin
 * on ("already-dead connection throws inline"). No new degradation path — the
 * failure is simply routed into the one that was already tested.
 *
 * <p><b>Why the retry is rate-limited.</b> A connect attempt per read against a
 * dead tier is a TCP handshake per read, which is slower than having no cache.
 * The circuit breaker in {@code FlintObjectClient} already skips tier calls once
 * failures accumulate, so this is the second line rather than the first, but the
 * two protect different windows: the breaker is closed again the moment it
 * half-opens, and that is exactly when a dead endpoint would be dialled.
 *
 * <p>The interface is proxied rather than reimplemented because
 * {@code RedisAsyncCommands} has several hundred methods and every one of them
 * must fail identically. An enumerated subset would work until the first call to
 * a method nobody thought to list, which is the failure mode that made
 * {@code TierSupport}'s allowlist-by-case-label a bug once already.
 */
public final class LazyTierCommands implements InvocationHandler {

  private final RedisClient redis;
  private final long retryNanos;

  private volatile RedisAsyncCommands<byte[], byte[]> delegate;
  private volatile StatefulRedisConnection<byte[], byte[]> connection;
  private long nextAttemptNanos;                 // guarded by this

  /** Observable so a test can prove the rate limit is doing something. */
  public final AtomicLong connectAttempts = new AtomicLong();
  public final AtomicLong rejectedWhileDown = new AtomicLong();

  private LazyTierCommands(RedisClient redis, long retryMillis) {
    this.redis = redis;
    this.retryNanos = retryMillis * 1_000_000L;
    this.nextAttemptNanos = System.nanoTime();   // first call may dial at once
  }

  @SuppressWarnings("unchecked")
  public static LazyTierCommands install(RedisClient redis, long retryMillis) {
    return new LazyTierCommands(redis, retryMillis);
  }

  @SuppressWarnings("unchecked")
  public RedisAsyncCommands<byte[], byte[]> commands() {
    return (RedisAsyncCommands<byte[], byte[]>) Proxy.newProxyInstance(
        RedisAsyncCommands.class.getClassLoader(),
        new Class<?>[] { RedisAsyncCommands.class }, this);
  }

  /** Null until a connect has succeeded; TierSupport.close must tolerate that. */
  public StatefulRedisConnection<byte[], byte[]> connection() { return connection; }

  @Override
  public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
    // Object's own methods must never dial. toString() on a tier handle is
    // something a logger does, and a log line is not a reason to open a socket.
    if (m.getDeclaringClass() == Object.class) {
      switch (m.getName()) {
        case "toString": return "LazyTierCommands[connected=" + (delegate != null) + "]";
        case "hashCode": return System.identityHashCode(proxy);
        case "equals":   return proxy == args[0];
        default: break;
      }
    }
    RedisAsyncCommands<byte[], byte[]> d = delegate;
    if (d == null) {
      // close() on a handle that never connected is a no-op, not an error, or
      // shutdown would throw for the very deployments this exists to support.
      if ("close".equals(m.getName()) && (args == null || args.length == 0)) return null;
      d = connect();
    }
    try {
      return m.invoke(d, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private synchronized RedisAsyncCommands<byte[], byte[]> connect() {
    RedisAsyncCommands<byte[], byte[]> d = delegate;
    if (d != null) return d;                      // another thread got there
    long now = System.nanoTime();
    if (now - nextAttemptNanos < 0) {
      rejectedWhileDown.incrementAndGet();
      throw new RedisConnectionException(
          "tier still unreachable; next connect attempt is rate-limited");
    }
    nextAttemptNanos = now + retryNanos;
    connectAttempts.incrementAndGet();
    StatefulRedisConnection<byte[], byte[]> c = redis.connect(new ByteArrayCodec());
    connection = c;
    delegate = c.async();
    return delegate;
  }
}
