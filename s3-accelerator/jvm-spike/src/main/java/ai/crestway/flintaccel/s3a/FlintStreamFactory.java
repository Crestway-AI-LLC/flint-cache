// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.VectoredIOContext;
import org.apache.hadoop.fs.s3a.impl.streams.AbstractObjectInputStreamFactory;
import org.apache.hadoop.fs.s3a.impl.streams.InputStreamType;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStream;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectReadParameters;
import org.apache.hadoop.fs.s3a.impl.streams.StreamFactoryRequirements;

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

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

import ai.crestway.flintaccel.client.FlintObjectClient;
import ai.crestway.flintaccel.client.LazyTierCommands;

/**
 * The S3A entry point: `fs.s3a.input.stream.type=custom` plus
 * `fs.s3a.input.stream.custom.factory=ai.crestway.flintaccel.s3a.FlintStreamFactory`.
 *
 * We construct AAL ourselves -- it is public and takes an ObjectClient by
 * injection -- and hand it OUR client, so AAL keeps doing prefetch and
 * Parquet-awareness while every byte it fetches passes through the Flint tier.
 * S3A's own `analytics` factory hardcodes S3SdkObjectClient and cannot be
 * extended, which is why we register beside it rather than modifying it.
 *
 * Originally a registration probe for ADR-0023 D12.2 / D12.14.
 *
 * Everything about the S3A seam has so far been read, never run. This is the
 * smallest thing that can be RUN: a factory S3A can load from configuration.
 * `readObject` deliberately throws -- the question here is only whether
 * `StreamIntegration.factoryFromConfig` instantiates a third-party class named
 * by `fs.s3a.input.stream.custom.factory`, which decides whether any of the
 * deployment story is real.
 *
 * It also records whether SSE-C is visible at this layer. D13.1 concluded the
 * encryption bypass must be decided HERE, because AAL 1.1.0 cannot see it;
 * that conclusion rests on `ObjectReadParameters.getEncryptionSecrets()` being
 * available and populated, which is asserted rather than assumed below.
 */
public final class FlintStreamFactory extends AbstractObjectInputStreamFactory {

  /** Set when S3A actually constructs one, so the probe cannot pass vacuously. */
  private static final Logger LOG = LoggerFactory.getLogger(FlintStreamFactory.class);

  public static volatile boolean CONSTRUCTED = false;
  public static volatile Configuration INIT_CONF = null;

  /** Config keys, all under our own prefix so S3A never has to know them. */
  public static final String TIER_URI      = "fs.s3a.flint.tier.uri";
  public static final String CHUNK_BYTES   = "fs.s3a.flint.chunk.bytes";
  public static final String TIER_BUDGET   = "fs.s3a.flint.tier.budget.ms";
  public static final String META_TTL      = "fs.s3a.flint.meta.ttl.seconds";
  public static final String CACHE_SSE_KMS = "fs.s3a.flint.cache.sse-kms";
  public static final String MAX_OBJECT   = "fs.s3a.flint.max.object.bytes";
  public static final String MAX_PART     = "fs.s3a.flint.max.part.bytes";
  public static final String IMMUTABLE    = "fs.s3a.flint.immutable";
  public static final String META_TTL_IMM = "fs.s3a.flint.meta.ttl.immutable.seconds";
  public static final String RECONNECT_MS = "fs.s3a.flint.tier.reconnect.ms";
  /** Refuse to start on a shim collision. Default true: a customer's working
   *  job is worth more than our cache. */
  public static final String SHIM_FAIL_FAST = "fs.s3a.flint.shim.failfast";

  private S3AsyncClient s3;
  private RedisClient redis;
  private StatefulRedisConnection<byte[], byte[]> conn;
  /** Non-null only when the tier was down at bind time. */
  public LazyTierCommands lazy;
  private S3SeekableInputStreamFactory aalCaching;
  private S3SeekableInputStreamFactory aalBypass;
  private FlintObjectClient cachingClient;

  public FlintStreamFactory() {
    super("FlintStreamFactory");
    CONSTRUCTED = true;
  }

  /** Visible for tests: the client every cached read flows through. */
  public FlintObjectClient client() { return cachingClient; }

  /** Set at init so callers can inspect what the guard found. */
  public static volatile ShimGuard SHIM = null;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    INIT_CONF = conf;
    // Check BEFORE any S3 request, so a classpath problem is a sentence at
    // startup rather than a NoClassDefFoundError mid-job (ADR-0023 D12.20).
    SHIM = ShimGuard.inspect(getClass().getClassLoader());
    switch (SHIM.state) {
      case SINGLE -> LOG.debug("flint-accel shim check: {}", SHIM);
      case ABSENT -> LOG.warn("flint-accel: {}", SHIM.detail);
      case COLLISION, WRONG_SHAPE -> {
        LOG.error("flint-accel: {}", SHIM.detail);
        for (String l : SHIM.locations) LOG.error("flint-accel:   found at {}", l);
        String msg = failFastMessage(SHIM.state, SHIM.detail,
            conf.getBoolean(SHIM_FAIL_FAST, true));
        if (msg != null) throw new IllegalStateException(msg);
      }
    }
    super.serviceInit(conf);
  }

  /**
   * The fail-fast decision, extracted so it is reachable without a real
   * classpath collision.
   *
   * <p>Inline, this branch could only be entered by a JVM that genuinely had
   * two copies of the shim, so the one setting that steers it was untestable
   * and shared the shape of BUG-0066 — a key that is read, but whose being
   * read nothing demonstrates. Returns the message to throw, or null to
   * proceed.
   *
   * @return null when the run should continue
   */
  static String failFastMessage(ShimGuard.State state, String detail, boolean failFast) {
    boolean broken = state == ShimGuard.State.COLLISION
        || state == ShimGuard.State.WRONG_SHAPE;
    if (!broken || !failFast) return null;
    return "flint-accel: " + detail + " Set " + SHIM_FAIL_FAST
        + "=false to proceed anyway.";
  }

  @Override
  public void bind(org.apache.hadoop.fs.s3a.impl.streams.FactoryBindingParameters p)
      throws IOException {
    super.bind(p);
    Configuration conf = INIT_CONF != null ? INIT_CONF : new Configuration(false);
    String uri = conf.get(TIER_URI, "redis://127.0.0.1:6379");
    int chunk = conf.getInt(CHUNK_BYTES, FlintObjectClient.DEFAULT_CHUNK_BYTES);
    long budget = conf.getLong(TIER_BUDGET, 50);
    long ttl = conf.getLong(META_TTL, 60);

    redis = RedisClient.create(uri);
    redis.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .cancelCommandsOnReconnectFailure(true).autoReconnect(true).build());
    // A TIER THAT IS ALREADY DOWN MUST NOT FAIL THE JOB (D12.9, BUG-0058).
    //
    // This connected eagerly, so a tier that was down when the job started
    // threw out of FileSystem.get and killed the job outright -- on the path
    // the preflight script recommends FIRST. BUG-0058 fixed exactly this, but
    // its fix landed in TierSupport, which paths 2 and 3 build through and this
    // one does not; TierDownSuite builds through TierSupport too, so the gate
    // that exists for this property never looked here.
    //
    // Same fallback TierSupport uses: dial once, and on refusal fall back to a
    // proxy that redials at a rate limit and rejects commands until it
    // succeeds. Every rejected command degrades to the origin, which is a slow
    // read rather than a dead job.
    long reconnectMs = conf.getLong(RECONNECT_MS, 5_000);
    RedisAsyncCommands<byte[], byte[]> tierCmds;
    try {
      conn = redis.connect(new ByteArrayCodec());
      tierCmds = conn.async();
    } catch (RuntimeException down) {
      LOG.warn("flint-accel: tier {} is not answering at startup ({}); reads "
          + "will go to S3 until it does", uri, down.toString());
      lazy = LazyTierCommands.install(redis, reconnectMs);
      tierCmds = lazy.commands();
    }

    // We build our OWN S3AsyncClient rather than reusing S3A's sync client.
    //
    // Hadoop's callbacks offer getOrCreateSyncClient(), and hadoop-aws 3.4.3's
    // own AnalyticsStreamFactory wraps it in S3SyncSdkObjectClient -- a class
    // that exists in NO published AAL version (checked 0.0.1-0.0.4, 1.0.0,
    // 1.1.0). Released Hadoop's analytics path is therefore compiled against
    // an unreleased AAL and would fail at bind time against the Maven Central
    // artifact. We cannot follow it. The published AAL offers only
    // S3SdkObjectClient(S3AsyncClient), so an async client it is.
    this.s3 = buildAsyncClient(conf);
    var origin = new S3SdkObjectClient(s3, false);   // false: we own it, not AAL
    // The S3AsyncClient is passed so SSE-KMS can be DETECTED (D13.3). Without
    // it the client cannot check, and silently caches KMS plaintext -- which
    // is what this path did until now, on the very route the preflight script
    // recommends first. The default-safe behaviour was implemented on two of
    // three paths and absent from the one most customers will use.
    boolean cacheKms = conf.getBoolean(CACHE_SSE_KMS, false);
    // READ EVERY KEY THIS CLASS DECLARES.
    //
    // MAX_OBJECT was declared here and never read: the constant existed, the
    // README documented the setting for this path, and the client was built
    // from the short constructor that takes the defaults -- so setting it did
    // nothing and nothing said so. max.part.bytes, immutable and the immutable
    // TTL had no constant at all while the README listed them for paths 1
    // and 2.
    //
    // This is the SAME defect FlintS3AFileSystem already carries a comment
    // about: an enumerated key list fails closed, which is right, but it does
    // so silently and one key at a time, and nothing fails when a key is
    // forgotten. Path 2 fixed it by mapping flint.* -> fs.s3a.flint.* by RULE.
    // Path 1 cannot use that rule -- it builds the client directly rather than
    // through TierSupport -- so the enumeration stays and the gate now asserts
    // that a value set here reaches the client. See BUG-0066.
    long maxObj = conf.getLong(MAX_OBJECT, FlintObjectClient.DEFAULT_MAX_OBJECT_BYTES);
    long maxPart = conf.getLong(MAX_PART, FlintObjectClient.DEFAULT_MAX_PART_BYTES);
    boolean immutable = conf.getBoolean(IMMUTABLE, false);
    long immTtl = conf.getLong(META_TTL_IMM, 86_400);
    cachingClient = new FlintObjectClient(origin, tierCmds, chunk, budget, ttl,
        false, s3, cacheKms, immutable, immTtl, maxObj, maxPart);
    aalCaching = new S3SeekableInputStreamFactory(
        cachingClient, S3SeekableInputStreamConfiguration.DEFAULT);

    // A second AAL factory over a BYPASSING client, for SSE-C (D13). Built
    // eagerly so the SSE-C path cannot fail for want of setup at the moment it
    // is needed -- which would turn a privacy control into an outage.
    aalBypass = new S3SeekableInputStreamFactory(
        new FlintObjectClient(new S3SdkObjectClient(s3, false),
            tierCmds, chunk, budget, ttl, true, s3, cacheKms,
            immutable, immTtl, maxObj, maxPart),
        S3SeekableInputStreamConfiguration.DEFAULT);
  }

  /** Standard S3A keys, so this behaves like any other S3A component. */
  private static S3AsyncClient buildAsyncClient(Configuration conf) {
    var b = S3AsyncClient.builder()
        .region(Region.of(conf.get("fs.s3a.endpoint.region", "us-east-1")));
    String endpoint = conf.get("fs.s3a.endpoint", "");
    if (!endpoint.isEmpty()) {
      b.endpointOverride(URI.create(
          endpoint.startsWith("http") ? endpoint : "https://" + endpoint));
    }
    if (conf.getBoolean("fs.s3a.path.style.access", false)) b.forcePathStyle(true);
    String ak = conf.get("fs.s3a.access.key", ""), sk = conf.get("fs.s3a.secret.key", "");
    b.credentialsProvider(ak.isEmpty()
        ? DefaultCredentialsProvider.create()
        : StaticCredentialsProvider.create(AwsBasicCredentials.create(ak, sk)));
    return b.build();
  }

  /**
   * SSE-C reads bypass the tier entirely (ADR-0023 D13).
   *
   * Only SSE_C. The others are deliberately NOT bypassed and the distinction
   * is the whole point: with SSE-S3, SSE-KMS and DSSE-KMS the server decrypts
   * for any caller the bucket policy already allows, so a cached plaintext sits
   * inside the same trust boundary the object did. With SSE-C the key is
   * supplied per request by the caller and is the ONLY thing standing between
   * a reader and the bytes; caching plaintext there hands the object to anyone
   * with tier access and no key at all. Client-side encryption (CSE_*) is
   * decrypted above this stream, so what passes through us is ciphertext and
   * is safe to cache.
   */
  private static boolean isSseC(ObjectReadParameters p) {
    S3ObjectAttributes a = p.getObjectAttributes();
    return a != null && a.getServerSideEncryptionAlgorithm() == S3AEncryptionMethods.SSE_C;
  }

  @Override
  public ObjectInputStream readObject(ObjectReadParameters parameters) throws IOException {
    S3ObjectAttributes attrs = parameters.getObjectAttributes();
    S3URI uri = S3URI.of(attrs.getBucket(), attrs.getKey());
    var factory = isSseC(parameters) ? aalBypass : aalCaching;
    return new FlintObjectStream(parameters, factory.createStream(uri));
  }

  @Override
  public InputStreamType streamType() {
    return InputStreamType.Custom;
  }

  @Override
  public StreamFactoryRequirements factoryRequirements() {
    return new StreamFactoryRequirements(0, 0, new VectoredIOContext());
  }

  @Override
  protected void serviceStop() throws Exception {
    if (aalCaching != null) aalCaching.close();
    if (aalBypass != null) aalBypass.close();
    if (conn != null) conn.close();
    // The lazy proxy may have connected after bind returned, so its connection
    // is a separate thing to close and is null until it succeeds.
    if (lazy != null && lazy.connection() != null) lazy.connection().close();
    if (redis != null) redis.shutdown();
    if (s3 != null) s3.close();
    super.serviceStop();
  }
}
