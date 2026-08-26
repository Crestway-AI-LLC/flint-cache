// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.async.RedisAsyncCommands;

import software.amazon.s3.analyticsaccelerator.request.*;

/**
 * One client, composing what seven scattered spikes each proved separately.
 *
 * Every mechanism here has a measurement behind it in ADR-0023:
 *
 *   D3   two-level keys      metadata under a TTL, data addressed by ETag
 *   D4   chunking            absolute 64 KiB grid, fetched ON the grid
 *   D5   single-flight       per CHUNK, not per request
 *   D12.9  resilience        any tier failure degrades to origin, on a budget
 *
 * The consolidation is not tidying. Three of the spikes had different fill
 * paths and one of them cached nothing at all (D12.12) while passing every
 * correctness check; leaving them side by side invites copying the wrong one.
 * More importantly, the mechanisms had never been COMBINED, and the
 * combinations are where the remaining bugs live -- a tier that dies while
 * readers are joined to an in-flight fetch is not covered by testing
 * single-flight and resilience apart.
 */
public final class FlintObjectClient implements ObjectClient {

  /**
   * When true, every request goes straight to the origin and NOTHING is
   * cached (D13).
   *
   * This exists for SSE-C. Under customer-supplied keys S3 returns PLAINTEXT
   * to whoever holds the key; caching that in a shared tier puts it where any
   * other reader of the namespace can have it without the key, defeating the
   * control the customer chose SSE-C to get.
   *
   * The flag is set per client rather than per request because the encryption
   * context is NOT VISIBLE HERE. AAL 1.1.0 ships no EncryptionSecrets class at
   * all -- its OpenStreamInformation carries only StreamContext,
   * ObjectMetadata and InputPolicy, and our three ObjectClient methods receive
   * none of it. Only the layer above, S3A's
   * ObjectReadParameters.getEncryptionSecrets(), can see it, so that layer
   * must choose a bypassing client (or decline our stream type entirely) per
   * stream.
   *
   * Fail-safe direction: a caller who cannot determine the encryption state
   * should pass true. Caching plaintext that should not be cached is a breach;
   * not caching what could have been cached is a slow read.
   */
  public final boolean bypass;

  public final int chunkBytes;
  public final long tierBudgetMs;
  public final long metaTtlSec;

  /**
   * Objects declared IMMUTABLE revalidate on a much longer TTL.
   *
   * D3 splits the cache in two: data addressed by ETag, metadata under a short
   * TTL. The TTL exists because an object at a path can be replaced, and a
   * stale length is not merely old -- D12.29 showed it makes reads hit EOF
   * early, which is worse than a stale value because it looks like truncation.
   *
   * But for a format whose files are never rewritten, that revalidation is
   * pure cost: a HEAD per object per minute, buying protection against a
   * change that the format guarantees cannot happen. ADR-0023 left this as an
   * open question; Alluxio's PrestoCacheContext is prior art for the shape of
   * the answer -- the ENGINE knows facts about the data that the cache cannot
   * infer, so let it say so.
   *
   * A long TTL rather than none. If a caller declares immutability and is
   * wrong -- deletes a path and writes different bytes there -- an infinite
   * TTL means permanent wrongness, while a day bounds it. Our own writes
   * invalidate regardless (D12.29), so the exposure is only to out-of-band
   * rewrites by another process.
   */
  public final long metaTtlImmutableSec;
  /** Objects above this are read from the origin and never chunk-cached.
   *
   *  512 MiB (Jeff, 2026-08-25), and the reasoning is PAYOFF rather than
   *  keyspace. Measured: a warm read moves the SAME bytes as a cold one -- the
   *  cache does not reduce data transferred, it changes where it comes from.
   *  So for a large object read sequentially the whole benefit is
   *  bytes x (1/S3_throughput - 1/tier_throughput): near zero against parallel
   *  range GETs on a fast NIC, and NEGATIVE when the tier is slower. Past some
   *  size the client is better off going to S3 directly.
   *
   *  512 MiB sits above the data files this cache is for -- Parquet and
   *  Iceberg land at 128-512 MB -- so the analytics working set still caches
   *  while objects whose only payoff is a throughput differential do not.
   *
   *  The keyspace argument still holds underneath: an object of size S occupies
   *  S/chunkBytes keys, so a 1 TB object would cost ~1.7 GB of per-key overhead
   *  before a byte of its data is stored.
   *
   *  THIS NUMBER IS AN ARGUMENT, NOT A FINDING -- the crossover wants measuring
   *  on a cluster. Tracked, blocked on M0. */
  public static final long DEFAULT_MAX_OBJECT_BYTES = 512L * 1024 * 1024;

  /** The chunk grid, and the ONE place it is spelled.
   *
   *  It costs 1.25x tier memory to be a power of two -- CHUNK + SEAL is one
   *  byte past jemalloc's 64 KiB class -- and 65,408 was implemented to escape
   *  that and then withdrawn: application read offsets are powers of two, so a
   *  grid that is not one stops dividing them and every selective read drags an
   *  extra chunk. Measured, +19.8% origin bytes for a ~4% memory saving. See
   *  ADR-0023 D20 and the python client's flint_accel.tier.CHUNK, which carries
   *  the same finding.
   *
   *  MUST equal that constant: the two clients share one keyspace and an index
   *  is an offset divided by this number, so a disagreement is a correctness
   *  bug, not a miss. It lived as a literal in fifteen places before it had a
   *  name -- including the production S3A path -- so moving "the default" moved
   *  one of them, and the cross-language drill was the only check that could
   *  see the other fourteen. */
  public static final int DEFAULT_CHUNK_BYTES = 65536;
  public final long maxObjectBytes;
  private final Map<String, Boolean> oversizeSeen = new ConcurrentHashMap<>();
  private final boolean immutable;

  private long ttlFor() { return immutable ? metaTtlImmutableSec : metaTtlSec; }

  private final ObjectClient origin;
  private final RedisAsyncCommands<byte[], byte[]> tier;
  private final ConcurrentMap<String, CompletableFuture<byte[]>> inflight =
      new ConcurrentHashMap<>();

  /** Every claim this product makes is a counter. */
  public final AtomicLong chunkHits = new AtomicLong();
  public final AtomicLong chunkMisses = new AtomicLong();
  public final AtomicLong metaHits = new AtomicLong();
  public final AtomicLong metaMisses = new AtomicLong();
  public final AtomicLong originGets = new AtomicLong();
  public final AtomicLong originBytes = new AtomicLong();
  public final AtomicLong tierFailures = new AtomicLong();
  public final AtomicLong degraded = new AtomicLong();
  public final AtomicLong bypassed = new AtomicLong();
  public final AtomicLong claimed = new AtomicLong();
  public final AtomicLong joined = new AtomicLong();
  public final AtomicLong integrityFailures = new AtomicLong();
  public final AtomicLong kmsBypassed = new AtomicLong();
  public final AtomicLong kmsUndetectable = new AtomicLong();
  public final AtomicLong breakerOpens = new AtomicLong();
  public final AtomicLong breakerSkips = new AtomicLong();
  public final AtomicLong oversizeBypassed = new AtomicLong();

  /**
   * A breaker, because a SICK tier is worse than no tier.
   *
   * A dead tier is cheap: the connection refuses inline and the read falls
   * through at once. A tier that still answers, slowly, is the dangerous state
   * -- every request burns the whole budget, fails, and goes to the origin
   * anyway, so the cache ADDS its budget to every read instead of removing
   * anything. Measured before this existed: 62 ms with no tier, 2 ms with a
   * healthy one, and 164 ms with a sick one. The cache made the fleet 165%
   * slower than never installing it.
   *
   * That is exactly the state an operator misreads, because the tier is UP.
   *
   * So: after {@code TRIP} consecutive failures, stop calling the tier for a
   * cooling window and serve straight from the origin -- which is what the
   * customer had before us, and is the floor a cache must never fall below.
   * The window doubles up to a cap so a long outage costs one probe every few
   * seconds rather than one per request, and any success resets it, so
   * recovery is immediate rather than waiting out the last backoff.
   */
  private static final int TRIP = 3;
  private static final long BREAKER_BASE_MS = 500;
  private static final long BREAKER_CAP_MS = 10_000;
  private final AtomicLong consecutiveFailures = new AtomicLong();
  private final AtomicLong openUntilNanos = new AtomicLong();
  private final AtomicLong cooldownMs = new AtomicLong(BREAKER_BASE_MS);

  /** Whether the breaker is currently open. For metrics; does not probe. */
  public boolean isBreakerOpen() {
    long until = openUntilNanos.get();
    return until != 0 && System.nanoTime() < until;
  }

  /** True when the tier should be skipped entirely right now. */
  private boolean breakerOpen() {
    long until = openUntilNanos.get();
    if (until == 0) return false;
    if (System.nanoTime() < until) return true;
    // Half-open: exactly ONE caller gets through to probe. The CAS decides
    // which, so a recovering tier is not stampeded by every waiting reader
    // the instant the window expires.
    return !openUntilNanos.compareAndSet(until, 0);
  }

  private void tierFailed() {
    if (consecutiveFailures.incrementAndGet() < TRIP) return;
    consecutiveFailures.set(0);
    long wait = cooldownMs.get();
    openUntilNanos.set(System.nanoTime() + wait * 1_000_000L);
    cooldownMs.set(Math.min(wait * 2, BREAKER_CAP_MS));
    breakerOpens.incrementAndGet();
  }

  private void tierWorked() {
    consecutiveFailures.set(0);
    cooldownMs.set(BREAKER_BASE_MS);
  }

  /**
   * SSE-KMS reads BYPASS the cache by default (ADR-0023 D13.3).
   *
   * Unlike SSE-C, S3 decrypts SSE-KMS server-side and hands us plaintext
   * lawfully, so caching it is not a key-handling violation. Two things the
   * customer paid for are lost anyway:
   *
   *   - the KMS grant stops being the gate. Anyone who can read the tier
   *     namespace reads the plaintext WITHOUT holding kms:Decrypt.
   *   - every cache hit is a decrypt that never reaches CloudTrail. For many
   *     customers that audit trail IS the compliance requirement, and no
   *     mitigation restores it, because it is a property of not calling KMS.
   *
   * So: default off, opt-in after the customer's own review, never a surprise
   * in a security review. `flint.cache.sse-kms=true` turns it on.
   *
   * Detection costs nothing extra. AAL's ObjectMetadata carries only
   * contentLength and etag -- the x-amz-server-side-encryption header is
   * discarded before our ObjectClient ever sees it -- so we issue the HEAD
   * ourselves through the SDK and take all three fields from the one call,
   * REPLACING the delegated head rather than adding to it.
   */
  private final boolean cacheKms;
  private final S3AsyncClient raw;
  private final Map<String, Boolean> kmsSeen = new ConcurrentHashMap<>();

  /**
   * REDUCED CAPABILITY, and not the production path.
   *
   * Without an S3AsyncClient this client cannot determine whether an object is
   * SSE-KMS encrypted, so it caches NO metadata at all: writing an unverified
   * "not encrypted" is worse than writing nothing, because a client that CAN
   * check would then read and trust it. The consequence is that this overload
   * also forfeits D3's metadata saving and its staleness window -- which the
   * suites discovered by failing, not by being told.
   *
   * TierSupport always supplies the handle, so every adoption path has the
   * full behaviour. These overloads exist for direct construction in tests.
   */
  public FlintObjectClient(ObjectClient origin, RedisAsyncCommands<byte[], byte[]> tier,
                           int chunkBytes, long tierBudgetMs, long metaTtlSec) {
    this(origin, tier, chunkBytes, tierBudgetMs, metaTtlSec, false);
  }

  public FlintObjectClient(ObjectClient origin, RedisAsyncCommands<byte[], byte[]> tier,
                           int chunkBytes, long tierBudgetMs, long metaTtlSec,
                           boolean bypass) {
    this(origin, tier, chunkBytes, tierBudgetMs, metaTtlSec, bypass, null, false);
  }

  public FlintObjectClient(ObjectClient origin, RedisAsyncCommands<byte[], byte[]> tier,
                           int chunkBytes, long tierBudgetMs, long metaTtlSec,
                           boolean bypass, S3AsyncClient raw, boolean cacheKms) {
    this(origin, tier, chunkBytes, tierBudgetMs, metaTtlSec, bypass, raw, cacheKms,
        false, 86_400, DEFAULT_MAX_OBJECT_BYTES);
  }

  public FlintObjectClient(ObjectClient origin, RedisAsyncCommands<byte[], byte[]> tier,
                           int chunkBytes, long tierBudgetMs, long metaTtlSec,
                           boolean bypass, S3AsyncClient raw, boolean cacheKms,
                           boolean immutable, long metaTtlImmutableSec) {
    this(origin, tier, chunkBytes, tierBudgetMs, metaTtlSec, bypass, raw, cacheKms,
        immutable, metaTtlImmutableSec, DEFAULT_MAX_OBJECT_BYTES);
  }

  public FlintObjectClient(ObjectClient origin, RedisAsyncCommands<byte[], byte[]> tier,
                           int chunkBytes, long tierBudgetMs, long metaTtlSec,
                           boolean bypass, S3AsyncClient raw, boolean cacheKms,
                           boolean immutable, long metaTtlImmutableSec,
                           long maxObjectBytes) {
    this.maxObjectBytes = maxObjectBytes;
    this.immutable = immutable;
    this.metaTtlImmutableSec = metaTtlImmutableSec;
    this.bypass = bypass;
    this.origin = origin; this.tier = tier;
    this.chunkBytes = chunkBytes; this.tierBudgetMs = tierBudgetMs;
    this.metaTtlSec = metaTtlSec;
    this.raw = raw; this.cacheKms = cacheKms;
  }

  /**
   * True when this object must not touch the tier.
   *
   * Unknown means "not yet headed". AAL always heads before it gets, so the
   * unknown branch is rare; treating unknown as safe-to-cache would be the
   * wrong default, and treating it as bypass would disable the cache whenever
   * the map missed. Neither: an unknown object is HEADed, which is the only
   * answer that is both correct and cheap.
   */
  private boolean kmsBypass(Object s3Uri) {
    if (cacheKms || raw == null) return false;
    Boolean k = kmsSeen.get(String.valueOf(s3Uri));
    return k != null && k;
  }

  private static byte[] utf8(String s) {
    return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  /** ETags arrive quoted from AAL; normalise once, here, or keys silently split. */
  private static String norm(String etag) {
    if (etag == null) return "";
    return etag.startsWith("\"") && etag.endsWith("\"") && etag.length() >= 2
        ? etag.substring(1, etag.length() - 1) : etag;
  }

  /**
   * A cached chunk is stored SEALED: a 4-byte CRC32 over the chunk's identity
   * -- its object ETag and its chunk index -- followed by the bytes.
   *
   * <p><b>CRC32, not CRC32C, and the reason is interoperability.</b> The tier
   * is shared between the JVM and Python clients, so the seal has to be
   * computed identically in both. {@code java.util.zip.CRC32} and Python's
   * {@code zlib.crc32} are the same polynomial and agree byte for byte; CRC32C
   * has no standard-library equivalent in Python. The marginal difference in
   * burst-error detection between the two polynomials is worth far less than
   * both languages having the primitive built in and hardware-accelerated --
   * and a seal only one client can compute would split the cache in two,
   * which is exactly the bug this change was made to fix.
   *
   * Sealing the identity, not just the content, is the whole point. A checksum
   * over content alone detects bit-rot and nothing else; it cannot tell that
   * chunk 5 has been served where chunk 3 belongs, because chunk 5's bytes are
   * perfectly valid bytes. The adversarial suite demonstrates exactly that:
   * swapping two chunks of one object returned 130,544 wrong bytes with every
   * structural check -- count, length, contiguity -- passing.
   *
   * A failed seal is treated as a MISS, not an error. The chunk is refetched
   * from the origin, which is authoritative, so a corrupt tier degrades to a
   * slow tier rather than a wrong one. That reuses the fallback path the
   * ABSENT case already exercises rather than inventing a second one.
   *
   * <p><b>What this does not do.</b> CRC32 is a corruption detector, not an
   * adversary detector. Anyone able to write arbitrary values into the tier can
   * also write a matching checksum. Defending against a forger needs a MAC
   * keyed with a secret the tier does not hold -- a different threat model, and
   * a real cost. This catches bit-rot, misplacement, key collisions, namespace
   * bugs and truncation, which are the failures a tier actually produces.
   */
  private static final int SEAL = 4;

  private static long sealOf(String etag, long id, byte[] body, int off, int len) {
    CRC32 c = new CRC32();
    c.update(utf8(norm(etag)));
    byte[] idb = new byte[8];
    for (int i = 0; i < 8; i++) idb[i] = (byte) (id >>> (8 * i));
    c.update(idb, 0, 8);
    c.update(body, off, len);
    return c.getValue();
  }

  private static byte[] seal(String etag, long id, byte[] body) {
    byte[] out = new byte[SEAL + body.length];
    long t = sealOf(etag, id, body, 0, body.length);
    for (int i = 0; i < SEAL; i++) out[i] = (byte) (t >>> (8 * i));
    System.arraycopy(body, 0, out, SEAL, body.length);
    return out;
  }

  /** Bench-only handles on the seal, so measuring it needs no production flag. */
  static byte[] sealForBench(String etag, long id, byte[] body) { return seal(etag, id, body); }

  byte[] unsealForBench(String etag, long id, byte[] sealed) { return unseal(etag, id, sealed); }

  /** Returns the payload, or null if this value is not what it claims to be. */
  private byte[] unseal(String etag, long id, byte[] sealed) {
    if (sealed == null || sealed.length < SEAL) {
      if (sealed != null) integrityFailures.incrementAndGet();
      return null;
    }
    long want = 0;
    for (int i = SEAL - 1; i >= 0; i--) want = (want << 8) | (sealed[i] & 0xFFL);
    if (want != (sealOf(etag, id, sealed, SEAL, sealed.length - SEAL) & 0xFFFFFFFFL)) {
      integrityFailures.incrementAndGet();
      return null;
    }
    return Arrays.copyOfRange(sealed, SEAL, sealed.length);
  }

  /**
   * The prefix is versioned, and it must be bumped whenever the value format
   * changes. Not tidiness: a mixed fleet in which old clients write unsealed
   * values and new clients reject them would miss on EVERY key and stampede
   * the origin -- an outage caused by an upgrade that looked backward
   * compatible. Separate keyspaces let both formats coexist while old entries
   * age out under eviction.
   */
  private byte[] chunkKey(String etag, long id) { return utf8("c1/" + norm(etag) + "/" + id); }
  /**
   * Versioned like the chunk prefix, and for the same reason: the value now
   * carries a third field and a mixed fleet must not read one format as the
   * other. See chunkKey.
   */
  private byte[] metaKey(Object uri) { return utf8("m1/" + uri); }

  /** Bound every tier call and turn any failure into "no cache". */
  private <T> CompletableFuture<T> guarded(Supplier<CompletableFuture<T>> op) {
    if (breakerOpen()) {
      breakerSkips.incrementAndGet();
      return CompletableFuture.completedFuture(null);   // straight to origin
    }
    CompletableFuture<T> f;
    try {
      f = op.get();
    } catch (RuntimeException e) {      // already-dead connection throws inline
      tierFailures.incrementAndGet();
      tierFailed();
      return CompletableFuture.completedFuture(null);
    }
    return f.orTimeout(tierBudgetMs, TimeUnit.MILLISECONDS)
        .handle((v, err) -> {
          if (err != null) { tierFailures.incrementAndGet(); tierFailed(); return null; }
          tierWorked();
          return v;
        });
  }

  /** Fills must respect the breaker too, or a sick tier still gets written to. */
  private boolean tierUsable() { return !breakerOpen(); }

  public interface Supplier<T> { T get(); }

  // ---------------------------------------------------------------- HEAD

  @Override
  public CompletableFuture<ObjectMetadata> headObject(HeadRequest r) {
    if (bypass) { bypassed.incrementAndGet(); return origin.headObject(r); }
    byte[] mk = metaKey(r.getS3Uri());
    return guarded(() -> tier.get(mk).toCompletableFuture())
        .thenCompose(v -> {
          if (v != null) {
            String s = new String(v, java.nio.charset.StandardCharsets.UTF_8);
            // length|etag|kms. The third field is load-bearing: kmsSeen used to
            // be populated ONLY by our own HEAD, so an object whose metadata
            // came from the tier skipped detection entirely and was treated as
            // unencrypted. That made the SSE-KMS rule depend on which client
            // happened to populate the metadata first -- a client that could
            // not detect (no SDK handle) poisoned the entry for every client
            // that could. Carrying the answer with the metadata removes the
            // ordering dependence rather than documenting it.
            int b1 = s.indexOf('|');
            int b2 = b1 < 0 ? -1 : s.indexOf('|', b1 + 1);
            if (b1 > 0 && b2 > b1) {
              metaHits.incrementAndGet();
              kmsSeen.put(String.valueOf(r.getS3Uri()), "1".equals(s.substring(b2 + 1)));
              long cachedLen = Long.parseLong(s.substring(0, b1));
              oversizeSeen.put(String.valueOf(r.getS3Uri()), cachedLen > maxObjectBytes);
              return CompletableFuture.completedFuture(ObjectMetadata.builder()
                  .contentLength(cachedLen)
                  .etag(s.substring(b1 + 1, b2)).build());
            }
          }
          metaMisses.incrementAndGet();
          if (raw == null) {
            // Detection unavailable: no SDK client was supplied. TierSupport
            // always supplies one, so this is the direct-construction path in
            // tests. Counted rather than silent, because "we could not check"
            // and "we checked and it was fine" must not look the same.
            kmsUndetectable.incrementAndGet();
            return origin.headObject(r).thenApply(m -> {
              // No SDK handle means we could not check, so nothing is written:
              // caching "kms=0" that we never verified is worse than caching
              // nothing, because a client that CAN check would then trust it.
              return m;
            });
          }
          return ourHead(r);
        });
  }

  /** One HEAD, three answers: length, etag, and whether KMS decrypted it. */
  private CompletableFuture<ObjectMetadata> ourHead(HeadRequest r) {
    String bucket = bucketOf(r.getS3Uri()), key = keyOf(r.getS3Uri());
    return raw.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build())
        .thenApply(resp -> {
          boolean kms = resp.serverSideEncryption() == ServerSideEncryption.AWS_KMS;
          kmsSeen.put(String.valueOf(r.getS3Uri()), kms);
          oversizeSeen.put(String.valueOf(r.getS3Uri()),
              resp.contentLength() != null && resp.contentLength() > maxObjectBytes);
          ObjectMetadata m = ObjectMetadata.builder()
              .contentLength(resp.contentLength()).etag(resp.eTag()).build();
          if (kms && !cacheKms) {
            // Bypass means bypass: no chunk data AND no metadata. Caching the
            // length and etag of an object we refuse to cache would be a
            // smaller leak, not no leak, and it would make the rule harder to
            // state to a customer than it is worth.
            return m;
          }
          try {
            tier.setex(metaKey(r.getS3Uri()), ttlFor(),
                utf8(m.getContentLength() + "|" + m.getEtag() + "|" + (kms ? "1" : "0")));
          } catch (RuntimeException e) { tierFailures.incrementAndGet(); }
          return m;
        })
        .exceptionallyCompose(e -> {
          // A failed probe must not fail the read. Fall back to the delegated
          // head, and count it: an undetectable object is cached, so this
          // number is the size of the hole in the guarantee.
          kmsUndetectable.incrementAndGet();
          return origin.headObject(r);
        });
  }

  private static String bucketOf(Object uri) {
    String s = String.valueOf(uri);
    int i = s.indexOf("://");
    String rest = i < 0 ? s : s.substring(i + 3);
    int slash = rest.indexOf('/');
    return slash < 0 ? rest : rest.substring(0, slash);
  }

  private static String keyOf(Object uri) {
    String s = String.valueOf(uri);
    int i = s.indexOf("://");
    String rest = i < 0 ? s : s.substring(i + 3);
    int slash = rest.indexOf('/');
    return slash < 0 ? "" : rest.substring(slash + 1);
  }

  // ----------------------------------------------------------------- GET

  @Override
  public CompletableFuture<ObjectContent> getObject(GetRequest r) { return get(r, null); }

  @Override
  public CompletableFuture<ObjectContent> getObject(GetRequest r, StreamContext c) { return get(r, c); }

  private CompletableFuture<ObjectContent> get(GetRequest r, StreamContext ctx) {
    if (bypass) { bypassed.incrementAndGet(); return passthrough(r, ctx); }
    if (kmsBypass(r.getS3Uri())) { kmsBypassed.incrementAndGet(); return passthrough(r, ctx); }
    // Capacity admission. Counted rather than silent: "not cached because too
    // large" and "not cached because something broke" must never look alike.
    // Unknown size does NOT reject -- refusing on an unknown is how a cap
    // quietly becomes "cache nothing".
    if (Boolean.TRUE.equals(oversizeSeen.get(String.valueOf(r.getS3Uri())))) {
      oversizeBypassed.incrementAndGet();
      return passthrough(r, ctx);
    }
    final long start = r.getRange().getStart(), end = r.getRange().getEnd();
    final long first = start / chunkBytes, last = end / chunkBytes;
    final int n = (int) (last - first + 1);
    final String etag = r.getEtag();

    byte[][] keys = new byte[n][];
    for (int i = 0; i < n; i++) keys[i] = chunkKey(etag, first + i);

    return guarded(() -> tier.mget(keys).toCompletableFuture())
        .thenCompose(vals -> {
          if (vals == null) {                       // tier unusable -> plain S3
            degraded.incrementAndGet();
            return passthrough(r, ctx);
          }
          @SuppressWarnings("unchecked")
          CompletableFuture<byte[]>[] slots = new CompletableFuture[n];
          List<Integer> mine = new ArrayList<>();

          for (int i = 0; i < n; i++) {
            KeyValue<byte[], byte[]> kv = vals.get(i);
            byte[] cached = kv.hasValue() ? unseal(etag, first + i, kv.getValue()) : null;
            if (cached != null) {
              chunkHits.incrementAndGet();
              slots[i] = CompletableFuture.completedFuture(cached);
              continue;
            }
            chunkMisses.incrementAndGet();
            final String ik = norm(etag) + "/" + (first + i);
            boolean[] leader = {false};
            slots[i] = inflight.computeIfAbsent(ik, k -> {
              leader[0] = true;
              return new CompletableFuture<>();
            });
            if (leader[0]) { claimed.incrementAndGet(); mine.add(i); }
            else { joined.incrementAndGet(); }
          }

          for (int[] run : runs(mine)) fetchRun(r, ctx, etag, first, run);

          // A joined follower must not hang forever if its leader dies with
          // the tier. Bound the wait and fall back to the origin -- the
          // interaction none of the separate spikes covered.
          return CompletableFuture.allOf(slots)
              .orTimeout(tierBudgetMs * 40, TimeUnit.MILLISECONDS)
              .handle((v, err) -> err)
              .thenCompose(err -> {
                if (err != null) { degraded.incrementAndGet(); return passthrough(r, ctx); }
                ObjectContent oc = assemble(slots, first, start, end, n);
                if (oc == null) {          // gap: the origin is authoritative
                  degraded.incrementAndGet();
                  return passthrough(r, ctx);
                }
                return CompletableFuture.completedFuture(oc);
              });
        });
  }

  private CompletableFuture<ObjectContent> passthrough(GetRequest r, StreamContext ctx) {
    originGets.incrementAndGet();
    return (ctx == null) ? origin.getObject(r) : origin.getObject(r, ctx);
  }

  /** Fetch a contiguous run of missing chunks ON THE GRID, then fill and complete. */
  private void fetchRun(GetRequest r, StreamContext ctx, String etag, long first, int[] run) {
    long lo = (first + run[0]) * (long) chunkBytes;
    long hi = (first + run[1]) * (long) chunkBytes + chunkBytes - 1;
    GetRequest sub = GetRequest.builder()
        .s3Uri(r.getS3Uri()).etag(etag).referrer(r.getReferrer())
        .range(new Range(lo, hi)).build();
    originGets.incrementAndGet();
    CompletableFuture<ObjectContent> up =
        (ctx == null) ? origin.getObject(sub) : origin.getObject(sub, ctx);
    up.whenComplete((oc, err) -> {
      if (err != null) { failRun(etag, first, run, err); return; }
      byte[] all;
      try (InputStream in = oc.getStream()) { all = in.readAllBytes(); }
      catch (IOException e) { failRun(etag, first, run, e); return; }
      originBytes.addAndGet(all.length);
      for (int i = run[0]; i <= run[1]; i++) {
        int off = (i - run[0]) * chunkBytes;
        byte[] piece = off >= all.length ? new byte[0]
            : Arrays.copyOfRange(all, off, Math.min(off + chunkBytes, all.length));
        final String ik = norm(etag) + "/" + (first + i);

        // Publish to the tier BEFORE releasing the claim.
        //
        // The claim used to be dropped the instant the SET was ISSUED, and the
        // SET is asynchronous -- so a reader arriving in the gap found no
        // chunk in the tier and no claim to join, and fetched the same bytes
        // again. Every such reader is a duplicate origin request, and the
        // window widens exactly when it hurts: on a loaded machine. Locally
        // 24 concurrent readers cost 1-2 GETs and on a shared CI runner the
        // same test cost 6, which is how the race was found at all.
        //
        // Holding the claim until the write lands means late readers JOIN
        // instead of duplicating. Bounded by the tier budget and completed
        // either way, because a slow tier must not strand readers who are
        // waiting on us -- that would trade duplicate fetches for a hang.
        Runnable release = () -> {
          CompletableFuture<byte[]> slot = inflight.remove(ik);
          if (slot != null) slot.complete(piece);
        };
        CompletableFuture<?> written = null;
        try {
          if (piece.length > 0 && tierUsable()) {
            written = tier.set(chunkKey(etag, first + i), seal(etag, first + i, piece))
                .toCompletableFuture()
                .orTimeout(tierBudgetMs, TimeUnit.MILLISECONDS);
          }
        } catch (RuntimeException e) {
          tierFailures.incrementAndGet();
        }
        if (written == null) {
          release.run();
        } else {
          written.whenComplete((v, e) -> {
            if (e != null) tierFailures.incrementAndGet();
            release.run();
          });
        }
      }
    });
  }

  private void failRun(String etag, long first, int[] run, Throwable err) {
    for (int i = run[0]; i <= run[1]; i++) {
      CompletableFuture<byte[]> slot = inflight.remove(norm(etag) + "/" + (first + i));
      if (slot != null) slot.completeExceptionally(err);
    }
  }

  /**
   * Reassemble, or say so. Returns null when the chunks do not form a
   * contiguous run, which the caller turns into an origin read.
   *
   * The first version skipped a missing chunk and carried on. That turns a
   * HOLE into a short read, and a short read at the wrong moment is
   * indistinguishable from end-of-file: Hadoop's contract suite saw it as
   * "End of file reached before reading fully" on multi-chunk objects, and
   * only intermittently, because it needs a chunk to go missing at all.
   *
   * A gap is never acceptable. Silently returning fewer bytes than asked for
   * is the one failure mode a caching layer must not have, because every
   * layer above treats short-then-nothing as EOF.
   */
  private ObjectContent assemble(CompletableFuture<byte[]>[] slots, long first,
                                 long start, long end, int n) {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    for (int i = 0; i < n; i++) {
      byte[] c;
      try {
        c = slots[i].join();
      } catch (RuntimeException e) {
        return null;
      }
      long cs = (first + i) * (long) chunkBytes;
      if (c == null || c.length == 0) {
        // Legitimate only if this chunk begins at or past the end of what was
        // asked for; anywhere earlier is a gap.
        if (cs <= end) return null;
        continue;
      }
      int from = (int) Math.max(0, start - cs);
      int to = (int) Math.min(c.length, end - cs + 1);
      if (to > from) out.write(c, from, to - from);
      // A short chunk mid-run is also a gap: the next chunk cannot follow it.
      if (c.length < chunkBytes && cs + c.length <= end) return null;
    }
    return ObjectContent.builder()
        .stream(new ByteArrayInputStream(out.toByteArray())).build();
  }

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

  /**
   * Drop the cached metadata for one object.
   *
   * ADR-0023 D1 says writes invalidate metadata, and it was never implemented.
   * The consequence is worse than staleness: the metadata carries the object
   * LENGTH, so a path rewritten at a different size leaves AAL believing the
   * object ends where the old one did, and reads stop early with EOF rather
   * than returning stale bytes. Hadoop's contract suite rewrites the same path
   * repeatedly and caught it; nothing of ours did, because our tests wrote
   * each object once.
   *
   * Data keys need no invalidation -- they are addressed by ETag and a new
   * version simply misses.
   */
  public void invalidate(Object s3Uri) {
    try {
      tier.del(metaKey(s3Uri));
    } catch (RuntimeException e) {
      tierFailures.incrementAndGet();
    }
  }

  @Override public void close() throws IOException { origin.close(); }

  public String counters() {
    return String.format(
        "chunk %d/%d  meta %d/%d  origin %dG/%dB  sf %dc/%dj  tierFail %d  degraded %d  bypassed %d  oversize %d",
        chunkHits.get(), chunkHits.get() + chunkMisses.get(),
        metaHits.get(), metaHits.get() + metaMisses.get(),
        originGets.get(), originBytes.get(), claimed.get(), joined.get(),
        tierFailures.get(), degraded.get(), bypassed.get(), oversizeBypassed.get());
  }
}
