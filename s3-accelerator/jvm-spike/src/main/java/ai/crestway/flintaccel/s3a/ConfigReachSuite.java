// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * BUG-0066, generalised: every config key adoption path 1 DECLARES must be one
 * somebody has shown to do something.
 *
 * <p>The bug was a {@code public static final String} on
 * {@link FlintStreamFactory} that was documented, printed to customers by the
 * preflight script, and read by nothing — the {@code conf.getLong} call it was
 * declared for had never been written. There was no symptom and there could not
 * have been: <b>an ignored setting behaves exactly like a setting left at its
 * default</b>, and every test used defaults.
 *
 * <p>It was also a recurrence. The sibling class carries a comment about the
 * identical failure, written when a different key was found unreachable, and
 * says why: an enumerated key list fails closed, which is right, but it does so
 * silently and one key at a time. Path 2 replaced its enumeration with a rule.
 * Path 1 cannot — it builds the client directly rather than through
 * {@code TierSupport} — so the enumeration stays and this suite is what stops it
 * rotting.
 *
 * <h2>Three layers, because one is not enough</h2>
 *
 * <ol>
 *   <li><b>Registry.</b> Every declared {@code fs.s3a.flint.*} constant must
 *       appear in {@link #REGISTRY}, found by REFLECTION rather than by a list
 *       kept here. Add a constant without classifying it and this suite fails.
 *       That is the part that stops the silent forget, which is the actual
 *       bug — every other check only covers keys somebody remembered.
 *   <li><b>Structural.</b> Every declared constant must be REFERENCED in the
 *       factory's source somewhere other than its own declaration. This is the
 *       literal shape of BUG-0066 — {@code MAX_OBJECT} appeared exactly once,
 *       on the line that declared it — and it costs nothing to check for every
 *       key, including the ones no probe can reach.
 *   <li><b>Behavioural.</b> Where a key admits a cheap probe, set it to a value
 *       whose effect is unmistakable and assert the effect, each with a control
 *       showing the default behaves the other way. Asserted by EFFECT and never
 *       by reading the value back: a check that read the field would pass on a
 *       client that stored the setting and ignored it, which is a nearby
 *       version of this same bug.
 * </ol>
 *
 * <p>A key with no behavioural probe here is not skipped — it is classified,
 * and the classification names the suite that covers it or says plainly that
 * only the structural check does.
 */
public final class ConfigReachSuite {

  static boolean ok = true;

  /**
   * The "short" TTL under test, in seconds, and how long to wait past it.
   *
   * <p>Three, not one. At a 1 s TTL the armed check below saw ZERO metadata
   * keys immediately after the read that wrote them — not a bug in the client:
   * building a FileSystem and opening a stream takes longer than a second, so
   * the key had already expired by the time it could be looked at. A TTL
   * shorter than the operation that observes it cannot be observed.
   */
  static final long SHORT_TTL_S = 3;

  /**
   * Reads per arm of the reconnect probe.
   *
   * <p>Twenty, not six. At six the two arms measured 2 attempts against 1 --
   * directionally right and one sample from being a coin flip. The gap has to
   * be wide enough that the check is about the rate limit rather than about
   * scheduling.
   */
  static final int RETRY_READS = 20;

  /**
   * A tier URI that must never answer.
   *
   * <p>One constant, and the port is DECLARED in the exclusivity check even
   * though nothing binds it: if a sibling harness ever bound 9498, the
   * dead-tier probes here would connect to something and pass for the wrong
   * reason. A port that must stay closed needs the same declaration as one that
   * gets bound -- more, because the failure is a silent green.
   */
  static final String DEAD_TIER = "redis://127.0.0.1:9498";
  static final long PAST_TTL_MS = SHORT_TTL_S * 1000 + 1500;
  static String endpoint, tier, slowTier;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  /** How a declared key's effect is demonstrated. */
  enum How {
    /** A behavioural probe in this suite. */
    PROBED,
    /** A behavioural probe in another gated suite, named in the note. */
    ELSEWHERE,
    /** Only the structural check reaches it; the note says why. */
    STRUCTURAL_ONLY
  }

  record Cover(How how, String note) {}

  /**
   * Keyed by the CONSTANT NAME, so a renamed constant is a new entry rather
   * than a silently-matched string.
   */
  static final Map<String, Cover> REGISTRY = new LinkedHashMap<>();
  static {
    REGISTRY.put("TIER_URI", new Cover(How.PROBED,
        "a tier that is not there must stop caching without breaking reads"));
    REGISTRY.put("CHUNK_BYTES", new Cover(How.PROBED,
        "a different grid must produce a different number of chunk keys"));
    REGISTRY.put("TIER_BUDGET", new Cover(How.PROBED,
        "a budget under the tier's latency must degrade to the origin"));
    REGISTRY.put("META_TTL", new Cover(How.PROBED,
        "a 1 s metadata TTL must revalidate where a long one does not"));
    REGISTRY.put("META_TTL_IMM", new Cover(How.PROBED,
        "the immutable TTL must be the one that applies once immutable is set"));
    REGISTRY.put("IMMUTABLE", new Cover(How.PROBED,
        "declaring immutability must suppress revalidation the TTL would force"));
    REGISTRY.put("MAX_PART", new Cover(How.PROBED,
        "a 1-byte part cap must stop this path caching"));
    REGISTRY.put("MAX_OBJECT", new Cover(How.PROBED,
        "a 1-byte object cap must stop this path caching"));
    REGISTRY.put("RECONNECT_MS", new Cover(How.PROBED,
        "a dead tier must not cost a reconnect per read: the attempt count is "
        + "bounded across many reads, and a tiny budget must attempt MORE than "
        + "a huge one"));
    REGISTRY.put("CACHE_SSE_KMS", new Cover(How.ELSEWHERE,
        "SseKmsPathsSuite sets it on THIS path (gated as 'SSE-KMS on all 3 "
        + "adoption paths') and asserts the tier is populated only when it is on"));
    REGISTRY.put("SHIM_FAIL_FAST", new Cover(How.ELSEWHERE,
        "shim_guard_test.sh drives the REAL factory under a genuinely colliding "
        + "classpath -- refuses at true, proceeds at false on the same "
        + "classpath, and a healthy classpath still starts -- so it is proven "
        + "read from the Configuration and not merely declared. The decision "
        + "function is also probed below"));
  }

  // ---------------------------------------------------------------- helpers

  static Configuration base() {
    Configuration c = new Configuration();
    c.set("fs.s3a.endpoint", endpoint);
    c.set("fs.s3a.endpoint.region", "us-east-1");
    c.setBoolean("fs.s3a.path.style.access", true);
    c.set("fs.s3a.access.key", "s"); c.set("fs.s3a.secret.key", "s");
    c.setInt("fs.s3a.bucket.probe", 0);
    c.set("fs.s3a.change.detection.mode", "none");
    c.set("fs.s3a.input.stream.type", "custom");
    c.set("fs.s3a.input.stream.custom.factory", FlintStreamFactory.class.getName());
    c.set(FlintStreamFactory.TIER_URI, tier);
    // Each arm needs its OWN factory, or Hadoop's FileSystem cache hands back
    // the one built with the previous arm's settings and every probe below
    // compares a configuration against itself.
    c.set("fs.s3a.impl.disable.cache", "true");
    return c;
  }

  static io.lettuce.core.api.StatefulRedisConnection<byte[], byte[]> rc;

  static int chunkKeys() {
    return ai.crestway.flintaccel.TierScan.keys(rc, "c2/*").size();
  }

  static void flush() { rc.sync().flushall(); }

  /** One read through path 1 under the given configuration. */
  static byte[] read(Configuration c, String key, long off, int len) throws Exception {
    try (FileSystem fs = FileSystem.get(URI.create("s3a://bucket/"), c);
         FSDataInputStream in = fs.open(new Path("s3a://bucket/" + key))) {
      byte[] b = new byte[len];
      in.readFully(off, b, 0, len);
      return b;
    }
  }

  /** Chunk keys left in the tier by one cold read under this configuration. */
  static int coldKeys(Configuration c, String key, long off, int len) throws Exception {
    flush();
    byte[] got = read(c, key, off, len);
    check(Arrays.equals(got, expect(key, off, len)),
        "  (bytes correct under this arm)");
    return chunkKeys();
  }

  static int originGets() throws Exception { return stat("gets"); }
  static int originHeads() throws Exception { return stat("heads"); }

  static int stat(String field) throws Exception {
    String body = new String(URI.create(endpoint + "/__stats").toURL()
        .openStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    Matcher m = Pattern.compile("\"" + field + "\"\\s*:\\s*(\\d+)").matcher(body);
    return m.find() ? Integer.parseInt(m.group(1)) : -1;
  }

  /** The fixture's oracle, identical to S3aSuite's. */
  static byte[] expect(String key, long off, int len) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      long abs = off + i;
      md.reset();
      out[i] = md.digest((key + ":0:" + (abs / 16)).getBytes("UTF-8"))[(int) (abs % 16)];
    }
    return out;
  }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    tier = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    slowTier = args.length > 2 ? args[2] : null;
    if (slowTier == null) {
      System.out.println("[FAIL] no slow-tier URI given; the budget probe cannot "
          + "run and a skipped probe is how BUG-0066 stayed invisible");
      System.exit(1);
    }

    io.lettuce.core.RedisClient cli = io.lettuce.core.RedisClient.create(tier);
    rc = cli.connect(new io.lettuce.core.codec.ByteArrayCodec());
    try {
      registry();
      structural();
      behavioural();
    } finally {
      rc.close();
      cli.shutdown();
    }
    System.out.println("\nCONFIG REACH SUITE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }

  // ------------------------------------------------------- layer 1: registry

  /** Every declared key, found by reflection, must be classified. */
  static List<Field> declaredKeys() throws Exception {
    List<Field> out = new ArrayList<>();
    for (Field f : FlintStreamFactory.class.getDeclaredFields()) {
      if (!Modifier.isStatic(f.getModifiers()) || f.getType() != String.class) continue;
      f.setAccessible(true);
      Object v = f.get(null);
      if (v instanceof String s && s.startsWith("fs.s3a.flint.")) out.add(f);
    }
    return out;
  }

  static void registry() throws Exception {
    List<Field> keys = declaredKeys();
    check(keys.size() >= 10,
        "reflection found the declared keys (" + keys.size() + ")");
    List<String> unclassified = new ArrayList<>();
    for (Field f : keys) if (!REGISTRY.containsKey(f.getName())) unclassified.add(f.getName());
    check(unclassified.isEmpty(),
        "every declared fs.s3a.flint.* key is classified"
            + (unclassified.isEmpty() ? "" : " -- UNCLASSIFIED: " + unclassified
               + ". Add it to REGISTRY with a probe, or say why it has none."));
    // And the reverse: a registry entry for a key that no longer exists is a
    // stale exemption, which is how an allowlist starts lying.
    Set<String> live = new HashSet<>();
    for (Field f : keys) live.add(f.getName());
    List<String> stale = new ArrayList<>(REGISTRY.keySet());
    stale.removeAll(live);
    check(stale.isEmpty(), "no stale registry entries" + (stale.isEmpty() ? "" : " -- " + stale));
  }

  // ----------------------------------------------------- layer 2: structural

  /**
   * Every declared constant must be referenced somewhere other than its own
   * declaration. This IS BUG-0066's shape: MAX_OBJECT appeared exactly once.
   */
  static void structural() throws Exception {
    // java.nio.file.Path spelled out: org.apache.hadoop.fs.Path owns the
    // simple name in this file, and the two are silently different types.
    java.nio.file.Path src = java.nio.file.Paths.get(
        "src/main/java/ai/crestway/flintaccel/s3a/FlintStreamFactory.java");
    if (!Files.exists(src)) {
      src = java.nio.file.Paths.get(
          "jvm-spike/src/main/java/ai/crestway/flintaccel/s3a/FlintStreamFactory.java");
    }
    check(Files.exists(src), "found the factory source to scan (" + src + ")");
    if (!Files.exists(src)) return;
    String text = Files.readString(src);
    List<String> unread = new ArrayList<>();
    for (Field f : declaredKeys()) {
      // Count references to the CONSTANT, not to the string: the string also
      // appears in the README and in this suite, and matching it would let a
      // key pass on being mentioned rather than on being used.
      Matcher m = Pattern.compile("\\b" + Pattern.quote(f.getName()) + "\\b").matcher(text);
      int n = 0; while (m.find()) n++;
      if (n < 2) unread.add(f.getName() + " (" + n + " reference)");
    }
    check(unread.isEmpty(),
        "every declared key is USED, not just declared"
            + (unread.isEmpty() ? "" : " -- DECLARED AND NEVER READ: " + unread));
  }

  // ---------------------------------------------------- layer 3: behavioural

  static void behavioural() throws Exception {
    final String K = "data/000002.bin";
    final String K2 = "data/000003.bin";

    // ---- MAX_PART, MAX_OBJECT: a 1-byte cap must stop caching.
    for (String[] c : new String[][]{
        {FlintStreamFactory.MAX_PART, "MAX_PART"},
        {FlintStreamFactory.MAX_OBJECT, "MAX_OBJECT"}}) {
      Configuration capped = base();
      capped.setLong(c[0], 1);
      int cappedKeys = coldKeys(capped, K, 300_000, 4096);
      int normalKeys = coldKeys(base(), K, 300_000, 4096);
      check(cappedKeys == 0 && normalKeys > 0,
          c[1] + " reaches the client: " + cappedKeys + " chunk keys at a 1-byte "
              + "cap against " + normalKeys + " without it");
    }

    // ---- CHUNK_BYTES: a different grid must land a different number of keys.
    Configuration wide = base();
    wide.setInt(FlintStreamFactory.CHUNK_BYTES, 256 * 1024);
    int wideKeys = coldKeys(wide, K2, 0, 512 * 1024);
    int narrowKeys = coldKeys(base(), K2, 0, 512 * 1024);
    check(wideKeys > 0 && narrowKeys > wideKeys,
        "CHUNK_BYTES reaches the client: the same 512 KiB read lands "
            + narrowKeys + " chunks on the 64 KiB grid and " + wideKeys
            + " on a 256 KiB one");

    // ---- TIER_URI: a tier that is not there must not break reads, and must
    // not cache. The control is that the SAME read against the real tier does.
    Configuration dead = base();
    dead.set(FlintStreamFactory.TIER_URI, DEAD_TIER);
    flush();
    byte[] got = read(dead, K, 500_000, 4096);
    check(Arrays.equals(got, expect(K, 500_000, 4096)),
        "TIER_URI reaches the client: a dead tier still returns correct bytes");
    check(chunkKeys() == 0,
        "  and cached nothing in the REAL tier (" + chunkKeys() + " keys), so it "
            + "was the given URI that was used and not the default");

    // ---- RECONNECT_MS: a dead tier must not cost a TCP handshake per read.
    //
    // Counted, not timed. The rate limit is a duration, but asserting on
    // elapsed time would be a flake on a loaded box -- so what is checked is
    // the number of CONNECT ATTEMPTS across a fixed number of reads, which the
    // rate limit is the only thing bounding. A huge budget must attempt fewer
    // times than a tiny one over the same reads; without that comparison the
    // check passes on a client that never retries at all.
    Configuration slowRetry = base();
    slowRetry.set(FlintStreamFactory.TIER_URI, DEAD_TIER);
    slowRetry.setLong(FlintStreamFactory.RECONNECT_MS, 600_000);
    long slowAttempts = attemptsOver(slowRetry, K, RETRY_READS);

    Configuration fastRetry = base();
    fastRetry.set(FlintStreamFactory.TIER_URI, DEAD_TIER);
    fastRetry.setLong(FlintStreamFactory.RECONNECT_MS, 1);
    long fastAttempts = attemptsOver(fastRetry, K, RETRY_READS);

    check(slowAttempts >= 1,
        "  armed: the dead-tier path DID try to connect (" + slowAttempts
            + " attempts), so a low count is a rate limit and not a no-op");
    check(fastAttempts > slowAttempts,
        "RECONNECT_MS reaches the client: " + RETRY_READS + " reads against a dead tier cost "
            + fastAttempts + " connect attempts at a 1 ms retry budget and only "
            + slowAttempts + " at a 10 min one");

    // ---- TIER_BUDGET: a budget under the tier's latency must degrade.
    // Needs the slow proxy: nothing else makes a budget observable.
    Configuration tight = base();
    tight.set(FlintStreamFactory.TIER_URI, slowTier);
    tight.setLong(FlintStreamFactory.TIER_BUDGET, 1);
    Configuration loose = base();
    loose.set(FlintStreamFactory.TIER_URI, slowTier);
    loose.setLong(FlintStreamFactory.TIER_BUDGET, 30_000);
    flush();
    read(loose, K2, 900_000, 4096);          // populate through the slow proxy
    int warmed = chunkKeys();
    check(warmed > 0, "  (armed: the slow tier is reachable and was populated, "
        + warmed + " keys)");
    int before = originGets();
    read(tight, K2, 900_000, 4096);
    int tightGets = originGets() - before;
    before = originGets();
    read(loose, K2, 900_000, 4096);
    int looseGets = originGets() - before;
    check(tightGets > looseGets,
        "TIER_BUDGET reaches the client: a 1 ms budget against the same warm "
            + "slow tier goes to the origin (" + tightGets + " GETs) where a "
            + "30 s budget does not (" + looseGets + ")");

    // ---- META_TTL / IMMUTABLE / META_TTL_IMM.
    //
    // Observed as the METADATA KEY'S OWN EXPIRY, not as a re-HEAD of the
    // origin. Counting HEADs cannot work: S3A issues its own HEAD on every
    // open() for file status, so both arms HEAD every time and the signal is
    // swamped. The first draft did count HEADs -- both TTL arms then "revalidated",
    // which failed the two checks that need a difference and made the third
    // pass VACUOUSLY, on a condition that was true no matter what the setting
    // did. The TTL is set on the m1/ key, so the key's survival is the thing
    // the setting actually controls.
    Configuration shortTtl = base();
    shortTtl.setLong(FlintStreamFactory.META_TTL, SHORT_TTL_S);
    Configuration longTtl = base();
    longTtl.setLong(FlintStreamFactory.META_TTL, 3600);
    check(!metaSurvives(shortTtl, K) && metaSurvives(longTtl, K),
        "META_TTL reaches the client: the metadata key is gone "
            + "under a " + SHORT_TTL_S + " s TTL and still there under a 3600 s one");

    // immutable=true with the SAME 1 s ordinary TTL must keep the key, because
    // the immutable TTL (86400 s by default) is the one that applies.
    Configuration imm = base();
    imm.setLong(FlintStreamFactory.META_TTL, SHORT_TTL_S);
    imm.setBoolean(FlintStreamFactory.IMMUTABLE, true);
    check(metaSurvives(imm, K),
        "IMMUTABLE reaches the client: the same " + SHORT_TTL_S + " s TTL stops expiring the "
            + "metadata once the object is declared immutable");

    // and the immutable TTL is the one in force: set IT to 1 s and the key
    // expires again, with the ordinary TTL left long.
    Configuration immShort = base();
    immShort.setLong(FlintStreamFactory.META_TTL, 3600);
    immShort.setBoolean(FlintStreamFactory.IMMUTABLE, true);
    immShort.setLong(FlintStreamFactory.META_TTL_IMM, SHORT_TTL_S);
    check(!metaSurvives(immShort, K),
        "META_TTL_IMM reaches the client: with immutable set, a " + SHORT_TTL_S + " s IMMUTABLE "
            + "TTL expires the key though the ordinary TTL is 3600 s");

    // ---- SHIM_FAIL_FAST: the decision it steers, which is as far as an
    // in-process test can reach. Said plainly rather than counted as coverage.
    check(FlintStreamFactory.failFastMessage(
              ShimGuard.State.COLLISION, "two copies", true) != null,
        "SHIM_FAIL_FAST steers the decision: a collision throws when it is on");
    check(FlintStreamFactory.failFastMessage(
              ShimGuard.State.COLLISION, "two copies", false) == null,
        "  and proceeds when it is off");
    check(FlintStreamFactory.failFastMessage(
              ShimGuard.State.SINGLE, "one copy", true) == null,
        "  control -- a healthy classpath does not throw with it on, so the "
            + "flag is not simply always throwing");
  }

  /**
   * Connect attempts the lazy fallback made across {@code reads} reads against
   * a tier that is not there.
   *
   * <p>Reaches the factory through {@code LAST_BOUND}: S3A hands back a
   * FileSystem, and the handle that counts the attempts lives on the factory
   * behind it.
   */
  static long attemptsOver(Configuration c, String key, int reads) throws Exception {
    FlintStreamFactory.LAST_BOUND = null;
    try (FileSystem fs = FileSystem.get(URI.create("s3a://bucket/"), c)) {
      for (int i = 0; i < reads; i++) {
        try (FSDataInputStream in = fs.open(new Path("s3a://bucket/" + key))) {
          byte[] b = new byte[4096];
          in.readFully(200_000 + i * 4096L, b, 0, 4096);
        }
      }
      FlintStreamFactory f = FlintStreamFactory.LAST_BOUND;
      if (f == null || f.lazy == null) return -1;      // never fell back: a FAIL
      return f.lazy.connectAttempts.get();
    }
  }

  /**
   * Is the object's metadata key still in the tier a second after the read
   * that wrote it?
   *
   * <p>The TTL under test is the one set on that key, so its survival is the
   * direct observable. Armed on every call: the key must EXIST immediately
   * after the read, or the arm proves nothing and says so rather than
   * reporting an expiry that never happened.
   */
  static boolean metaSurvives(Configuration c, String key) throws Exception {
    flush();
    read(c, key, 100_000, 4096);
    int written = ai.crestway.flintaccel.TierScan.keys(rc, "m1/*").size();
    check(written > 0, "  (armed: the read cached metadata, " + written + " m1/ keys)");
    Thread.sleep(PAST_TTL_MS);                 // past the short TTL, far under 3600
    return ai.crestway.flintaccel.TierScan.keys(rc, "m1/*").size() > 0;
  }
}
