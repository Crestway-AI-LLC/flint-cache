// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * The counters, read the way an OPERATOR reads them.
 *
 * Every value here is fetched through the platform MBeanServer by object name,
 * never from the object this suite registered. Reading your own reference
 * proves the getters compile; it proves nothing about whether the bean is
 * registered, reachable, or named something a JMX exporter will find -- which
 * is the entire question.
 */
public final class MetricsSuite {

  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static void note(String s) { System.out.println("       " + s); }

  static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();

  static Object attr(ObjectName n, String a) throws Exception {
    return MBS.getAttribute(n, a);
  }

  static TierSupport build(String endpoint, String tier, boolean cacheKms) {
    Map<String, String> p = new HashMap<>();
    p.put("flint.tier.uri", tier);
    p.put("s3.endpoint", endpoint);
    p.put("s3.path-style-access", "true");
    p.put("s3.access-key-id", "m");
    p.put("s3.secret-access-key", "m");
    p.put("s3.region", "us-east-1");
    if (cacheKms) p.put("flint.cache.sse-kms", "true");
    return TierSupport.build(TierSupport.from(p));
  }

  /**
   * A FRESH AAL factory per read (D12.5).
   *
   * Reusing one factory made the first version of this suite report 0 hits
   * after two reads of the same object: AAL's own in-process cache answered
   * the second one and our client never saw it. Measuring a cache through a
   * cache in front of it measures the one in front.
   */
  static void read(TierSupport t, String key, int len) throws Exception {
    try (var f = new S3SeekableInputStreamFactory(t.client,
             software.amazon.s3.analyticsaccelerator
                 .S3SeekableInputStreamConfiguration.DEFAULT);
         var in = f.createStream(S3URI.of("bucket", key))) {
      byte[] b = new byte[len];
      in.seek(0);
      in.read(b, 0, len);
    }
  }

  public static void main(String[] args) throws Exception {
    String plain = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String kmsEp = args.length > 1 ? args[1] : "http://127.0.0.1:9530";
    String tier  = args.length > 2 ? args[2] : "redis://127.0.0.1:9399";
    final String KEY = "data/000001.bin";

    TierSupport t = build(plain, tier, false);
    check(t.metrics != null, "a bean was registered at all");
    ObjectName n = t.metrics.objectName();

    // -- findable the way an exporter finds things: by PATTERN, not by the
    //    exact name we happen to hold a reference to.
    Set<ObjectName> found = MBS.queryNames(
        new ObjectName("ai.crestway.flintaccel:type=Cache,*"), null);
    check(found.contains(n),
        "and it is discoverable by wildcard query, as a JMX exporter would ("
        + found.size() + " bean(s))");

    // -- an idle cache must not look like a broken one -----------------------
    double idle = (Double) attr(n, "ChunkHitRatePercent");
    check(Double.isNaN(idle),
        "before any read the hit rate is NaN, not 0% -- idle and broken must "
        + "not render identically");

    // -- armed: the numbers actually move ------------------------------------
    long hits0 = (Long) attr(n, "ChunkHits");
    read(t, KEY, 200_000);
    read(t, KEY, 200_000);
    long hits1 = (Long) attr(n, "ChunkHits");
    check(hits1 > hits0,
        "armed: ChunkHits moves through the MBeanServer (" + hits0 + " -> " + hits1 + ")");
    double rate = (Double) attr(n, "ChunkHitRatePercent");
    check(rate > 0 && rate <= 100, String.format("hit rate is a real number (%.1f%%)", rate));
    check(((Long) attr(n, "OriginGets")) > 0, "OriginGets is populated");

    String sum = (String) attr(n, "Summary");
    note(sum);
    check(sum.contains("hit rate"), "Summary is one pasteable line");
    check(!sum.contains("BYPASSED") && !sum.contains("breaker"),
        "negative control: a healthy run's summary mentions NEITHER failure mode "
        + "-- a summary that always lists everything trains the reader to skip it");

    // -- the silent-zero-acceleration case is now visible ---------------------
    // An SSE-KMS bucket bypasses the cache entirely. Before this bean the
    // symptom was "the cache does nothing" with no cause anywhere.
    // Fresh tier for the KMS client, and the reason is a finding rather than
    // hygiene. Both fixtures serve the SAME logical object -- same bucket, same
    // key, same bytes -- and cache keys are not scoped by ENDPOINT: metadata
    // lives at m1/s3://bucket/key and chunks are content-addressed by ETag. So
    // the plain client's entries answered for the KMS client and detection
    // never ran.
    //
    // In this suite that is an artefact: one URI cannot be both encrypted and
    // not. In a deployment where one tier is shared across two DIFFERENT S3
    // endpoints holding overlapping bucket/key names, it is not an artefact --
    // recorded as an open item rather than papered over here.
    java.util.List<byte[]> before = t.conn.sync().keys("*".getBytes());
    t.conn.sync().flushall();
    check(!before.isEmpty(), "armed: the plain run had populated the tier ("
        + before.size() + " keys) before it was cleared for the KMS run");

    TierSupport k = build(kmsEp, tier, false);
    ObjectName kn = k.metrics.objectName();
    check(!kn.equals(n), "a second client gets its OWN bean, not a silent overwrite");
    read(k, KEY, 200_000);
    long bypassed = (Long) attr(kn, "SseKmsBypassed");
    check(bypassed > 0, "SseKmsBypassed explains a KMS bucket's missing acceleration ("
        + bypassed + ")");
    String ksum = (String) attr(kn, "Summary");
    note(ksum);
    check(ksum.contains("flint.cache.sse-kms=true"),
        "and the summary names the OPT-IN, so the reader has the next step and "
        + "not merely a diagnosis");

    // -- teardown actually deregisters ---------------------------------------
    t.close();
    k.close();
    Set<ObjectName> after = MBS.queryNames(
        new ObjectName("ai.crestway.flintaccel:type=Cache,*"), null);
    check(!after.contains(n) && !after.contains(kn),
        "close() unregisters both beans -- a long-lived JVM building clients per "
        + "job would otherwise leak one every time");

    System.out.println(ok ? "METRICS SUITE PASSED" : "METRICS SUITE FAILED");
    System.exit(ok ? 0 : 1);
  }
}
