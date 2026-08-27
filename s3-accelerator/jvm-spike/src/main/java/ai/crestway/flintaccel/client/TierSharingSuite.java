// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.util.*;

/**
 * BUG-0057: building a client per FileSystem exhausts the JVM.
 *
 * <p>Hadoop keys {@code FileSystem.CACHE} on scheme+authority+UGI, so normally
 * {@code initialize()} runs once per bucket. Under
 * {@code fs.s3a.impl.disable.cache=true} it runs per {@code FileSystem.get},
 * the caller owns the instance, and Spark does not close the ones it opens for
 * {@code read.parquet}. Measured under Spark 4.0.4: after ~30 table reads,
 * {@code IllegalStateException: failed to create a child event loop}, with
 * {@code ulimit -n} 65535 and {@code ulimit -u} unlimited — so not a tight
 * limit, just too many event loop groups.
 *
 * <p>Any heavyweight FileSystem leaks under that setting, stock
 * {@code S3AFileSystem} included. The reason it is ours to fix is that we
 * allocate disproportionately more per instance, so we exhaust first, under a
 * setting we do not control.
 *
 * <p>The assertion is thread growth against instance count, not a raw
 * threshold: thread counts move with cores, GC and the SDK's own pools, and a
 * fixed number would be either vacuous on a big box or flaky on a small one.
 * What must hold is that the twelfth client costs about what the second did.
 */
public final class TierSharingSuite {

  static boolean ok = true;
  static final int N = 12;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static int threads() {
    return Thread.getAllStackTraces().size();
  }

  /** Threads whose name matches a pool we care about, for the diagnosis line. */
  static Map<String, Integer> byPool() {
    Map<String, Integer> m = new TreeMap<>();
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      String n = t.getName();
      // Group by name with the instance number stripped, so a pool that grows
      // per client is visible as a count rather than as N distinct names.
      String k = n.replaceAll("[-_]?\\d+", "#");
      m.merge(k, 1, Integer::sum);
    }
    return m;
  }

  static TierSupport build(String endpoint, String tier) {
    return build(endpoint, tier, false);
  }

  static TierSupport build(String endpoint, String tier, boolean immutable) {
    Map<String, String> p = new HashMap<>();
    if (immutable) p.put("flint.immutable", "true");
    p.put("flint.tier.uri", tier);
    p.put("s3.endpoint", endpoint);
    p.put("s3.path-style-access", "true");
    p.put("s3.access-key-id", "i");
    p.put("s3.secret-access-key", "i");
    p.put("s3.region", "us-east-1");
    p.put("client.region", "us-east-1");
    return TierSupport.build(TierSupport.from(p));
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String tier = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";

    int base = threads();
    List<TierSupport> held = new ArrayList<>();

    held.add(build(endpoint, tier));
    Thread.sleep(300);
    int afterOne = threads() - base;

    for (int i = 1; i < N; i++) held.add(build(endpoint, tier));
    Thread.sleep(500);
    int afterAll = threads() - base;
    int perInstanceWouldBe = afterOne * N;

    System.out.printf("     threads: base %d, +%d after 1 mount, +%d after %d%n",
        base, afterOne, afterAll, N);
    System.out.println("     pools: " + byPool());

    // The claim is connection sharing; threads are the evidence for it.
    // Asserting only on threads would pass for any change that happened to
    // allocate less for an unrelated reason.
    check(TierConnections.redisCreated.get() == 1
          && TierConnections.redisReused.get() == N - 1,
          "one tier connection for " + N + " mounts: created="
          + TierConnections.redisCreated.get() + " reused="
          + TierConnections.redisReused.get());
    check(TierConnections.s3Created.get() == 1,
          "and one S3 client for " + N + " mounts: s3Created="
          + TierConnections.s3Created.get());

    // NOT FLAT, DELIBERATELY. Flat needs the whole TierSupport pooled, which
    // shares AAL's object cache -- see the class comment. This bound is what
    // sharing the connections buys, and the connections are the resource
    // BUG-0057 actually ran out of.
    check(afterAll < perInstanceWouldBe * 3 / 4,
          "thread growth is well below per-instance: +" + afterAll + " for " + N
          + ", against about +" + perInstanceWouldBe + " with nothing shared");

    // The constraint that killed full pooling, asserted so it stays killed.
    TierSupport a = held.get(0), b = held.get(1);
    check(a != b, "each mount keeps its OWN TierSupport");
    check(a.caching != b.caching,
          "and its own AAL factory -- the object that must not be shared, "
          + "because a stale cached length reads as truncation");

    for (TierSupport t : held) t.close();
    Thread.sleep(700);
    int afterClose = threads() - base;
    check(afterClose <= afterOne,
          "closing every mount releases the shared connections: +" + afterClose
          + " threads remain");
    check(TierConnections.pooled() == 0,
          "and the connection pool is empty afterwards: " + TierConnections.pooled()
          + " entries. A pool that never drains is the leak with extra steps");

    System.exit(ok ? 0 : 1);
  }
}
