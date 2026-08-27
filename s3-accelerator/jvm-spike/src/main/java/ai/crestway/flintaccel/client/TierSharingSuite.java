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

    System.out.printf("     threads: base %d, +%d after 1 client, +%d after %d%n",
        base, afterOne, afterAll, N);
    System.out.println("     pools: " + byPool());
    // Per-instance allocation would put afterAll near N * afterOne. Sharing
    // puts it near afterOne. The midpoint is a wide, unambiguous gap; anything
    // in it means only part of the stack is shared and the rest still scales.
    int perInstanceWouldBe = afterOne * N;
    check(afterAll < perInstanceWouldBe / 2,
          "thread growth is SUBLINEAR in client count: +" + afterAll + " for " + N
          + " clients, where per-instance allocation would be about +"
          + perInstanceWouldBe);

    check(afterOne == 0 || afterAll <= afterOne * 2,
          "the " + N + "th client costs about what the 1st did: +" + afterOne
          + " -> +" + afterAll);

    // Thread counts are evidence; the pool counters are the claim. Asserting
    // only on threads would pass for any change that happened to allocate less.
    check(TierSupport.created.get() == 1 && TierSupport.reused.get() == N - 1,
          "identical mounts share ONE instance: created=" + TierSupport.created.get()
          + " reused=" + TierSupport.reused.get() + " for " + N + " builds");
    check(TierConnections.redisCreated.get() == 1,
          "and one tier connection underneath: redisCreated="
          + TierConnections.redisCreated.get());

    // The saving must not be bought with a correctness bug. flint.immutable
    // changes what a read DOES -- it stops the revalidation HEADs -- so a mount
    // that sets it must not be handed the instance built for one that did not.
    long before = TierSupport.created.get();
    TierSupport diff = build(endpoint, tier, true);
    check(TierSupport.created.get() == before + 1,
          "a DIFFERENTLY configured mount gets its own instance, not the pooled one");
    check(diff != held.get(0), "and it is a different object");
    diff.close();

    for (TierSupport t : held) t.close();
    Thread.sleep(700);
    int afterClose = threads() - base;
    check(afterClose <= afterOne,
          "closing every client releases the shared stack: +" + afterClose
          + " threads remain");
    check(TierConnections.pooled() == 0,
          "and the connection pool is empty afterwards: " + TierConnections.pooled()
          + " entries. A pool that never drains is the leak with extra steps");

    System.exit(ok ? 0 : 1);
  }
}
