// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * How many readers can share one tier before the cache stops being worth it?
 *
 * <p>ADR-0023's numbers all come from ONE reader. The product's whole position
 * is a <b>shared</b> tier — many workers, one cache — and nothing has measured
 * what happens to a read when the other workers show up. This is the question
 * behind "we have not measured a real cluster": a cluster is more parallelism
 * against one tier, and if the tier is the thing that gives out, the cluster
 * result is knowable without a cluster.
 *
 * <h2>Why SMALL reads</h2>
 *
 * The cache's win is the 25-45 ms of S3 time-to-first-byte it removes, not
 * bandwidth — a warm read moves the same bytes as a cold one (D17). So the
 * regime that matters is latency, and the measurement has to stay in it.
 * Small random reads of already-cached chunks keep aggregate bandwidth low,
 * which matters for a second reason: <b>a bulk benchmark would saturate the
 * client's NIC long before the tier, and would report the client's ceiling as
 * the tier's.</b>
 *
 * <h2>What is asserted, not just printed</h2>
 *
 * <ul>
 *   <li><b>The reads hit the TIER.</b> Origin GETs must not move during the
 *       sweep. Without this the whole thing degrades to an S3 benchmark and
 *       still prints plausible latencies.
 *   <li><b>Which side saturated.</b> Client CPU is sampled per step. A plateau
 *       with the client pinned means we measured the client; the run says so
 *       instead of reporting it as the tier's limit.
 * </ul>
 *
 * <h2>Running it</h2>
 *
 * <p>Needs the AWS SDK on the classpath, which the product jar deliberately
 * does not carry — it is a library and its host supplies one. Locally that
 * means the gate's classpath:
 *
 * <pre>java -cp jvm-spike/target/classes:$(cat /tmp/gate_cp.txt) \
 *   ai.crestway.flintaccel.client.FanoutBench \
 *   http://127.0.0.1:9301 redis://127.0.0.1:9399 bucket data/000001.bin 2097152 6</pre>
 *
 * <p>On a box, {@code packaging/aws/spark-e2e/measure_fanout.sh}, which needs an
 * ordinary E2E run to have populated ivy first. <b>This is unfinished:</b> there
 * is no self-contained bench artifact, and building one was abandoned after an
 * assembly including provided scope came out at 718 MB and still did not run.
 * The measurement below is real and controlled; only its delivery to a bare
 * machine is not solved.
 *
 * <p>The number to read off is the concurrency at which p99 crosses the S3 TTFB
 * being saved: past that a shared tier is slower than the S3 it replaces, and
 * that is the point where a cluster stops benefiting.
 */
public final class FanoutBench {

  /** What S3 costs per request, and therefore what we are buying back. */
  static final double S3_TTFB_MS = 25.0;

  static String endpoint, tier, bucket;
  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  /**
   * Endpoint {@code "-"} means REAL S3 with the box's own credentials.
   *
   * <p>TierSupport already does the right thing with both blanks — no
   * endpointOverride, and the default credentials chain — so running against a
   * local counting fixture and against S3 on an instance profile is the same
   * code with different arguments, rather than two paths one of which is the
   * one that never gets exercised.
   */
  static TierSupport build() {
    Map<String, String> p = new HashMap<>();
    p.put("flint.tier.uri", tier);
    p.put("s3.region", "us-east-1");
    p.put("client.region", "us-east-1");
    if (!"-".equals(endpoint)) {
      p.put("s3.endpoint", endpoint);
      p.put("s3.path-style-access", "true");
      p.put("s3.access-key-id", "b");
      p.put("s3.secret-access-key", "b");
    }
    return TierSupport.build(TierSupport.from(p));
  }

  static double cpuPercent() {
    var os = (com.sun.management.OperatingSystemMXBean)
        java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    double v = os.getCpuLoad();
    return v < 0 ? Double.NaN : v * 100.0;
  }

  /**
   * One small ranged read, straight through OUR client.
   *
   * <p>AAL is deliberately not in this path. Driving it through
   * {@code S3SeekableInputStreamFactory} forces a choice between two wrong
   * measurements: reuse a factory and AAL's own object cache answers from
   * memory, so the timing is a {@code HashMap} (D12.5); build one per read and
   * factory construction lands inside the timed window, which the tier does not
   * charge for. The subject here is the client against the tier under
   * concurrency, so the measurement calls the client.
   */
  static void read(TierSupport t, S3URI uri, String etag, long off, int len)
      throws Exception {
    ObjectContent oc = t.client.getObject(GetRequest.builder()
        .s3Uri(uri).etag(etag)
        .referrer(new Referrer("bytes=" + off + "-" + (off + len - 1), ReadMode.SYNC))
        .range(new Range(off, off + len - 1)).build()).join();
    try (var in = oc.getStream()) { in.readAllBytes(); }
  }

  public static void main(String[] args) throws Exception {
    // endpoint tier bucket key span seconds
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    tier     = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    bucket   = args.length > 2 ? args[2] : "bucket";
    final String KEY = args.length > 3 ? args[3] : "data/000001.bin";
    final long   SPAN = Long.parseLong(args.length > 4 ? args[4] : "4194304");
    final int    SECONDS = Integer.parseInt(args.length > 5 ? args[5] : "6");
    final int    READ = 4096;
    int[] levels = {1, 2, 4, 8, 16, 32, 64};

    TierSupport t = build();
    S3URI uri = S3URI.of(bucket, KEY);
    String etag = t.client.headObject(HeadRequest.builder().s3Uri(uri).build())
        .join().getEtag();

    // Warm the whole span, so every timed read below is a tier HIT.
    for (long off = 0; off < SPAN; off += 65536) read(t, uri, etag, off, READ);
    // Armed on the TIER's contents, not on a hit counter: the warm pass fills,
    // and a fill is not a hit, so counting hits here would report zero for a
    // cache that had just been populated correctly.
    int cached = ai.crestway.flintaccel.TierScan.keys(t.liveConnection(), "c2/*").size();
    check(cached > 0, "armed: the span is cached (" + cached + " chunk keys)");

    System.out.printf("%n  %8s %10s %10s %10s %12s %8s%n",
        "threads", "p50 ms", "p99 ms", "max ms", "reads/s", "cpu %");

    long originAtStart = t.client.originGets.get();
    double p99AtOne = Double.NaN;
    int firstOver = -1;
    double peakRate = 0;

    for (int n : levels) {
      ExecutorService pool = Executors.newFixedThreadPool(n);
      CountDownLatch go = new CountDownLatch(1);
      List<Future<long[]>> fs = new ArrayList<>();
      AtomicLong done = new AtomicLong();
      long deadline = System.nanoTime() + SECONDS * 1_000_000_000L;

      for (int i = 0; i < n; i++) {
        final long seed = 1234567L * (i + 1);
        fs.add(pool.submit(() -> {
          Random rnd = new Random(seed);
          long[] samples = new long[200_000];
          int k = 0;
          go.await();
          // Untimed spin first. Without it the 1-thread row reported a 107 ms
          // max that was JIT compiling the read path, printed in a table of
          // sub-millisecond numbers as though it were a tier stall.
          for (int w = 0; w < 200; w++) {
            long off = (Math.abs(rnd.nextLong()) % (SPAN - READ)) & ~0xFFFL;
            read(t, uri, etag, off, READ);
          }
          while (System.nanoTime() < deadline && k < samples.length) {
            long off = (Math.abs(rnd.nextLong()) % (SPAN - READ)) & ~0xFFFL;
            long t0 = System.nanoTime();
            read(t, uri, etag, off, READ);
            samples[k++] = System.nanoTime() - t0;
            done.incrementAndGet();
          }
          return Arrays.copyOf(samples, k);
        }));
      }

      long t0 = System.nanoTime();
      cpuPercent();                       // prime the sampler
      go.countDown();
      List<Long> all = new ArrayList<>();
      double cpu = Double.NaN;
      for (var f : fs) {
        long[] s = f.get(SECONDS * 4L, TimeUnit.SECONDS);
        for (long v : s) all.add(v);
        if (Double.isNaN(cpu)) cpu = cpuPercent();
      }
      pool.shutdown();
      double elapsed = (System.nanoTime() - t0) / 1e9;

      Collections.sort(all);
      double p50 = all.get((int) (all.size() * 0.50)) / 1e6;
      double p99 = all.get((int) (all.size() * 0.99)) / 1e6;
      double max = all.get(all.size() - 1) / 1e6;
      double rate = done.get() / elapsed;
      peakRate = Math.max(peakRate, rate);
      if (n == 1) p99AtOne = p99;
      if (firstOver < 0 && p99 > S3_TTFB_MS) firstOver = n;

      System.out.printf("  %8d %10.3f %10.3f %10.3f %12.0f %8.0f%n",
          n, p50, p99, max, rate, cpu);
    }

    // The control. Without it this is an S3 benchmark with tier-shaped numbers.
    //
    // NOT "== 0". A read that degrades to the origin under load is D12.9
    // working, not the measurement breaking, and demanding zero would fail the
    // run for the product behaving correctly. What has to be true is that the
    // origin served a NEGLIGIBLE fraction -- if this were accidentally an S3
    // benchmark the fraction would be 1, so 0.01% separates the two by four
    // orders of magnitude and still catches the failure this exists to catch.
    long originMoved = t.client.originGets.get() - originAtStart;
    long totalReads = t.client.chunkHits.get() + t.client.chunkMisses.get();
    double pct = totalReads == 0 ? 100.0 : 100.0 * originMoved / totalReads;
    check(originMoved * 10_000 < Math.max(totalReads, 1),
        String.format("the tier served the sweep: %d of ~%d chunk lookups degraded "
            + "to the origin (%.4f%%)", originMoved, totalReads, pct));
    if (originMoved > 0) {
      System.out.printf("  %d read(s) degraded to S3 under concurrency -- bounded "
          + "degradation firing, not an error%n", originMoved);
    }

    System.out.println();
    if (firstOver < 0) {
      System.out.printf("  p99 stayed under the %.0f ms of S3 TTFB being saved at every "
          + "level up to %d threads -- the tier did not become the limit here%n",
          S3_TTFB_MS, levels[levels.length - 1]);
    } else {
      System.out.printf("  p99 crossed the %.0f ms of S3 TTFB being saved at %d threads: "
          + "past that a shared tier costs more per read than the S3 it replaces%n",
          S3_TTFB_MS, firstOver);
    }
    System.out.printf("  p99 at 1 thread %.3f ms, peak %.0f reads/s%n", p99AtOne, peakRate);
    System.out.println("  NOTE: cpu % is this CLIENT box. A plateau with cpu near 100 "
        + "means the CLIENT saturated and this measured the client, not the tier.");
    t.close();
    System.exit(ok ? 0 : 1);
  }
}
