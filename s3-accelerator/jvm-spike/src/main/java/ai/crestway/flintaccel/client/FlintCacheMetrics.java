// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/** Reads a live client's counters; registers and unregisters itself. */
public final class FlintCacheMetrics implements FlintCacheMXBean {

  private static final AtomicLong SEQ = new AtomicLong();

  private final FlintObjectClient c;
  private final ObjectName name;
  private volatile boolean registered;

  private FlintCacheMetrics(FlintObjectClient c, ObjectName name) {
    this.c = c;
    this.name = name;
  }

  /**
   * Registers a bean for this client, or returns null if it cannot.
   *
   * Never throws. A metrics failure must not fail a read: this library sits in
   * the data path of someone else's job, and an unregisterable MBean -- a name
   * collision, a restrictive SecurityManager, a JVM with JMX disabled -- is a
   * reason to have no metrics, not a reason to fail their query.
   */
  public static FlintCacheMetrics register(FlintObjectClient c, String tierUri) {
    try {
      // Unique per instance: Spark builds one client per executor and several
      // per JVM, and a fixed name would silently report only the first.
      ObjectName n = new ObjectName(String.format(
          "ai.crestway.flintaccel:type=Cache,tier=%s,id=%d",
          ObjectName.quote(tierUri == null ? "unknown" : tierUri),
          SEQ.incrementAndGet()));
      FlintCacheMetrics m = new FlintCacheMetrics(c, n);
      MBeanServer s = ManagementFactory.getPlatformMBeanServer();
      s.registerMBean(m, n);
      m.registered = true;
      return m;
    } catch (Exception ignored) {
      return null;
    }
  }

  public void unregister() {
    if (!registered) return;
    registered = false;
    try {
      ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
    } catch (Exception ignored) {
      // Same rule: teardown must not throw into a caller's close path.
    }
  }

  public ObjectName objectName() { return name; }

  @Override public long getChunkHits()          { return c.chunkHits.get(); }
  @Override public long getChunkMisses()        { return c.chunkMisses.get(); }
  @Override public long getMetadataHits()       { return c.metaHits.get(); }
  @Override public long getOriginGets()         { return c.originGets.get(); }
  @Override public long getOriginBytes()        { return c.originBytes.get(); }
  @Override public long getSingleFlightJoins()  { return c.joined.get(); }
  @Override public long getSseKmsBypassed()     { return c.kmsBypassed.get(); }
  @Override public long getOversizeBypassed()   { return c.oversizeBypassed.get(); }
  @Override public long getSseKmsUndetectable() { return c.kmsUndetectable.get(); }
  @Override public boolean isBreakerOpen()      { return c.isBreakerOpen(); }
  @Override public long getBreakerOpens()       { return c.breakerOpens.get(); }
  @Override public long getBreakerSkips()       { return c.breakerSkips.get(); }
  @Override public long getTierFailures()       { return c.tierFailures.get(); }
  @Override public long getDegradedReads()      { return c.degraded.get(); }
  @Override public long getIntegrityFailures()  { return c.integrityFailures.get(); }

  @Override
  public double getChunkHitRatePercent() {
    long h = c.chunkHits.get(), m = c.chunkMisses.get();
    // Zero reads is not a 0% hit rate; reporting it as one would read as a
    // broken cache rather than an idle one.
    return (h + m) == 0 ? Double.NaN : 100.0 * h / (h + m);
  }

  @Override
  public String getSummary() {
    double r = getChunkHitRatePercent();
    StringBuilder b = new StringBuilder("flint-accel: ");
    if (Double.isNaN(r)) {
      b.append("no chunk reads yet");
    } else {
      b.append(String.format("%.1f%% hit rate, %d hits / %d misses",
          r, getChunkHits(), getChunkMisses()));
    }
    b.append(String.format(", %d origin GETs, %d MiB from S3",
        getOriginGets(), getOriginBytes() / (1024 * 1024)));
    // Only mention the failure modes when they are happening. A summary that
    // always lists everything trains the reader to skip it.
    if (getOversizeBypassed() > 0) {
      b.append(String.format("; %d reads BYPASSED (object above "
          + "flint.max.object.bytes = %d)", getOversizeBypassed(), c.maxObjectBytes));
    }
    if (getSseKmsBypassed() > 0) {
      b.append(String.format("; %d reads BYPASSED (SSE-KMS, off by default -- "
          + "set flint.cache.sse-kms=true to accelerate them)", getSseKmsBypassed()));
    }
    if (isBreakerOpen() || getBreakerOpens() > 0) {
      b.append(String.format("; breaker %s after %d opens (the tier is sick)",
          isBreakerOpen() ? "OPEN" : "closed", getBreakerOpens()));
    }
    if (getIntegrityFailures() > 0) {
      b.append(String.format("; %d cached chunks REJECTED as corrupt or misplaced",
          getIntegrityFailures()));
    }
    return b.toString();
  }
}
