// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

/**
 * What an operator can see, over JMX.
 *
 * ADR-0023 D8 says every claim this product makes is a counter. Sixteen of them
 * existed and NOTHING surfaced them outside the test suites -- so a customer
 * could install the library and have no way to answer "is it working?". Worse,
 * the two silent-zero-acceleration cases were invisible by construction: an
 * SSE-KMS bucket bypasses the cache entirely (D13.3) and a sick tier opens the
 * breaker (D12.36), and in both the reads simply go to S3 as before, correct
 * and unaccelerated, with nothing to look at.
 *
 * <p>JMX and nothing else, deliberately. This library runs inside someone
 * else's Spark cluster, where every dependency it adds is a dependency they
 * did not choose and a conflict they may have to resolve -- we already shade
 * to avoid exactly that. {@code javax.management} is in the JDK, and the JMX
 * exporters that Spark and Prometheus deployments already run will scrape this
 * with no code from us.
 */
public interface FlintCacheMXBean {

  // -- is it working? ------------------------------------------------------
  /** Chunks served from the tier rather than S3. */
  long getChunkHits();

  /** Chunks that had to be fetched from S3. */
  long getChunkMisses();

  /** Percentage of chunk reads served from the tier. The headline number. */
  double getChunkHitRatePercent();

  /** Object metadata served from the tier -- each one is a HEAD not sent. */
  long getMetadataHits();

  /** GET requests actually issued to S3. */
  long getOriginGets();

  /** Bytes actually pulled from S3. */
  long getOriginBytes();

  /** Reads that joined an in-flight fetch instead of duplicating it. */
  long getSingleFlightJoins();

  // -- why is it NOT working? ----------------------------------------------
  /**
   * Reads that bypassed the cache because the object is SSE-KMS encrypted.
   *
   * If acceleration looks absent on a KMS-protected bucket, this is the number
   * that explains it, and {@code flint.cache.sse-kms=true} is the opt-in --
   * after reading what it costs. Without this counter the symptom is "the
   * cache does nothing" with no cause visible anywhere.
   */
  long getSseKmsBypassed();

  /** Reads that skipped the cache because the object exceeds
   *  flint.max.object.bytes. A large steady number means the cap is doing
   *  work; a large number the operator did not expect means it is set wrong,
   *  and those must be distinguishable from a cache that is merely missing. */
  long getOversizeBypassed();

  /** Reads that skipped the cache because the PART exceeded
   *  flint.max.part.bytes (D17.5). Distinct from getOversizeBypassed(): one
   *  says the OBJECT was refused, this says this READ was, and an operator
   *  tuning the cap needs to tell them apart. */
  long getOversizePartBypassed();

  /** Tier calls the server refused because the namespace is FULL (-QUOTA).
   *  Deliberately not getTierFailures(): Flint sheds writes with -QUOTA while
   *  still serving reads, so this is a healthy tier in a configuration someone
   *  chose. Steady and non-zero with getTierFailures() at zero is the exact
   *  signature of a tier that is out of room rather than unwell. TWO causes
   *  share the code and the remedies differ: the tenant is over its configured
   *  cap, or the host is low on disk. The client cannot separate them and does
   *  not try -- its own behaviour is identical -- so read this as "go and look
   *  at the tier". It does not open the breaker. */
  long getTierFull();

  /** Objects whose encryption could not be determined, and were cached anyway.
   *  The exact size of the hole in the SSE-KMS guarantee. */
  long getSseKmsUndetectable();

  /** True while the breaker is open and the tier is being skipped entirely. */
  boolean isBreakerOpen();

  /** How many times the breaker has opened. Non-zero means a sick tier. */
  long getBreakerOpens();

  /** Tier calls skipped because the breaker was open. */
  long getBreakerSkips();

  /** Tier calls that failed or timed out. */
  long getTierFailures();

  /** Reads that fell back to S3 because the tier was unusable. */
  long getDegradedReads();

  /** Metadata lookups that missed and cost a HEAD. Pairs with
   *  getMetadataHits(); without it a metadata hit rate cannot be computed. */
  long getMetadataMisses();

  /** Reads that deliberately skipped the cache entirely (bypass mode, SSE-C).
   *  Not a miss and not a failure -- work the cache was never asked to do. */
  long getBypassed();

  /** Reads that took the single-flight claim and did the fetch. The partner of
   *  getSingleFlightJoins(): claims plus joins is the population D5 divides. */
  long getSingleFlightClaims();

  /** Cached chunks rejected because they were not what they claimed to be
   *  (D14). Non-zero means the tier is corrupting or misplacing data. */
  long getIntegrityFailures();

  /** One line an operator can paste into a ticket. */
  String getSummary();
}
