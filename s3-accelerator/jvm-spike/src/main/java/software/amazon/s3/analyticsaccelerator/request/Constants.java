// SPDX-License-Identifier: Apache-2.0
package software.amazon.s3.analyticsaccelerator.request;

import software.amazon.awssdk.core.interceptor.ExecutionAttribute;

/**
 * COMPATIBILITY SHIM. This class is not ours, and it is deliberately in
 * someone else's package.
 *
 * hadoop-aws 3.4.3 and 3.5.0 compile a hard reference to
 * `software.amazon.s3.analyticsaccelerator.request.Constants.SPAN_ID` and
 * `.OPERATION_NAME` into `AWSRequestAnalyzer.isRequestAuditedOutsideOfCurrentSpan`.
 * No published AAL release contains the class — checked 0.0.1 through 1.1.0 —
 * so those Hadoop versions throw NoClassDefFoundError on EVERY S3 request, in
 * the audit interceptor, whether or not the analytics stream is in use. They
 * were evidently built against an unreleased AAL.
 *
 * Supplying the two fields Hadoop reads restores the released jars to working
 * order. The semantics are right rather than merely quiet: the method asks
 * "did AAL's own client already audit this request outside Hadoop's span?",
 * answering true only when BOTH attributes are present. Nothing populates
 * them here — our reads go through our own S3AsyncClient — so it answers
 * false, which is the truth, and Hadoop audits normally.
 *
 * The alternative is `fs.s3a.audit.enabled=false`, which also works and costs
 * the customer their S3 access-log correlation. This shim keeps auditing.
 *
 * MUST be shipped as a separate optional artifact, never inside the main jar:
 * the day a real AAL publishes this class, two copies on one classpath is a
 * problem we would have created. See ADR-0023 D12.18.
 */
public final class Constants {

  public static final ExecutionAttribute<String> SPAN_ID =
      new ExecutionAttribute<>("AAL_SPAN_ID");

  public static final ExecutionAttribute<String> OPERATION_NAME =
      new ExecutionAttribute<>("AAL_OPERATION_NAME");

  private Constants() { }
}
