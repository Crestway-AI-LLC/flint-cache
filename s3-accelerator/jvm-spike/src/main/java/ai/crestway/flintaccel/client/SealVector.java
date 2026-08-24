// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

/**
 * Prints the D14 seal for one fixed vector, so the cross-language drill can
 * assert that both clients compute it identically.
 *
 * It calls the PRODUCTION seal rather than reimplementing it. A vector
 * generator with its own copy of the algorithm would agree with Python while
 * the real client disagreed, which is the failure the drill exists to prevent.
 *
 * The ETag is quoted deliberately: ETags arrive quoted from both S3 clients and
 * both sides must strip them the same way, or the keys silently split.
 */
public final class SealVector {
  public static void main(String[] args) {
    byte[] sealed = FlintObjectClient.sealForBench("\"abc123\"", 7,
        "flint-interop-vector".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    long v = 0;
    for (int i = 3; i >= 0; i--) v = (v << 8) | (sealed[i] & 0xFFL);
    System.out.println(v);
  }
}
