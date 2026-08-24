// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Step 1 of M2: prove by RUNNING what ADR-0023 D12.1 established by reading —
 * that AAL routes every byte through an injected {@link ObjectClient}.
 *
 * Not a product. The client here counts and delegates; it caches nothing.
 * The question is only whether the seam is real and whether it is complete.
 */
public final class SeamSpike {

  /**
   * Counts what AAL asks for, then delegates.
   *
   * NOTE the signatures. The published 1.1.0 interface is THREE methods, all
   * returning CompletableFuture, with a StreamContext overload. AAL's `main`
   * branch has a different, synchronous, two-method shape taking
   * OpenStreamInformation. ADR-0023 D12.1 documented `main` because that is
   * what was read; this file is compiled against what actually ships.
   */
  static final class CountingObjectClient implements ObjectClient {
    private final ObjectClient delegate;
    final AtomicInteger heads = new AtomicInteger();
    final AtomicInteger gets = new AtomicInteger();

    CountingObjectClient(ObjectClient delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompletableFuture<ObjectMetadata> headObject(HeadRequest r) {
      heads.incrementAndGet();
      System.out.printf("    -> our ObjectClient: HEAD %s%n", r.getS3Uri());
      return delegate.headObject(r);
    }

    @Override
    public CompletableFuture<ObjectContent> getObject(GetRequest r) {
      gets.incrementAndGet();
      System.out.printf("    -> our ObjectClient: GET range=%s%n", r.getRange());
      return delegate.getObject(r);
    }

    @Override
    public CompletableFuture<ObjectContent> getObject(GetRequest r, StreamContext ctx) {
      gets.incrementAndGet();
      System.out.printf("    -> our ObjectClient: GET range=%s (ctx)%n", r.getRange());
      return delegate.getObject(r, ctx);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  /** counting_s3.py generates block i of key k as md5("k:i"). Mirrored to verify. */
  static byte[] expected(String key, long start, int len) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      long abs = start + i;
      long block = abs / 16;
      md.reset();
      byte[] b = md.digest((key + ":" + block).getBytes("UTF-8"));
      out[i] = b[(int) (abs % 16)];
    }
    return out;
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String bucket = "bucket";
    String key = "data/000003.bin";
    boolean ok = true;

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("spike", "spike")))
        .forcePathStyle(true)
        .build();

    CountingObjectClient counting = new CountingObjectClient(new S3SdkObjectClient(s3));

    try (S3SeekableInputStreamFactory factory =
             new S3SeekableInputStreamFactory(counting, S3SeekableInputStreamConfiguration.DEFAULT)) {

      System.out.println("[..] factory constructed with OUR ObjectClient");

      try (S3SeekableInputStream in = factory.createStream(S3URI.of(bucket, key))) {
        byte[] buf = new byte[4096];
        in.seek(1024);
        int n = in.read(buf, 0, 256);

        byte[] want = expected(key, 1024, n);
        boolean bytesOk = n == 256 && java.util.Arrays.equals(
            java.util.Arrays.copyOf(buf, n), want);
        ok &= bytesOk;
        System.out.printf("[%s] read %d bytes at offset 1024 and they match the oracle%n",
            bytesOk ? "ok" : "FAIL", n);
      }
    }

    int calls = counting.gets.get() + counting.heads.get();
    boolean called = calls > 0;
    ok &= called;
    System.out.printf("[%s] AAL routed through our client (%d HEAD, %d GET)%n",
        called ? "ok" : "FAIL", counting.heads.get(), counting.gets.get());

    System.out.println("\nSEAM SPIKE " + (ok ? "PASSED" : "FAILED"));
    System.out.println("Now compare against the endpoint's /__stats: any request there "
        + "that our client did not issue is a BYPASS.");
    System.exit(ok ? 0 : 1);
  }
}
