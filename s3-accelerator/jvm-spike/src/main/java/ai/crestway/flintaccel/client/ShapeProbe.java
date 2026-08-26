// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * One JVM read in a named ACCESS PATTERN, so the two language paths can be
 * compared on the same question.
 *
 * It exists because the Python measurement answered only half of it. s3fs
 * hands our client ~4 MiB per read and AAL hands it ~128 KiB, so read
 * amplification -- how much an engine drags from the origin for a small
 * request -- is a property of the ENGINE, not of this cache, and the two
 * cannot be assumed to behave alike. Quoting the Python figure for both would
 * be quoting the wrong instrument.
 *
 * Prints nothing an assertion depends on: counting_s3 does the counting, and
 * this only performs the reads.
 */
public final class ShapeProbe {
  public static void main(String[] args) throws Exception {
    String endpoint = args[0], bucket = args[1], key = args[2], tierUri = args[3];
    String mode = args[4];                       // full | sparse
    long objBytes = Long.parseLong(args[5]);

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("x", "x")))
        .forcePathStyle(true).build();
    RedisClient rc = RedisClient.create(tierUri);
    StatefulRedisConnection<byte[], byte[]> cn = rc.connect(new ByteArrayCodec());

    long asked = 0;
    try (var sdk = new S3SdkObjectClient(s3, false)) {
      var c = new FlintObjectClient(sdk, cn.async(), FlintObjectClient.DEFAULT_CHUNK_BYTES, 50, 300, false, s3, false);
      try (var f = new S3SeekableInputStreamFactory(c, S3SeekableInputStreamConfiguration.DEFAULT);
           var in = f.createStream(S3URI.of(bucket, key))) {
        if ("full".equals(mode)) {
          byte[] buf = new byte[8 * 1024 * 1024];
          in.seek(0);
          long off = 0;
          while (off < objBytes) {
            int want = (int) Math.min(buf.length, objBytes - off);
            int got = in.read(buf, 0, want);
            if (got <= 0) break;
            off += got; asked += got;
          }
        } else {
          // 32 reads of 64 KiB, spread so they touch 2 MiB of the object --
          // genuinely sparse. The Python version of this test first used a
          // step equal to the read size, which covers the object contiguously
          // and measures a sequential read wearing a sparse label.
          int n = 32, sz = 64 * 1024;
          long step = objBytes / n;
          byte[] buf = new byte[sz];
          for (int i = 0; i < n; i++) {
            in.seek(i * step);
            int got = in.read(buf, 0, sz);
            if (got > 0) asked += got;
          }
        }
      }
    }
    System.out.println("asked=" + asked);
    cn.close(); rc.shutdown(); s3.close();
    System.exit(0);
  }
}
