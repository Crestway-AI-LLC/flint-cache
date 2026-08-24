// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.client;

import java.net.URI;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** One JVM read through the real client, for the cross-language drill to time. */
public final class CrossLangProbe {
  public static void main(String[] args) throws Exception {
    String endpoint = args[0];
    String bucket = args.length > 1 ? args[1] : "bucket";
    String key = args.length > 2 ? args[2] : "data/000001.bin";
    int len = args.length > 3 ? Integer.parseInt(args[3]) : 100_000;

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(
            software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                AwsBasicCredentials.create("x", "x")))
        .forcePathStyle(true).build();
    RedisClient rc = RedisClient.create("redis://127.0.0.1:9399");
    StatefulRedisConnection<byte[], byte[]> cn = rc.connect(new ByteArrayCodec());

    try (var sdk = new S3SdkObjectClient(s3, false)) {
      var c = new FlintObjectClient(sdk, cn.async(), 64 * 1024, 50, 2, false, s3, false);
      try (var f = new S3SeekableInputStreamFactory(c, S3SeekableInputStreamConfiguration.DEFAULT);
           var in = f.createStream(S3URI.of(bucket, key))) {
        byte[] b = new byte[len];
        in.seek(0);
        in.read(b, 0, len);
        java.nio.file.Files.write(java.nio.file.Path.of("/tmp/xlang_java_bytes"), b);
      }
    }
    cn.close(); rc.shutdown(); s3.close();
    System.exit(0);
  }
}
