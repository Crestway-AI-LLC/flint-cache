// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.net.URI;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * The whole path, end to end: s3a:// -> S3A -> our factory -> AAL -> our
 * ObjectClient -> Flint/Valkey -> origin.
 *
 * Everything below this has been tested in isolation. This is the first time
 * Hadoop's own FileSystem drives it.
 */
public final class EndToEnd {

  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static byte[] expect(String key, long off, int len) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      long abs = off + i;
      md.reset();
      out[i] = md.digest((key + ":0:" + (abs / 16)).getBytes("UTF-8"))[(int) (abs % 16)];
    }
    return out;
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String tier = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";

    Configuration conf = new Configuration();
    conf.set("fs.s3a.endpoint", endpoint);
    conf.set("fs.s3a.endpoint.region", "us-east-1");
    conf.setBoolean("fs.s3a.path.style.access", true);
    conf.set("fs.s3a.access.key", "e2e");
    conf.set("fs.s3a.secret.key", "e2e");
    conf.setInt("fs.s3a.bucket.probe", 0);
    conf.set("fs.s3a.change.detection.mode", "none");
    // WORKAROUND UNDER TEST: does disabling the audit path avoid the
    // NoClassDefFoundError in AWSRequestAnalyzer on 3.4.3/3.5.0?
    if (System.getenv("FLINT_NOAUDIT") != null) {
      conf.setBoolean("fs.s3a.audit.enabled", false);
      System.out.println("     (audit disabled via fs.s3a.audit.enabled=false)");
    }
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    // The registration under test.
    conf.set("fs.s3a.input.stream.type", "custom");
    conf.set("fs.s3a.input.stream.custom.factory", FlintStreamFactory.class.getName());
    conf.set(FlintStreamFactory.TIER_URI, tier);

    String key = "data/000001.bin";
    long off = 100_000; int len = 8192;

    try (FileSystem fs = FileSystem.get(URI.create("s3a://bucket/"), conf)) {
      check(fs != null, "S3AFileSystem created against the counting endpoint");

      byte[] first;
      try (FSDataInputStream in = fs.open(new Path("s3a://bucket/" + key))) {
        check(in.getWrappedStream() instanceof FlintObjectStream
                || in.getWrappedStream().getClass().getName().contains("Flint"),
            "the open stream is OURS (" + in.getWrappedStream().getClass().getSimpleName() + ")");
        first = new byte[len];
        in.seek(off);
        in.readFully(first, 0, len);
      }
      check(Arrays.equals(first, expect(key, off, len)),
          "bytes read through S3A verify against the oracle");

      // second read: should be served from the tier
      byte[] second;
      try (FSDataInputStream in = fs.open(new Path("s3a://bucket/" + key))) {
        second = new byte[len];
        in.seek(off);
        in.readFully(second, 0, len);
      }
      check(Arrays.equals(first, second), "a second read through S3A returns the same bytes");
    }

    System.out.println("\nEND-TO-END " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
