// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;

import ai.crestway.flintaccel.iceberg.FlintFileIO;

/**
 * ADR-0023 D13: an SSE-C read must cache NOTHING, on EVERY path.
 *
 * The stream-type path checked this from the start. The fs.s3a.impl and
 * Iceberg paths did not, so two of three entry points would have written a
 * customer's decrypted plaintext into a shared tier that any other reader of
 * that namespace can read without the key. A control guarding one door guards
 * nothing.
 */
public final class SseCSuite {

  static boolean ok = true;
  static void check(boolean c, String s) { ok &= c; System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", s); }

  static long tierKeys(String url) {
    RedisClient rc = RedisClient.create(url);
    try (var c = rc.connect(new ByteArrayCodec())) { return c.sync().dbsize(); }
    finally { rc.shutdown(); }
  }
  static void flush(String url) {
    RedisClient rc = RedisClient.create(url);
    try (var c = rc.connect(new ByteArrayCodec())) { c.sync().flushall(); }
    finally { rc.shutdown(); }
  }

  static Configuration base(String ep, String tier) {
    Configuration c = new Configuration();
    c.set("fs.s3a.endpoint", ep); c.set("fs.s3a.endpoint.region", "us-east-1");
    c.setBoolean("fs.s3a.path.style.access", true);
    c.set("fs.s3a.access.key", "k"); c.set("fs.s3a.secret.key", "k");
    c.setInt("fs.s3a.bucket.probe", 0);
    c.set("fs.s3a.change.detection.mode", "none");
    c.set("fs.s3a.impl", FlintS3AFileSystem.class.getName());
    c.set("fs.s3a.flint.tier.uri", tier);
    return c;
  }

  static void read(Configuration c, String key) throws Exception {
    try (FileSystem fs = FileSystem.newInstance(URI.create("s3a://bucket/"), c);
         FSDataInputStream in = fs.open(new Path("s3a://bucket/" + key))) {
      byte[] b = new byte[4096]; in.readFully(500_000, b, 0, 4096);
    }
  }

  public static void main(String[] a) throws Exception {
    String ep = a.length > 0 ? a[0] : "http://127.0.0.1:9000";
    String tier = a.length > 1 ? a[1] : "redis://127.0.0.1:6399";
    String key = "data/000007.bin";
    // a 32-byte AES key, base64, as S3A expects
    String k64 = Base64.getEncoder().encodeToString(new byte[32]);

    // ---- fs.s3a.impl, SSE-C configured ----
    flush(tier);
    Configuration enc = base(ep, tier);
    enc.set("fs.s3a.encryption.algorithm", "SSE-C");
    enc.set("fs.s3a.encryption.key", k64);
    read(enc, key);
    check(tierKeys(tier) == 0,
        "fs.s3a.impl + SSE-C wrote NOTHING to the tier (" + tierKeys(tier) + " keys)");
    // The read must also SUCCEED, which needs the key to have been sent. The
    // origin is running with --require-ssec, so a client that dropped the key
    // fails here instead of looking healthy.
    check(true, "fs.s3a.impl + SSE-C read succeeded against a key-requiring origin");

    // ---- control: same path WITHOUT SSE-C must populate ----
    flush(tier);
    read(base(ep, tier), key);
    long plain = tierKeys(tier);
    check(plain > 0,
        "negative control -- the same read WITHOUT SSE-C does populate (" + plain + " keys)");

    // ---- Iceberg io-impl, SSE-C configured ----
    flush(tier);
    Map<String, String> p = new HashMap<>();
    p.put("s3.endpoint", ep); p.put("s3.region", "us-east-1");
    p.put("s3.path-style-access", "true");
    p.put("s3.access-key-id", "k"); p.put("s3.secret-access-key", "k");
    p.put("flint.tier.uri", tier);
    p.put("s3.sse.type", "custom"); p.put("s3.sse.key", k64);
    p.put("s3.sse.md5", Base64.getEncoder().encodeToString(
        java.security.MessageDigest.getInstance("MD5").digest(new byte[32])));
    FlintFileIO io = new FlintFileIO();
    io.initialize(p);
    InputFile f = io.newInputFile("s3://bucket/" + key);
    try (SeekableInputStream in = f.newStream()) {
      in.seek(500_000); byte[] b = new byte[4096];
      int n = 0; while (n < 4096) { int r = in.read(b, n, 4096 - n); if (r < 0) break; n += r; }
    }
    check(tierKeys(tier) == 0,
        "Iceberg io-impl + SSE-C wrote NOTHING to the tier (" + tierKeys(tier) + " keys)");
    io.close();

    // ---- and Iceberg WITHOUT SSE-C must populate ----
    flush(tier);
    p.remove("s3.sse.type"); p.remove("s3.sse.key"); p.remove("s3.sse.md5");
    FlintFileIO io2 = new FlintFileIO(); io2.initialize(p);
    try (SeekableInputStream in = io2.newInputFile("s3://bucket/" + key).newStream()) {
      in.seek(500_000); byte[] b = new byte[4096];
      int n = 0; while (n < 4096) { int r = in.read(b, n, 4096 - n); if (r < 0) break; n += r; }
    }
    check(tierKeys(tier) > 0,
        "negative control -- Iceberg without SSE-C does populate (" + tierKeys(tier) + " keys)");
    io2.close();

    System.out.println("\nSSE-C SUITE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
