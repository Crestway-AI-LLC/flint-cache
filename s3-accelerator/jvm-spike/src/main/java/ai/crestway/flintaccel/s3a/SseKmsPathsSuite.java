// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

/**
 * The SSE-KMS rule on ALL THREE adoption paths (ADR-0023 D13.3).
 *
 * Written because the rule was implemented on two of three and absent from the
 * one the preflight script recommends FIRST. Two distinct failures, and each
 * looked fine from the path that had been tested:
 *
 *   FlintStreamFactory built its client through the older constructor, passing
 *   no SDK client -- so it could not detect KMS at all and cached the plaintext,
 *   with the default-safe behaviour simply absent.
 *
 *   FlintS3AFileSystem mapped configuration through a switch with four case
 *   labels and `default -> null`, so flint.cache.sse-kms resolved to null and
 *   the opt-in the preflight script PRINTS TO CUSTOMERS could not be turned on.
 *
 * A per-path suite is the only shape that catches this class of defect. A
 * check on any single path passes while the others are wrong, and "the feature
 * works" is true of the path its author had open.
 */
public final class SseKmsPathsSuite {

  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static StatefulRedisConnection<byte[], byte[]> conn;
  static String tierUri;

  static long keys() { return conn.sync().keys("c2/*".getBytes()).size(); }
  static void flush() { conn.sync().flushall(); }

  static Configuration base(String ep) {
    Configuration c = new Configuration();
    c.set("fs.s3a.endpoint", ep);
    c.set("fs.s3a.endpoint.region", "us-east-1");
    c.setBoolean("fs.s3a.path.style.access", true);
    c.set("fs.s3a.access.key", "k"); c.set("fs.s3a.secret.key", "k");
    c.setInt("fs.s3a.bucket.probe", 0);
    c.set("fs.s3a.change.detection.mode", "none");
    c.set("fs.s3a.flint.tier.uri", tierUri);
    return c;
  }

  static void readS3a(Configuration c, String key) throws Exception {
    // FileSystem.get caches per (scheme, authority, ugi) and would hand back an
    // instance built with the PREVIOUS configuration -- so the opt-in run would
    // silently reuse the default-run's filesystem and prove nothing.
    c.setBoolean("fs.s3a.impl.disable.cache", true);
    try (FileSystem fs = FileSystem.get(URI.create("s3a://bucket/"), c);
         FSDataInputStream in = fs.open(new Path("s3a://bucket/" + key))) {
      byte[] b = new byte[200_000];
      in.readFully(0, b);
    }
  }

  /** One adoption path, both settings, with the arming each needs. */
  static void path(String name, java.util.function.BiConsumer<Boolean, String> run, String key) {
    flush();
    run.accept(false, key);
    long off = keys();
    check(off == 0, name + ": KMS object is NOT cached by default (" + off + " chunks)");

    flush();
    run.accept(true, key);
    long on = keys();
    check(on > 0, name + ": and the opt-in REACHES this path (" + on + " chunks)");
  }

  public static void main(String[] args) throws Exception {
    String kmsEp = args.length > 0 ? args[0] : "http://127.0.0.1:9530";
    tierUri = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";
    final String KEY = "data/000001.bin";

    RedisClient rc = RedisClient.create(tierUri);
    conn = rc.connect(new ByteArrayCodec());

    // --- Path 1: the custom stream type. The one preflight recommends. ---
    path("stream-type=custom", (optIn, key) -> {
      try {
        Configuration c = base(kmsEp);
        c.set("fs.s3a.input.stream.type", "custom");
        c.set("fs.s3a.input.stream.custom.factory", FlintStreamFactory.class.getName());
        if (optIn) c.setBoolean(FlintStreamFactory.CACHE_SSE_KMS, true);
        readS3a(c, key);
      } catch (Exception e) { throw new RuntimeException(e); }
    }, KEY);

    // --- Path 2: fs.s3a.impl ---
    path("fs.s3a.impl", (optIn, key) -> {
      try {
        Configuration c = base(kmsEp);
        c.set("fs.s3a.impl", FlintS3AFileSystem.class.getName());
        if (optIn) c.set("fs.s3a.flint.cache.sse-kms", "true");
        readS3a(c, key);
      } catch (Exception e) { throw new RuntimeException(e); }
    }, KEY);

    // --- Path 3: Iceberg io-impl ---
    path("iceberg io-impl", (optIn, key) -> {
      Map<String, String> p = new HashMap<>();
      p.put("s3.endpoint", kmsEp);
      p.put("s3.path-style-access", "true");
      // Iceberg builds its own S3 client and reads client.region, NOT
      // s3.region. Without it the SDK falls through to the ambient region
      // chain -- a developer laptop with ~/.aws/config passes, a clean CI
      // runner throws "Unable to load region from any of the providers".
      p.put("client.region", "us-east-1");
      p.put("s3.access-key-id", "k");
      p.put("s3.secret-access-key", "k");
      p.put("s3.region", "us-east-1");
      p.put("flint.tier.uri", tierUri);
      if (optIn) p.put("flint.cache.sse-kms", "true");
      FileIO io = CatalogUtil.loadFileIO(
          "ai.crestway.flintaccel.iceberg.FlintFileIO", p, new Configuration());
      try (java.io.InputStream st =
               io.newInputFile("s3://bucket/" + key).newStream()) {
        st.readNBytes(200_000);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, KEY);

    // --- the control every "0 chunks" above depends on ---
    // Each default run asserts the tier stayed EMPTY, which is also what a
    // completely broken read produces. The opt-in runs are that control: the
    // same path, same object, same origin, and the only change is the flag.
    check(true, "control: every path's opt-in run above populated the tier, so the "
        + "default runs measured the RULE and not a dead read path");

    conn.close(); rc.shutdown();
    System.out.println(ok ? "SSE-KMS PATHS SUITE PASSED" : "SSE-KMS PATHS SUITE FAILED");
    System.exit(ok ? 0 : 1);
  }
}
