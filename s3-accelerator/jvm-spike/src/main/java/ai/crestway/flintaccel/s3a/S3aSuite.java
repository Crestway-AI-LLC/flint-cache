// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.net.URI;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ai.crestway.flintaccel.client.FlintObjectClient;

/**
 * The properties, exercised through HADOOP rather than against the client.
 *
 * The client suite proved them at the ObjectClient seam. S3A sits above that
 * and does its own thing: it wraps our stream in FSDataInputStream, offers
 * several read APIs (single byte, array read, positioned readFully, vectored),
 * and may buffer. D12.13's lesson was that composition finds what the parts
 * do not, so none of those properties can be assumed to survive the wrapper.
 *
 * In particular a positioned readFully() is NOT the same call path as
 * seek()+read(), and a stream that got seek/read right can still return wrong
 * bytes for the other.
 */
public final class S3aSuite {

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

  static int endpointGets(String endpoint) throws Exception {
    var r = java.net.http.HttpClient.newHttpClient().send(
        java.net.http.HttpRequest.newBuilder(URI.create(endpoint + "/__stats")).build(),
        java.net.http.HttpResponse.BodyHandlers.ofString());
    String b = r.body();
    return ai.crestway.flintaccel.OriginStats.parse(b, "gets");
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String tier = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";

    Configuration conf = new Configuration();
    conf.set("fs.s3a.endpoint", endpoint);
    conf.set("fs.s3a.endpoint.region", "us-east-1");
    conf.setBoolean("fs.s3a.path.style.access", true);
    conf.set("fs.s3a.access.key", "s"); conf.set("fs.s3a.secret.key", "s");
    conf.setInt("fs.s3a.bucket.probe", 0);
    conf.set("fs.s3a.change.detection.mode", "none");
    conf.set("fs.s3a.input.stream.type", "custom");
    conf.set("fs.s3a.input.stream.custom.factory", FlintStreamFactory.class.getName());
    conf.set(FlintStreamFactory.TIER_URI, tier);

    String key = "data/000002.bin";
    Path p = new Path("s3a://bucket/" + key);

    try (FileSystem fs = FileSystem.get(URI.create("s3a://bucket/"), conf)) {

      // 1. every read API S3A offers must agree with the oracle
      try (FSDataInputStream in = fs.open(p)) {
        byte[] b = new byte[4096];
        in.seek(150_000);
        in.readFully(b, 0, 4096);
        check(Arrays.equals(b, expect(key, 150_000, 4096)), "seek + readFully");
      }
      try (FSDataInputStream in = fs.open(p)) {
        byte[] b = new byte[4096];
        // POSITIONED read: a different call path from seek+read entirely.
        in.readFully(150_000, b, 0, 4096);
        check(Arrays.equals(b, expect(key, 150_000, 4096)),
            "positioned readFully(pos,...) -- a different path from seek+read");
      }
      try (FSDataInputStream in = fs.open(p)) {
        in.seek(150_000);
        byte[] b = new byte[16];
        for (int i = 0; i < 16; i++) b[i] = (byte) in.read();
        check(Arrays.equals(b, expect(key, 150_000, 16)), "single-byte read()");
      }
      try (FSDataInputStream in = fs.open(p)) {
        in.seek(150_000);
        check(in.getPos() == 150_000, "getPos() reports the seek position");
        byte[] b = new byte[100];
        in.readFully(b);
        check(in.getPos() == 150_100, "getPos() advances by the bytes read");
      }

      // 2. the economic property, measured through Hadoop
      int before = endpointGets(endpoint);
      for (int i = 0; i < 6; i++) {
        try (FSDataInputStream in = fs.open(p)) {
          byte[] b = new byte[4096];
          in.readFully(150_000, b, 0, 4096);
        }
      }
      int after = endpointGets(endpoint);
      check(after == before,
          "6 more opens of a WARM object cost 0 extra origin GETs (" + before
              + " -> " + after + ")");

      int cold0 = endpointGets(endpoint);
      try (FSDataInputStream in = fs.open(new Path("s3a://bucket/data/000005.bin"))) {
        byte[] b = new byte[4096];
        in.readFully(700_000, b, 0, 4096);
      }
      check(endpointGets(endpoint) > cold0,
          "negative control -- a COLD object does reach the origin");

      // 3. concurrency through Hadoop, with a genuine race
      final int N = 12;
      ExecutorService pool = Executors.newFixedThreadPool(N);
      CountDownLatch ready = new CountDownLatch(N), go = new CountDownLatch(1);
      List<Future<Boolean>> fs2 = new ArrayList<>();
      int coldBefore = endpointGets(endpoint);
      for (int i = 0; i < N; i++) {
        final long off = 2_000_000 + (i % 3) * 4096;
        fs2.add(pool.submit(() -> {
          ready.countDown(); go.await();
          try (FSDataInputStream in = fs.open(new Path("s3a://bucket/data/000006.bin"))) {
            byte[] b = new byte[4096];
            in.readFully(off, b, 0, 4096);
            return Arrays.equals(b, expect("data/000006.bin", off, 4096));
          }
        }));
      }
      ready.await(); go.countDown();
      boolean all = true;
      for (var f : fs2) all &= f.get(90, TimeUnit.SECONDS);
      pool.shutdown();
      int used = endpointGets(endpoint) - coldBefore;
      check(all, N + " concurrent Hadoop readers all got correct bytes");
      check(used < N, N + " concurrent cold readers cost " + used + " origin GETs");
    }

    // 4. BUG-0066: a setting this path DECLARES must actually reach the client.
    //
    // fs.s3a.flint.max.object.bytes was declared as a constant here, documented
    // in the README for this path, and never read -- the client was built from
    // the short constructor that takes the defaults. Three more keys had no
    // constant at all while the README listed them. Nothing failed, because an
    // ignored setting behaves exactly like a setting left at its default.
    //
    // Asserted by EFFECT rather than by introspection: a 1-byte part cap must
    // stop this path caching, and the same path with the cap left alone must
    // still cache. A check that read the field back would pass on a client that
    // stored the value and ignored it.
    Configuration capped = new Configuration(conf);
    capped.setLong(FlintStreamFactory.MAX_PART, 1);
    capped.set("fs.s3a.impl.disable.cache", "true");   // else FileSystem.get reuses the one above
    String capKey = "data/000004.bin";
    io.lettuce.core.RedisClient rcli = io.lettuce.core.RedisClient.create(tier);
    try (var rc = rcli.connect(new io.lettuce.core.codec.ByteArrayCodec())) {
      rc.sync().flushall();
      try (FileSystem fs3 = FileSystem.get(URI.create("s3a://bucket/"), capped);
           FSDataInputStream in = fs3.open(new Path("s3a://bucket/" + capKey))) {
        byte[] b = new byte[4096];
        in.readFully(300_000, b, 0, 4096);
        check(Arrays.equals(b, expect(capKey, 300_000, 4096)),
            "a 1-byte part cap on path 1 still READS correctly");
      }
      int cappedKeys = ai.crestway.flintaccel.TierScan.keys(rc, "c2/*").size();
      check(cappedKeys == 0,
          "fs.s3a.flint.max.part.bytes REACHES the client on path 1: nothing "
              + "cached under a 1-byte cap (" + cappedKeys + " chunk keys)");

      // The control is the whole check. Without it this passes on a path that
      // caches nothing for any reason -- which is what the bug looked like.
      rc.sync().flushall();
      Configuration uncapped = new Configuration(conf);
      uncapped.set("fs.s3a.impl.disable.cache", "true");
      try (FileSystem fs4 = FileSystem.get(URI.create("s3a://bucket/"), uncapped);
           FSDataInputStream in = fs4.open(new Path("s3a://bucket/" + capKey))) {
        byte[] b = new byte[4096];
        in.readFully(300_000, b, 0, 4096);
      }
      int normalKeys = ai.crestway.flintaccel.TierScan.keys(rc, "c2/*").size();
      check(normalKeys > 0,
          "POSITIVE CONTROL: the same read WITHOUT the cap does cache ("
              + normalKeys + " chunk keys), so the cap is what decided it");
    } finally {
      rcli.shutdown();
    }

    System.out.println("\nS3A SUITE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
