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
    int i = b.indexOf("\"gets\":");
    return Integer.parseInt(b.substring(i + 7, b.indexOf(',', i)).trim());
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

    System.out.println("\nS3A SUITE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
