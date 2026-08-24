// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.net.URI;
import java.security.MessageDigest;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import ai.crestway.flintaccel.iceberg.FlintFileIO;

/** The two paths ADR-0023 D12.21/D12.22 specify, both exercised. */
public final class AdoptionSuite {

  static boolean ok = true;
  static void check(boolean c, String label) {
    ok &= c; System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static byte[] expect(String key, long off, int len) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      long abs = off + i; md.reset();
      out[i] = md.digest((key + ":0:" + (abs / 16)).getBytes("UTF-8"))[(int) (abs % 16)];
    }
    return out;
  }

  static int gets(String ep) throws Exception {
    var r = java.net.http.HttpClient.newHttpClient().send(
        java.net.http.HttpRequest.newBuilder(URI.create(ep + "/__stats")).build(),
        java.net.http.HttpResponse.BodyHandlers.ofString());
    String b = r.body(); int i = b.indexOf("\"gets\":");
    return Integer.parseInt(b.substring(i + 7, b.indexOf(',', i)).trim());
  }

  public static void main(String[] a) throws Exception {
    String ep = a.length > 0 ? a[0] : "http://127.0.0.1:9000";
    String tier = a.length > 1 ? a[1] : "redis://127.0.0.1:6399";
    String key = "data/000003.bin";

    // ---------- Path B: fs.s3a.impl (any Hadoop version) ----------
    Configuration conf = new Configuration();
    conf.set("fs.s3a.endpoint", ep);
    conf.set("fs.s3a.endpoint.region", "us-east-1");
    conf.setBoolean("fs.s3a.path.style.access", true);
    conf.set("fs.s3a.access.key", "a"); conf.set("fs.s3a.secret.key", "a");
    conf.setInt("fs.s3a.bucket.probe", 0);
    conf.set("fs.s3a.change.detection.mode", "none");
    conf.set("fs.s3a.impl", FlintS3AFileSystem.class.getName());
    conf.set("fs.s3a.flint.tier.uri", tier);

    try (FileSystem fs = FileSystem.get(URI.create("s3a://bucket/"), conf)) {
      check(fs instanceof FlintS3AFileSystem,
          "fs.s3a.impl gave us OURS (" + fs.getClass().getSimpleName() + ")");

      // DOOR 1
      try (FSDataInputStream in = fs.open(new Path("s3a://bucket/" + key))) {
        byte[] b = new byte[4096]; in.readFully(300_000, b, 0, 4096);
        check(Arrays.equals(b, expect(key, 300_000, 4096)), "door 1: open() reads correctly");
      }
      // DOOR 2 -- openFile(), which routes to openFileWithOptions
      int before = gets(ep);
      try (FSDataInputStream in = fs.openFile(new Path("s3a://bucket/" + key))
              .build().get()) {
        check(in.getWrappedStream() instanceof FlintS3AFileSystem.AalStream,
            "door 2: openFile() ALSO lands in our stream ("
                + in.getWrappedStream().getClass().getSimpleName() + ")");
        byte[] b = new byte[4096]; in.readFully(300_000, b, 0, 4096);
        check(Arrays.equals(b, expect(key, 300_000, 4096)), "door 2: openFile() reads correctly");
      }
      check(gets(ep) == before,
          "door 2 was served from the tier, not the origin -- so it is genuinely cached, "
          + "not merely correct");
    }

    // ---------- Path A: Iceberg io-impl ----------
    Map<String, String> props = new HashMap<>();
    props.put("s3.endpoint", ep);
    props.put("s3.region", "us-east-1");
    props.put("s3.path-style-access", "true");
    // Iceberg builds its own S3 client and reads client.region, NOT
    // s3.region. Without it the SDK falls through to the ambient region
    // chain -- a developer laptop with ~/.aws/config passes, a clean CI
    // runner throws "Unable to load region from any of the providers".
    props.put("client.region", "us-east-1");
    props.put("s3.access-key-id", "a");
    props.put("s3.secret-access-key", "a");
    props.put("flint.tier.uri", tier);

    // Instantiate exactly as Iceberg's io-impl does: no-arg ctor + initialize.
    Class<?> c = Class.forName(FlintFileIO.class.getName());
    FlintFileIO io = (FlintFileIO) c.getDeclaredConstructor().newInstance();
    io.initialize(props);
    check(true, "io-impl reflection contract: no-arg ctor + initialize(Map)");

    String loc = "s3://bucket/" + key;
    for (boolean withLength : new boolean[]{false, true}) {
      InputFile f = withLength ? io.newInputFile(loc, 8L * 1024 * 1024) : io.newInputFile(loc);
      try (SeekableInputStream in = f.newStream()) {
        in.seek(300_000);
        byte[] b = new byte[4096];
        int n = 0; while (n < 4096) { int r = in.read(b, n, 4096 - n); if (r < 0) break; n += r; }
        check(n == 4096 && Arrays.equals(b, expect(key, 300_000, 4096)),
            "iceberg newInputFile(" + (withLength ? "path,length" : "path") + ") reads correctly");
      }
    }
    // BOTH overloads must be ours, or Iceberg reads past the cache silently.
    check(io.newInputFile(loc).getClass().getName().contains("FlintInputFile")
            && io.newInputFile(loc, 123).getClass().getName().contains("FlintInputFile"),
        "BOTH newInputFile overloads return OUR InputFile -- no silent bypass");
    io.close();

    System.out.println("\nADOPTION SUITE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
