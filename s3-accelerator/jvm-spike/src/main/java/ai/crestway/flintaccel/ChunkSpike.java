// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import software.amazon.s3.analyticsaccelerator.*;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * What does AAL actually ASK for? D4 picked 8 MiB chunks by reasoning about
 * application reads, and D12.4 showed that is the wrong layer -- a 256-byte
 * read became a 64512-byte GET. We cache what AAL requests, so the chunk size
 * must be chosen against AAL's distribution, not the application's.
 *
 * The question chunking exists to answer is SHARING: if two readers with
 * different access patterns produce different AAL ranges, then caching whole
 * ranges (as the tier spike does) shares nothing between them, and only
 * absolute-offset chunks would. This measures whether that is true.
 */
public final class ChunkSpike {

  record Req(long start, long end) {
    long len() { return end - start + 1; }
  }

  static final class RecordingObjectClient implements ObjectClient {
    private final ObjectClient origin;
    final Queue<Req> seen = new ConcurrentLinkedQueue<>();

    RecordingObjectClient(ObjectClient origin) { this.origin = origin; }

    @Override public CompletableFuture<ObjectMetadata> headObject(HeadRequest r) {
      return origin.headObject(r);
    }
    @Override public CompletableFuture<ObjectContent> getObject(GetRequest r) {
      seen.add(new Req(r.getRange().getStart(), r.getRange().getEnd()));
      return origin.getObject(r);
    }
    @Override public CompletableFuture<ObjectContent> getObject(GetRequest r, StreamContext c) {
      seen.add(new Req(r.getRange().getStart(), r.getRange().getEnd()));
      return origin.getObject(r, c);
    }
    @Override public void close() throws IOException { origin.close(); }
  }

  /** Absolute-offset chunk ids a set of ranges would touch, at a given size. */
  static Set<Long> chunksTouched(Collection<Req> reqs, long chunkBytes) {
    Set<Long> out = new TreeSet<>();
    for (Req r : reqs) {
      for (long c = r.start() / chunkBytes; c <= r.end() / chunkBytes; c++) out.add(c);
    }
    return out;
  }

  static long bytesOf(Collection<Req> reqs) {
    long t = 0; for (Req r : reqs) t += r.len(); return t;
  }

  public static void main(String[] args) throws Exception {
    String endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String bucket = "bucket", key = "data/000004.bin";
    long objectBytes = args.length > 1 ? Long.parseLong(args[1]) : 8L * 1024 * 1024;

    S3AsyncClient s3 = S3AsyncClient.builder()
        .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("spike", "spike")))
        .forcePathStyle(true).build();
    var cfg = S3SeekableInputStreamConfiguration.DEFAULT;

    Map<String, List<Req>> byPattern = new LinkedHashMap<>();
    Map<String, Long> appBytes = new LinkedHashMap<>();

    record Pattern(String name, long[] offsets, int len) {}
    var patterns = List.of(
        new Pattern("sequential 8x64KB", new long[]{0, 65536, 131072, 196608,
            262144, 327680, 393216, 458752}, 65536),
        new Pattern("random 8x4KB", new long[]{3_100_000, 511_000, 7_900_000,
            1_250_000, 6_020_000, 202_000, 4_400_000, 88_000}, 4096),
        new Pattern("tail 8KB (footer)", new long[]{objectBytes - 8192}, 8192),
        new Pattern("head 1KB", new long[]{0}, 1024));

    for (Pattern p : patterns) {
      try (var sdk = new S3SdkObjectClient(s3, false)) {  // false: do NOT close the shared client
        var rec = new RecordingObjectClient(sdk);
        long asked = 0;
        for (long off : p.offsets()) {
          // fresh factory per read: AAL's own cache must not mask the pattern (D12.5)
          try (var f = new S3SeekableInputStreamFactory(rec, cfg);
               S3SeekableInputStream in = f.createStream(S3URI.of(bucket, key))) {
            byte[] b = new byte[p.len()];
            in.seek(off);
            int n = in.read(b, 0, p.len());
            asked += Math.max(n, 0);
          }
        }
        byPattern.put(p.name(), new ArrayList<>(rec.seen));
        appBytes.put(p.name(), asked);
      }
    }

    System.out.printf("%nobject = %d bytes%n%n", objectBytes);
    System.out.printf("%-20s %8s %12s %12s %8s%n",
        "pattern", "AAL reqs", "app bytes", "AAL bytes", "amp");
    for (var e : byPattern.entrySet()) {
      long ab = appBytes.get(e.getKey()), lb = bytesOf(e.getValue());
      System.out.printf("%-20s %8d %12d %12d %7.1fx%n",
          e.getKey(), e.getValue().size(), ab, lb, ab == 0 ? 0.0 : (double) lb / ab);
    }

    System.out.println("\n-- what AAL actually requested (start-end) --");
    for (var e : byPattern.entrySet()) {
      StringBuilder sb = new StringBuilder();
      for (Req r : e.getValue()) sb.append(r.start()).append('-').append(r.end()).append(' ');
      System.out.printf("  %-20s %s%n", e.getKey(),
          sb.length() > 150 ? sb.substring(0, 150) + "..." : sb.toString());
    }

    System.out.println("\n-- SHARING: do two patterns hit the same chunks? --");
    var seq = byPattern.get("sequential 8x64KB");
    var rnd = byPattern.get("random 8x4KB");
    System.out.printf("  %-12s %10s %10s %10s   %s%n",
        "chunk size", "seq", "random", "shared", "verdict");
    for (long cs : new long[]{8L << 20, 1L << 20, 256 << 10, 64 << 10, 8 << 10}) {
      Set<Long> a = chunksTouched(seq, cs), b = chunksTouched(rnd, cs);
      Set<Long> both = new TreeSet<>(a); both.retainAll(b);
      String label = cs >= (1 << 20) ? (cs >> 20) + " MiB" : (cs >> 10) + " KiB";
      System.out.printf("  %-12s %10d %10d %10d   %s%n", label, a.size(), b.size(),
          both.size(), both.isEmpty() ? "no sharing" : "shares " + both.size());
    }

    boolean exactMatch = new HashSet<>(seq).stream().anyMatch(new HashSet<>(rnd)::contains);
    System.out.printf("%n  whole-range caching (what the tier spike does) would share: %s%n",
        exactMatch ? "some ranges" : "NOTHING between these two patterns");
    System.out.println("\nCHUNK SPIKE (measurement only, no pass/fail)");
  }
}
