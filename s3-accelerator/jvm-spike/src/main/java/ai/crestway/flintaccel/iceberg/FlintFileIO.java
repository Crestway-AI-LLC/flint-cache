// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.iceberg;

import java.io.IOException;
import java.util.Map;

import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

import ai.crestway.flintaccel.client.TierSupport;

/**
 * The Iceberg path: `spark.sql.catalog.<cat>.io-impl=ai.crestway.flintaccel.iceberg.FlintFileIO`.
 *
 * One config line, and independent of the Hadoop version — which makes it the
 * *easiest* adoption route for a user even though it is more work for us
 * (ADR-0023 D12.21). It accelerates Iceberg table reads only, and that
 * limitation is also its safety: nothing outside table reads can be affected.
 *
 * Subclassing rather than delegating. `S3FileIO` is public and non-final with
 * a no-arg constructor and `initialize(Map)` — exactly Iceberg's `io-impl`
 * reflection contract — so credentials, `newOutputFile`, `deleteFile`,
 * `listPrefix` and Serializable all come for free. We override reads.
 *
 * BOTH overloads (D12.21). `newInputFile(String)` and
 * `newInputFile(String, long)` are separate methods and the default
 * `newInputFile(DataFile)` routes to the length-hint variant, so overriding
 * one leaves Iceberg reading straight past the cache with correct results and
 * no error.
 *
 * `FileIO extends Serializable` and Spark ships this to executors, so the tier
 * is `transient` and rebuilt lazily on the far side from the properties, which
 * `S3FileIO` already serializes for us.
 */
public class FlintFileIO extends S3FileIO {

  private Map<String, String> props;
  /** SSE-C means the cache is bypassed entirely -- ADR-0023 D13. Iceberg
   *  spells it s3.sse.type=custom. Decided at initialize(), per catalog. */
  private boolean sseC;
  private transient volatile TierSupport tier;

  @Override
  public void initialize(Map<String, String> properties) {
    super.initialize(properties);
    this.props = properties;
    this.sseC = "custom".equalsIgnoreCase(
        properties.getOrDefault("s3.sse.type", "none"));
  }

  /** Lazy because a deserialized copy on an executor has no tier yet. */
  private TierSupport tier() {
    TierSupport t = tier;
    if (t == null) {
      synchronized (this) {
        t = tier;
        if (t == null) {
      // ICEBERG FILES ARE IMMUTABLE BY THE FORMAT, so declare it unless the
      // catalog says otherwise. Iceberg never rewrites a file -- data files,
      // manifests, manifest lists and metadata JSON are all write-once, and a
      // change is a NEW file plus a commit. So revalidating their metadata on
      // the default 60s TTL is a HEAD per object per minute guarding against
      // something the format forbids.
      //
      // Default here rather than in TierSupport because it is only sound where
      // the format guarantees it: an arbitrary s3a:// path carries no such
      // promise, so that path keeps the short TTL unless a user opts in.
      java.util.Map<String, String> p2 = new java.util.HashMap<>(props);
      p2.putIfAbsent("flint.immutable", "true");
      tier = t = TierSupport.build(TierSupport.from(p2));
    }
      }
    }
    return t;
  }

  @Override
  public InputFile newInputFile(String path) {
    // SSE-C: return S3FileIO's own InputFile untouched (ADR-0023 D13.2).
    // Our S3 client does not carry the customer key, so routing through it
    // would fail against real S3 rather than merely skip the cache.
    if (sseC) return super.newInputFile(path);
    return new FlintInputFile(path, -1, super.newInputFile(path));
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    if (sseC) return super.newInputFile(path, length);
    return new FlintInputFile(path, length, super.newInputFile(path, length));
  }

  @Override
  public void close() {
    try { super.close(); } finally { if (tier != null) tier.close(); }
  }

  /** Delegates everything except the byte stream. */
  private final class FlintInputFile implements InputFile {
    private final String location;
    private final long length;
    private final InputFile delegate;

    FlintInputFile(String location, long length, InputFile delegate) {
      this.location = location; this.length = length; this.delegate = delegate;
    }

    @Override public long getLength() { return length >= 0 ? length : delegate.getLength(); }
    @Override public String location() { return location; }
    @Override public boolean exists() { return delegate.exists(); }

    @Override
    public SeekableInputStream newStream() {
      S3URI uri = parse(location);
      try {
        return new AalSeekableStream(tier().caching.createStream(uri));
      } catch (IOException e) {
        throw new org.apache.iceberg.exceptions.RuntimeIOException(e);
      }
    }
  }

  /** s3://bucket/key, s3a://…, s3n://… all reduce to (bucket, key). */
  static S3URI parse(String location) {
    java.net.URI u = java.net.URI.create(location);
    return S3URI.of(u.getHost(), u.getPath().replaceFirst("^/", ""));
  }

  /** Iceberg's SeekableInputStream over AAL's. */
  static final class AalSeekableStream extends SeekableInputStream {
    private final S3SeekableInputStream in;
    AalSeekableStream(S3SeekableInputStream in) { this.in = in; }
    @Override public long getPos() { return in.getPos(); }
    @Override public void seek(long newPos) throws IOException { in.seek(newPos); }
    @Override public int read() throws IOException { return in.read(); }
    @Override public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }
    @Override public void close() throws IOException { in.close(); }
  }
}
