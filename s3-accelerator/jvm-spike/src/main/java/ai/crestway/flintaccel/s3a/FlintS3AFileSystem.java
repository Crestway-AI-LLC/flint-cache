// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.util.LambdaUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

import ai.crestway.flintaccel.client.TierSupport;

/**
 * The universal path: `fs.s3a.impl=ai.crestway.flintaccel.s3a.FlintS3AFileSystem`.
 *
 * For customers on hadoop-aws older than 3.4.2 there is no `Custom` stream type
 * to register with (ADR-0023 D12.19), and if they do not read Iceberg tables
 * there is no `io-impl` either. This is what is left, and it works from 3.3.x
 * upward because `fs.s3a.impl` is a documented key whose default value is
 * `S3AFileSystem` itself.
 *
 * We override the READ entry points and nothing else. Writes, listings,
 * deletes, credentials, delegation tokens and every other S3A behaviour are
 * inherited untouched — which is the whole argument for subclassing rather
 * than reimplementing.
 *
 * BOTH doors, deliberately (D12.22). `open()` is the obvious one;
 * `openFileWithOptions()` is where `openFile()` lands, and Spark and Parquet
 * increasingly use it for length hints and read policy. Overriding one leaves
 * a cache that silently never engages for the readers that matter most —
 * correct results, no acceleration, no error. The same shape has now bitten at
 * three separate seams in this ecosystem.
 */
public class FlintS3AFileSystem extends S3AFileSystem {

  private volatile TierSupport tier;

  /**
   * True when this filesystem is configured for SSE-C, in which case NOTHING
   * is cached (ADR-0023 D13).
   *
   * Decided once at initialize() rather than per read, because
   * fs.s3a.encryption.algorithm is a filesystem-level (or per-bucket) setting.
   * Deciding once and conservatively is the right error direction: caching
   * plaintext the customer protected with their own key is a breach, while
   * declining to cache a bucket that turned out not to need it is a slow read.
   *
   * NOTE this was missing until it was looked for. The stream-type path
   * checked SSE-C from the start; this path and the Iceberg one did not, so
   * two of three entry points would have cached SSE-C plaintext. A control
   * that only guards one door guards nothing.
   */
  private volatile boolean sseC;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    String bucket = name.getHost();
    String algo = conf.get("fs.s3a.bucket." + bucket + ".encryption.algorithm",
        conf.get("fs.s3a.encryption.algorithm", ""));
    this.sseC = "SSE-C".equalsIgnoreCase(algo) || "SSE_C".equalsIgnoreCase(algo);

    // Under SSE-C we hand reads back to S3A (D13.2) -- but S3A's DEFAULT
    // stream type is Analytics, and on hadoop-aws 3.4.3+ that needs AAL
    // classes no published release contains. Handing the read back would swap
    // a privacy problem for a NoClassDefFoundError.
    //
    // Our shim supplies request.Constants for the AUDIT path; the analytics
    // stream needs util.RequestCallback and whatever else besides. Chasing
    // that set is a rabbit hole with no bottom, so pin the fallback to the
    // classic stream instead: we are not using AAL for these reads anyway.
    // Set BEFORE super.initialize(), since that is when the factory is built.
    if (sseC && conf.get("fs.s3a.input.stream.type") == null) {
      conf.set("fs.s3a.input.stream.type", "classic");
    }
    super.initialize(name, conf);
    // Read our keys off the Hadoop Configuration, mapping the shared names.
    // Every flint.* setting maps to fs.s3a.flint.* BY RULE, not by a case
    // label. The previous version enumerated four keys with `default -> null`,
    // so flint.cache.sse-kms -- added later, and printed to customers by the
    // preflight script -- resolved to null and the opt-in it documents could
    // not be turned on. An allowlist fails closed, which is right, but it does
    // so SILENTLY and one key at a time, and nothing fails when a key is
    // forgotten. Only the s3.* names, which genuinely differ from Hadoop's,
    // still need enumerating.
    this.tier = TierSupport.build(k -> k.startsWith("flint.")
        ? conf.get("fs.s3a." + k)
        : switch (k) {
      case "s3.endpoint"           -> conf.get("fs.s3a.endpoint");
      case "s3.region"             -> conf.get("fs.s3a.endpoint.region");
      case "s3.path-style-access"  -> conf.get("fs.s3a.path.style.access");
      case "s3.access-key-id"      -> conf.get("fs.s3a.access.key");
      case "s3.secret-access-key"  -> conf.get("fs.s3a.secret.key");
      default -> null;
    });
  }

  /** Door 1. */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    // SSE-C: hand the read back to S3A entirely (ADR-0023 D13.2).
    //
    // Bypassing our CACHE is not enough. We also build our own S3AsyncClient,
    // and it does not carry the customer's key -- so a bypassing read still
    // went out without the SSE-C headers and real S3 would reject it. The
    // fixture served it anyway, which is exactly how this hid.
    if (sseC) return super.open(f, bufferSize);
    return new FSDataInputStream(cached(f));
  }

  /** Door 2 — where openFile() lands, and where a one-door override leaks. */
  @Override
  public CompletableFuture<FSDataInputStream> openFileWithOptions(
      Path path, OpenFileParameters parameters) throws IOException {
    if (sseC) return super.openFileWithOptions(path, parameters);
    // The builder contract is not "open, then wrap in a future". Hadoop's
    // suite is explicit about two things the first version got wrong:
    // failure must be LAZY -- reported when the future is awaited, not thrown
    // from the call -- and unknown MANDATORY keys must be rejected, because a
    // caller who says "you must honour this" and is silently ignored has been
    // lied to. Neither is discoverable from the signature, and none of our own
    // tests asked.
    AbstractFSBuilderImpl.rejectUnknownMandatoryKeys(
        parameters.getMandatoryKeys(),
        Options.OpenFileOptions.FS_OPTION_OPENFILE_STANDARD_OPTIONS,
        "for " + path);
    return LambdaUtils.eval(new CompletableFuture<>(),
        () -> new FSDataInputStream(cached(path)));
  }

  private FSInputStream cached(Path f) throws IOException {
    Path abs = makeQualified(f);
    String bucket = abs.toUri().getHost();
    String key = abs.toUri().getPath().replaceFirst("^/", "");
    // AAL does its own HEAD through our ObjectClient, so there is nothing to
    // resolve here — the metadata lands in the tier on the way past.
    return new AalStream(tier.caching.createStream(S3URI.of(bucket, key)));
  }

  /** s3://bucket/key form, matching what AAL hands our client. */
  private void invalidate(Path f) {
    if (tier == null) return;
    try {
      Path abs = makeQualified(f);
      String b = abs.toUri().getHost();
      String k = abs.toUri().getPath().replaceFirst("^/", "");
      tier.client.invalidate(software.amazon.s3.analyticsaccelerator.util.S3URI.of(b, k));
    } catch (RuntimeException ignored) {
      // Invalidation is best-effort; the TTL is the backstop.
    }
  }

  @Override
  public org.apache.hadoop.fs.FSDataOutputStream create(
      Path f, org.apache.hadoop.fs.permission.FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      org.apache.hadoop.util.Progressable progress) throws IOException {
    invalidate(f);          // before, so a concurrent reader cannot re-cache
    var out = super.create(f, permission, overwrite, bufferSize, replication,
        blockSize, progress);
    invalidate(f);          // and after, because the write is what changed it
    return out;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    boolean r = super.delete(f, recursive);
    invalidate(f);
    return r;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    boolean r = super.rename(src, dst);
    invalidate(src);
    invalidate(dst);
    return r;
  }

  @Override
  public void close() throws IOException {
    try { super.close(); } finally { if (tier != null) tier.close(); }
  }

  /**
   * AAL's stream has FSInputStream's SHAPE but not its CONTRACT.
   *
   * The first version delegated every call straight through, on the reasoning
   * that the signatures matched. Hadoop's contract suite disagreed in nine
   * places at once, all of them about arguments a caller should never have
   * passed and every FileSystem must reject anyway:
   *
   *   seek(-1)              EOFException, not whatever the delegate does
   *   read() after close()  IOException naming the stream as closed
   *   read(b, off, 0)       0, without touching the delegate
   *   read(-1, b, ...)      EOFException
   *
   * Matching signatures is not implementing an interface. None of this is
   * visible from the type, and none of our own tests asked -- every read we
   * wrote passed arguments a sensible caller would pass.
   */
  static final class AalStream extends FSInputStream {
    private final S3SeekableInputStream in;
    private volatile boolean closed;

    AalStream(S3SeekableInputStream in) { this.in = in; }

    private void checkOpen() throws IOException {
      if (closed) throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
    }

    @Override public int read() throws IOException {
      checkOpen();
      return in.read();
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
      checkOpen();
      if (b == null) throw new NullPointerException();
      if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      }
      if (len == 0) return 0;          // must not be reported as EOF
      return in.read(b, off, len);
    }

    @Override public void seek(long pos) throws IOException {
      checkOpen();
      if (pos < 0) throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + pos);
      in.seek(pos);
    }

    @Override public int read(long position, byte[] b, int off, int len)
        throws IOException {
      checkOpen();
      if (position < 0) {
        throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + position);
      }
      if (len == 0) return 0;
      return super.read(position, b, off, len);
    }

    @Override public long getPos() { return in.getPos(); }

    @Override public boolean seekToNewSource(long targetPos) { return false; }

    @Override public int available() throws IOException {
      checkOpen();
      return 0;
    }

    @Override public void close() throws IOException {
      if (!closed) {
        closed = true;
        in.close();
      }
    }
  }
}
