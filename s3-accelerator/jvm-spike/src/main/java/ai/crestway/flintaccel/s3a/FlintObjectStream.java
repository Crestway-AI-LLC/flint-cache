// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.io.IOException;

import org.apache.hadoop.fs.s3a.impl.streams.InputStreamType;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStream;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectReadParameters;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;

/**
 * Hadoop's stream contract, satisfied by delegating to AAL's.
 *
 * AAL's S3SeekableInputStream already exposes read(), read(byte[],int,int),
 * seek(long), getPos() and close() -- exactly what FSInputStream needs -- so
 * this is a shim rather than an implementation. Everything interesting happens
 * below it, in the ObjectClient AAL was constructed with.
 */
final class FlintObjectStream extends ObjectInputStream {

  private final S3SeekableInputStream inner;
  private volatile boolean open = true;

  FlintObjectStream(ObjectReadParameters parameters, S3SeekableInputStream inner) {
    super(InputStreamType.Custom, parameters);
    this.inner = inner;
  }

  @Override public int read() throws IOException { return inner.read(); }

  @Override public int read(byte[] b, int off, int len) throws IOException {
    return inner.read(b, off, len);
  }

  @Override public void seek(long pos) throws IOException { inner.seek(pos); }

  @Override public long getPos() throws IOException { return inner.getPos(); }

  /**
   * False: there is one source. S3A calls this when a read fails and it wants
   * to know whether retrying against a different replica could help; for S3
   * there is no such thing, and saying true would invite a pointless retry
   * loop.
   */
  @Override public boolean seekToNewSource(long targetPos) { return false; }

  @Override protected boolean isStreamOpen() { return open; }

  @Override protected void abortInFinalizer() {
    open = false;
    try { inner.close(); } catch (IOException ignored) { }
  }

  @Override public synchronized void close() throws IOException {
    if (open) {
      open = false;
      inner.close();
    }
    super.close();
  }
}
