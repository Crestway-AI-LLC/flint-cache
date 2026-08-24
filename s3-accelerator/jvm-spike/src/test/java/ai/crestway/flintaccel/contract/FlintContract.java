// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.contract;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import ai.crestway.flintaccel.s3a.FlintStreamFactory;

/**
 * Binds Hadoop's own contract suite to FlintS3AFileSystem.
 *
 * These tests are the most valuable thing available to us and we did not write
 * a line of them: 18 seek tests and 27 open tests by Hadoop committers, and
 * they are adversarial about exactly the seek/read/EOF boundaries where a
 * chunk-reassembling stream breaks. Our own suites assert the properties we
 * thought to check; this one asserts the properties a decade of FileSystem
 * bugs taught someone else to check.
 *
 * Endpoint and tier come from system properties so the same class runs against
 * the local fixture or a real bucket.
 */
public class FlintContract extends AbstractFSContract {

  public static final String ENDPOINT =
      System.getProperty("flint.test.endpoint", "http://127.0.0.1:9000");
  public static final String TIER =
      System.getProperty("flint.test.tier", "redis://127.0.0.1:6399");
  public static final String BUCKET =
      System.getProperty("flint.test.bucket", "bucket");

  public FlintContract(Configuration conf) {
    super(conf);
    addConfResource("flint-contract.xml");
  }

  @Override public String getScheme() { return "s3a"; }

  @Override public Path getTestPath() { return new Path("/contract-test"); }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    Configuration c = new Configuration(getConf());
    c.set("fs.s3a.endpoint", ENDPOINT);
    c.set("fs.s3a.endpoint.region", "us-east-1");
    c.setBoolean("fs.s3a.path.style.access", true);
    c.set("fs.s3a.access.key", "contract");
    c.set("fs.s3a.secret.key", "contract");
    c.setInt("fs.s3a.bucket.probe", 0);
    c.set("fs.s3a.change.detection.mode", "none");
    c.set("fs.s3a.impl", "ai.crestway.flintaccel.s3a.FlintS3AFileSystem");
    c.set(FlintStreamFactory.TIER_URI, TIER);
    // Multipart is not implemented by the fixture; keep writes single-part.
    c.setLong("fs.s3a.multipart.size", 512L * 1024 * 1024);
    c.setLong("fs.s3a.multipart.threshold", 512L * 1024 * 1024);
    return FileSystem.newInstance(URI.create("s3a://" + BUCKET + "/"), c);
  }
}
