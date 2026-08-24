// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/** Hadoop's 27 open tests, against our overridden open()/openFile(). */
public class ITestFlintOpen extends AbstractContractOpenTest {
  @Override protected AbstractFSContract createContract(Configuration conf) {
    return new FlintContract(conf);
  }

  /**
   * S3 encrypts at rest by default, so S3AFileStatus reports isEncrypted=true
   * even for a zero-byte object. That is correct behaviour inherited from
   * S3AFileSystem, not something our read path affects -- the suite asks the
   * subclass to declare it, and S3A's own contract test declares the same.
   */
  @Override protected boolean areZeroByteFilesEncrypted() { return true; }
}
