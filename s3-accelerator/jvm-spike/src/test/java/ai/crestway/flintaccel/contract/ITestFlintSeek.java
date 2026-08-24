// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/** Hadoop's 18 seek tests, against our chunk-reassembling stream. */
public class ITestFlintSeek extends AbstractContractSeekTest {
  @Override protected AbstractFSContract createContract(Configuration conf) {
    return new FlintContract(conf);
  }
}
