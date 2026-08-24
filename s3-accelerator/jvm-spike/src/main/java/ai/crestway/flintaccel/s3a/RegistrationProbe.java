// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.streams.InputStreamType;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamFactory;
import org.apache.hadoop.fs.s3a.impl.streams.StreamIntegration;

/** Does S3A actually load a third-party factory from configuration? */
public final class RegistrationProbe {

  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  public static void main(String[] args) {
    // The two keys D12.14 read out of the released jar.
    Configuration conf = new Configuration(false);
    conf.set("fs.s3a.input.stream.type", "custom");
    conf.set("fs.s3a.input.stream.custom.factory", FlintStreamFactory.class.getName());

    InputStreamType t = StreamIntegration.determineInputStreamType(conf);
    check(t == InputStreamType.Custom,
        "fs.s3a.input.stream.type=custom resolves to InputStreamType.Custom (got " + t + ")");

    FlintStreamFactory.CONSTRUCTED = false;
    ObjectInputStreamFactory f = StreamIntegration.factoryFromConfig(conf);
    check(f instanceof FlintStreamFactory,
        "S3A instantiated OUR class from fs.s3a.input.stream.custom.factory ("
            + (f == null ? "null" : f.getClass().getName()) + ")");
    check(FlintStreamFactory.CONSTRUCTED,
        "armed-check: the constructor really ran, so this is not a cached instance");
    check(f != null && f.streamType() == InputStreamType.Custom,
        "the loaded factory reports streamType() = Custom");

    // Negative control: a config WITHOUT our key must not yield our class.
    Configuration plain = new Configuration(false);
    FlintStreamFactory.CONSTRUCTED = false;
    ObjectInputStreamFactory dflt = StreamIntegration.factoryFromConfig(plain);
    check(!(dflt instanceof FlintStreamFactory) && !FlintStreamFactory.CONSTRUCTED,
        "negative control -- default config loads " + dflt.getClass().getSimpleName()
            + ", not ours");

    // The key must be exactly the one D12.14 read; the DERIVED key must fail.
    Configuration wrong = new Configuration(false);
    wrong.set("fs.s3a.input.stream.type", "custom");
    wrong.set("fs.s3a.input.stream.type.custom", FlintStreamFactory.class.getName());
    boolean rejected;
    try {
      StreamIntegration.factoryFromConfig(wrong);
      rejected = false;
    } catch (RuntimeException e) {
      rejected = true;
    }
    check(rejected,
        "the key DERIVED in D12.2 (fs.s3a.input.stream.type.custom) is rejected, "
            + "confirming the corrected one is load-bearing");

    System.out.println("\nREGISTRATION PROBE " + (ok ? "PASSED" : "FAILED"));
    System.exit(ok ? 0 : 1);
  }
}
