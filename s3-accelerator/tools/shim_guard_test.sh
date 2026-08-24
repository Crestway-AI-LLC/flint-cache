#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Proves the shim guard sees a classpath collision BEFORE it can break a job.
#
# The guard exists because our compatibility shim (ADR-0023 D12.18) occupies a
# package that is not ours. If a vendor Hadoop distribution also ships
# analyticsaccelerator.request.Constants, two copies land on one classpath and
# which wins is arbitrary -- a coin flip we would have introduced into a
# customer's working job.
#
# Fixtures are built here rather than checked in, because the whole point is
# a class in someone else's namespace and committing two more of them would
# be the same mistake at rest.
set -euo pipefail
cd "$(dirname "$0")/../jvm-spike"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21}"
export PATH="$JAVA_HOME/bin:$PATH"

W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT

mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile="$W/cp.txt"
CP=$(cat "$W/cp.txt")
SDK=$(tr ':' '\n' < "$W/cp.txt" | grep -E "awssdk/sdk-core|awssdk/s3/" | paste -sd: -)

mk() { # dir, source
  mkdir -p "$W/$1/src/software/amazon/s3/analyticsaccelerator/request" "$W/$1/out"
  cat > "$W/$1/src/software/amazon/s3/analyticsaccelerator/request/Constants.java"
  javac ${2:-} -d "$W/$1/out" "$W/$1/src/software/amazon/s3/analyticsaccelerator/request/Constants.java"
  (cd "$W/$1/out" && jar cf "$W/$1.jar" .)
}

# a vendor-shipped Constants with the CORRECT shape
mk vendor "-cp $SDK" <<'JAVA'
package software.amazon.s3.analyticsaccelerator.request;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
public final class Constants {
  public static final ExecutionAttribute<String> SPAN_ID = new ExecutionAttribute<>("V_SPAN");
  public static final ExecutionAttribute<String> OPERATION_NAME = new ExecutionAttribute<>("V_OP");
}
JAVA

# a Constants with the WRONG shape -- hadoop would die on getstatic
mk bad "" <<'JAVA'
package software.amazon.s3.analyticsaccelerator.request;
public final class Constants {
  public static final String SPAN_ID = "nope";
  public static final String OPERATION_NAME = "nope";
}
JAVA

# our shim, packaged alone as it would actually ship
mkdir -p "$W/ours"
cp -r target/classes/software "$W/ours/"
(cd "$W/ours" && jar cf "$W/ours.jar" .)

# classes WITHOUT the shim, so ABSENT is a real absence
cp -r target/classes "$W/tc"
rm -rf "$W/tc/software/amazon/s3"

mkdir -p "$W/g"
cat > "$W/G.java" <<'JAVA'
import java.io.File; import java.net.*;
import ai.crestway.flintaccel.s3a.ShimGuard;
public class G {
  static int rc = 0;
  static ClassLoader cl(String... jars) throws Exception {
    URL[] u = new URL[jars.length];
    for (int i = 0; i < jars.length; i++) u[i] = new File(jars[i]).toURI().toURL();
    return new URLClassLoader(u, G.class.getClassLoader());
  }
  static void t(String label, String want, ClassLoader c) {
    ShimGuard g = ShimGuard.inspect(c);
    boolean ok = g.state.name().equals(want);
    if (!ok) rc = 1;
    System.out.printf("[%s] %-28s -> %-11s (%d copies)%n",
        ok ? "ok" : "FAIL", label, g.state, g.locations.size());
    if (g.state != ShimGuard.State.SINGLE) System.out.println("      " + g.detail);
  }
  public static void main(String[] a) throws Exception {
    t("no shim, no vendor", "ABSENT", cl());
    t("ours only", "SINGLE", cl(a[0]));
    t("vendor only", "SINGLE", cl(a[1]));
    t("ours + vendor (COLLISION)", "COLLISION", cl(a[0], a[1]));
    t("wrong-shape Constants", "WRONG_SHAPE", cl(a[2]));
    System.out.println("\nSHIM GUARD " + (rc == 0 ? "PASSED" : "FAILED"));
    System.exit(rc);
  }
}
JAVA
javac -cp "target/classes:$CP" -d "$W/g" "$W/G.java"
java -cp "$W/g:$W/tc:$CP" G "$W/ours.jar" "$W/vendor.jar" "$W/bad.jar"
