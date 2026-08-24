// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.s3a;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import software.amazon.awssdk.core.interceptor.ExecutionAttribute;

/**
 * Guards the D12.18 compatibility shim against breaking a job that works.
 *
 * The shim puts a class in someone else's package. If a vendor Hadoop
 * distribution (or a future AAL release) also supplies
 * `analyticsaccelerator.request.Constants`, two copies sit on one classpath and
 * which one wins is arbitrary — a coin flip we introduced into a customer's
 * production job.
 *
 * A class cannot decline to load once it has won that flip, so detection has
 * to happen ahead of use, by ENUMERATING the classpath rather than resolving
 * the name. `ClassLoader.getResources` finds every copy; `Class.forName` finds
 * only the winner and therefore cannot see a collision at all.
 *
 * Three outcomes, and all three are reported rather than assumed:
 *
 *   0 copies  -- on hadoop-aws 3.4.3+ the job will die on its first S3
 *                request, in the audit interceptor, with a NoClassDefFoundError
 *                that names nothing useful. Say so now, with the three fixes.
 *   1 copy    -- fine, whether it is ours or theirs.
 *   2+ copies -- collision. Name the jars. Ours is the one to remove, because
 *                theirs is the one their working job already depends on.
 *
 * Shape is checked too. A real Constants with different fields would throw
 * NoSuchFieldError deep inside the AWS SDK's interceptor chain; catching that
 * here turns it into a sentence.
 */
public final class ShimGuard {

  public static final String CLASS_NAME =
      "software.amazon.s3.analyticsaccelerator.request.Constants";
  private static final String RESOURCE =
      "software/amazon/s3/analyticsaccelerator/request/Constants.class";

  public enum State { ABSENT, SINGLE, COLLISION, WRONG_SHAPE }

  public final State state;
  public final List<String> locations;
  public final String detail;

  private ShimGuard(State s, List<String> locs, String detail) {
    this.state = s; this.locations = locs; this.detail = detail;
  }

  public static ShimGuard inspect(ClassLoader cl) {
    List<String> locs = new ArrayList<>();
    try {
      Enumeration<URL> e = cl.getResources(RESOURCE);
      while (e.hasMoreElements()) locs.add(e.nextElement().toString());
    } catch (Exception ex) {
      return new ShimGuard(State.ABSENT, locs, "could not enumerate: " + ex);
    }
    if (locs.isEmpty()) {
      return new ShimGuard(State.ABSENT, locs,
          "no " + CLASS_NAME + " on the classpath. On hadoop-aws 3.4.3+ every S3 "
          + "request will fail in the audit interceptor. Fixes: add the "
          + "flint-accel-hadoop-shim artifact, or set fs.s3a.audit.enabled=false, "
          + "or use hadoop-aws 3.4.2.");
    }
    // Shape check on whichever copy actually won.
    try {
      Class<?> c = Class.forName(CLASS_NAME, false, cl);
      for (String f : new String[]{"SPAN_ID", "OPERATION_NAME"}) {
        Field fl = c.getField(f);
        if (!ExecutionAttribute.class.isAssignableFrom(fl.getType())) {
          return new ShimGuard(State.WRONG_SHAPE, locs,
              CLASS_NAME + "." + f + " is " + fl.getType().getName()
              + ", not ExecutionAttribute. hadoop-aws will fail on it.");
        }
      }
    } catch (Throwable t) {
      return new ShimGuard(State.WRONG_SHAPE, locs,
          CLASS_NAME + " lacks the fields hadoop-aws reads (" + t + ")");
    }
    if (locs.size() > 1) {
      return new ShimGuard(State.COLLISION, locs,
          locs.size() + " copies of " + CLASS_NAME + " on the classpath. Remove the "
          + "flint-accel-hadoop-shim artifact: the other copy is the one this "
          + "deployment already worked with.");
    }
    return new ShimGuard(State.SINGLE, locs, "one copy: " + locs.get(0));
  }

  /** True when it is safe to proceed without risking a working job. */
  public boolean safe() { return state == State.SINGLE; }

  @Override public String toString() { return state + ": " + detail; }
}
