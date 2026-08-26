// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

import java.util.ArrayList;
import java.util.List;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * List keys matching a pattern, with SCAN rather than KEYS.
 *
 * <p><b>Flint does not implement KEYS.</b> Ten sites across these suites used
 * it, and every one passed for months because the gate resolves its tier with
 * {@code command -v valkey-server || command -v redis-server} and had never
 * run against the tier this product is actually for. The first run against
 * Flint failed eight stages on this alone.
 *
 * <p>SCAN is also the correct choice against a real Redis, where KEYS blocks
 * the server for the length of the keyspace — so this is a portability fix
 * that costs nothing and would have been right anyway.
 *
 * <p>Cursor-driven, because a single SCAN call returns an arbitrary slice: a
 * caller that read only the first page would silently undercount, which in a
 * suite full of "the tier got NO chunks" assertions is the direction that
 * passes.
 */
public final class TierScan {

  private TierScan() {}

  public static List<byte[]> keys(StatefulRedisConnection<byte[], byte[]> conn,
                                  String pattern) {
    return keys(conn.sync(), pattern);
  }

  /** Overload for callers holding the commands rather than the connection. */
  public static List<byte[]> keys(io.lettuce.core.api.sync.RedisCommands<byte[], byte[]> cmd,
                                  String pattern) {
    List<byte[]> out = new ArrayList<>();
    ScanCursor cur = ScanCursor.INITIAL;
    do {
      KeyScanCursor<byte[]> r = cmd.scan(cur, ScanArgs.Builder.matches(pattern).limit(1000));
      out.addAll(r.getKeys());
      cur = r;
    } while (!cur.isFinished());
    return out;
  }

  /** Convenience for the common `.size()` use. */
  public static long count(StatefulRedisConnection<byte[], byte[]> conn, String pattern) {
    return keys(conn, pattern).size();
  }
}
