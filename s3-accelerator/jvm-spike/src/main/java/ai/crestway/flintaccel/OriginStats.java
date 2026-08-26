// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel;

/**
 * Read one integer counter out of a counting-origin's {@code /__stats}.
 *
 * <p>Every counter assertion in the JVM suites rests on this read -- "the warm
 * read cost no origin GETs", "N concurrent readers cost 1", "above the cap the
 * tier got no chunks". It existed as seven hand-rolled copies of
 *
 * <pre>  int i = b.indexOf("\"gets\":");
 *  return Integer.parseInt(b.substring(i + 7, b.indexOf(',', i)).trim());</pre>
 *
 * <p>with the offset written as a literal per field (+7 for {@code gets}, +8
 * for {@code heads}, +13 for {@code generation}), which is what made each copy
 * its own chance to be wrong. Measured against the real {@code /__stats} body,
 * which begins {@code {"counting_enabled": true, "gets": N, ...}}:
 *
 * <ul>
 *   <li><b>A missing field throws, but unhelpfully.</b> {@code indexOf} returns
 *       -1, so the slice starts at 6 and the parse reports
 *       {@code NumberFormatException: For input string: "ting_enabled": true}.
 *       Loud, and it names nothing that leads back to the cause.
 *   <li><b>THE LAST FIELD CRASHES.</b> {@code indexOf(',', start)} returns -1
 *       when the counter is last in the document, and
 *       {@code substring(start, -1)} throws
 *       {@code StringIndexOutOfBoundsException}. No caller reads a last field
 *       today; one reordering of the snapshot dict is all it takes.
 *   <li><b>The silent case is real but not reachable here.</b> A body whose
 *       first field's value happens to sit at offset 6 returns a wrong number
 *       with no error at all -- {@code {"a":123,"g":7}} yields 23. This
 *       fixture's field names are too long for it. Recorded because the layout
 *       is the fixture's to change, not this parser's to rely on.
 * </ul>
 *
 * <p>So: anchored, named in its own error, and with a {@code \}} fallback for
 * the last field.
 *
 * <p><b>A verification helper that cannot fail is worth less than no helper</b>,
 * because its silence is read as a pass.
 */
public final class OriginStats {

  private OriginStats() {}

  /** Fetch {@code /__stats} from the endpoint and read one counter from it. */
  public static int field(String endpoint, String name) throws Exception {
    String body = java.net.http.HttpClient.newHttpClient().send(
        java.net.http.HttpRequest.newBuilder(
            java.net.URI.create(endpoint + "/__stats")).build(),
        java.net.http.HttpResponse.BodyHandlers.ofString()).body();
    return parse(body, name);
  }

  /** Split out from the fetch so the parse is exercisable without an origin. */
  public static int parse(String body, String name) {
    String key = "\"" + name + "\":";
    int i = body.indexOf(key);
    if (i < 0) {
      throw new IllegalStateException(
          "no " + key + " in /__stats -- the counter this assertion reads does "
          + "not exist, so the assertion proves nothing about the origin. Body: "
          + body);
    }
    int start = i + key.length();
    int end = body.indexOf(',', start);
    if (end < 0) end = body.indexOf('}', start);
    if (end < 0) {
      throw new IllegalStateException("unterminated " + key + " in: " + body);
    }
    return Integer.parseInt(body.substring(start, end).trim());
  }
}
