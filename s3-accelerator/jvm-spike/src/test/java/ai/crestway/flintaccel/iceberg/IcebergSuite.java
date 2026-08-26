// SPDX-License-Identifier: Apache-2.0
package ai.crestway.flintaccel.iceberg;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.util.*;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

/**
 * The Iceberg path, end to end, through Iceberg's own machinery.
 *
 * This is the one adoption route with NO inheritable contract suite. Hadoop
 * gave us AbstractContractSeekTest and found six real defects on its first run;
 * fsspec gave us tests.abstract and found the Python path's gaps. Iceberg has
 * no equivalent for FileIO, so the alternative to writing this is testing
 * FlintFileIO only against suites we wrote -- exactly the condition that proved
 * inadequate twice.
 *
 * So instead of unit-testing the class, this builds a REAL table: a catalog
 * configured with nothing but `io-impl`, a schema, Avro data files written
 * through the FileIO, a commit, and a read back through IcebergGenerics. Every
 * layer between the config line and the bytes is Iceberg's own code.
 *
 * The claim under test is the sales claim: ONE configuration line puts us in
 * the read path, and table reads then come from the tier.
 */
public final class IcebergSuite {

  static final HttpClient HTTP = HttpClient.newHttpClient();
  static String endpoint;
  static boolean ok = true;

  static void check(boolean c, String label) {
    ok &= c;
    System.out.printf("[%s] %s%n", c ? "ok" : "FAIL", label);
  }

  static void note(String s) { System.out.println("       " + s); }

  static int gets() throws Exception {
    String b = HTTP.send(HttpRequest.newBuilder(URI.create(endpoint + "/__stats")).build(),
        HttpResponse.BodyHandlers.ofString()).body();
    return ai.crestway.flintaccel.OriginStats.parse(b, "gets");
  }

  public static void main(String[] args) throws Exception {
    endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:9000";
    String tierUri = args.length > 1 ? args[1] : "redis://127.0.0.1:9399";

    RedisClient rc = RedisClient.create(tierUri);
    StatefulRedisConnection<byte[], byte[]> conn = rc.connect(new ByteArrayCodec());
    conn.sync().flushall();

    // ---------------------------------------------------------------- setup
    // Exactly the properties a customer sets. io-impl is the ONLY line that
    // mentions us; everything else is standard Iceberg S3 configuration.
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.FILE_IO_IMPL, FlintFileIO.class.getName());
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://bucket/wh");
    props.put("s3.endpoint", endpoint);
    props.put("s3.path-style-access", "true");
    // Iceberg builds its own S3 client and reads client.region, NOT
    // s3.region. Without it the SDK falls through to the ambient region
    // chain -- a developer laptop with ~/.aws/config passes, a clean CI
    // runner throws "Unable to load region from any of the providers".
    props.put("client.region", "us-east-1");
    props.put("s3.access-key-id", "ice");
    props.put("s3.secret-access-key", "ice");
    props.put("s3.region", "us-east-1");
    props.put("flint.tier.uri", tierUri);

    // HadoopCatalog, NOT InMemoryCatalog. InMemoryCatalog hardcodes its own
    // InMemoryFileIO and ignores io-impl entirely, so it cannot observe the
    // very property this suite exists to test -- it reported "our FileIO is
    // not in the path" for a reason that had nothing to do with our code.
    // Choosing an instrument that CAN see the thing you are measuring is half
    // the work.
    //
    // HadoopCatalog does what every real catalog does:
    //   CatalogUtil.loadFileIO(properties.get("io-impl"), properties, conf)
    // the same line JdbcCatalog, RESTCatalog and GlueCatalog run. So a pass
    // here is a pass for the catalogs customers actually deploy.
    Configuration conf = new Configuration();
    conf.set("fs.s3a.endpoint", endpoint);
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.access.key", "ice");
    conf.set("fs.s3a.secret.key", "ice");
    conf.set("fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    // Pin S3A to its CLASSIC stream. Not tidying: hadoop-aws 3.4.3 defaults to
    // its own AnalyticsStreamFactory when AAL is on the classpath, and that
    // factory references software.amazon.s3.analyticsaccelerator.util.
    // RequestCallback, which AAL 1.1.0 does not ship -- NoClassDefFoundError
    // inside S3A's own open(), before any of our code runs. That is the SECOND
    // hadoop-aws 3.4.3 / AAL 1.1.0 skew found (the first needed the
    // request.Constants shim), and it is the argument for shading AAL rather
    // than asking customers to put it on their classpath.
    //
    // Here S3A is used only for HadoopCatalog's directory operations; table
    // data goes through FlintFileIO, so classic is the right and honest
    // setting rather than a workaround hiding a defect of ours.
    conf.set("fs.s3a.input.stream.type", "classic");
    conf.set("fs.s3a.change.detection.mode", "none");
    conf.set("fs.s3a.change.detection.version.required", "false");

    HadoopCatalog cat = new HadoopCatalog();
    cat.setConf(conf);
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://bucket/wh");
    cat.initialize("flint", props);

    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "payload", Types.StringType.get()));

    // Both formats, because they read the object COMPLETELY differently and
    // only one of them was ever exercised.
    //
    // Avro is a sequential scan. Parquet reads the footer length from the LAST
    // bytes of the object, seeks BACKWARDS to the footer, parses it, then jumps
    // to individual column chunks. Tiny reads at the very end of an object and
    // backward seeks are precisely where an absolute-offset chunk grid can be
    // wrong while every sequential test stays green -- and Parquet is what real
    // Iceberg tables actually use.
    for (FileFormat fmt : new FileFormat[] { FileFormat.AVRO, FileFormat.PARQUET }) {
      System.out.println("--- " + fmt + " ---");
      runFormat(cat, props, conf, schema, fmt);
    }

    // ---------------------------------------------------------------- 7
    // What SPARK does to this object, without Spark.
    //
    // FileIO extends Serializable and Spark ships the catalog's instance to
    // every executor. Our TierSupport is not serialisable and cannot be -- it
    // owns sockets -- so the field is transient and rebuilt lazily on the far
    // side from the properties. That rebuild is a load-bearing assumption
    // written in a docstring and never once executed: if `props` did not
    // survive, or the lazy path NPEs, every Spark read fails on the executor
    // while every test here passes in the driver.
    //
    // A round trip through ObjectOutputStream is precisely what Spark's task
    // serialisation does, and it costs no Spark dependency to run.
    Table pq = cat.loadTable(TableIdentifier.of("db", "t_parquet"));
    String dataFile = null;
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks = pq.newScan().planFiles()) {
      for (org.apache.iceberg.FileScanTask t : tasks) { dataFile = t.file().location(); break; }
    }
    check(dataFile != null, "armed: found a real data file to read (" + dataFile + ")");

    byte[] wire;
    try (ByteArrayOutputStream bo = new ByteArrayOutputStream();
         ObjectOutputStream oo = new ObjectOutputStream(bo)) {
      oo.writeObject(pq.io());
      wire = bo.toByteArray();
    }
    check(wire.length > 0, "the FileIO SERIALISES at all (" + wire.length + " bytes)");

    FileIO revived;
    try (ObjectInputStream oi = new ObjectInputStream(new ByteArrayInputStream(wire))) {
      revived = (FileIO) oi.readObject();
    }
    check(revived instanceof FlintFileIO,
        "and deserialises back to OUR FileIO, not a degraded S3FileIO");

    // The deserialised copy has a null tier and must rebuild it from props.
    conn.sync().flushall();
    InputFile in = revived.newInputFile(dataFile);
    byte[] viaRevived;
    try (InputStream st = in.newStream()) { viaRevived = st.readAllBytes(); }
    check(viaRevived.length > 0,
        "AN EXECUTOR-SIDE COPY READS (" + viaRevived.length + " bytes) -- the lazy "
        + "tier rebuild works");
    long revivedChunks = conn.sync().keys("c2/*".getBytes()).size();
    check(revivedChunks > 0,
        "armed: and it went through OUR tier (" + revivedChunks + " chunks) rather than "
        + "silently falling back to plain S3");

    // Same bytes as the driver-side copy, or the executor is reading something
    // else -- which is the failure this whole product must never have.
    byte[] viaOriginal;
    try (InputStream st = pq.io().newInputFile(dataFile).newStream()) {
      viaOriginal = st.readAllBytes();
    }
    check(Arrays.equals(viaOriginal, viaRevived),
        "and the executor copy returns EXACTLY the driver's bytes");

    conn.close(); rc.shutdown();
    try { cat.close(); } catch (Exception ignored) { }
    System.out.println(ok ? "ICEBERG SUITE PASSED" : "ICEBERG SUITE FAILED");
    System.exit(ok ? 0 : 1);
  }

  static void runFormat(HadoopCatalog cat, Map<String, String> props, Configuration conf,
                        Schema schema, FileFormat fmt) throws Exception {
    RedisClient rc2 = RedisClient.create(props.get("flint.tier.uri"));
    StatefulRedisConnection<byte[], byte[]> conn = rc2.connect(new ByteArrayCodec());

    // Idempotent: the fixture keeps written objects for the life of the
    // process, so a second run in the same origin would hit AlreadyExists.
    TableIdentifier id = TableIdentifier.of("db", "t_" + fmt.name().toLowerCase());
    try { cat.dropTable(id, true); } catch (RuntimeException ignored) { }

    Table table = cat.createTable(id, schema, PartitionSpec.unpartitioned(),
        Map.of(TableProperties.DEFAULT_FILE_FORMAT, fmt.name().toLowerCase()));

        // ---------------------------------------------------------------- 1
      // The claim that sells the integration: one config line, and Iceberg is
      // holding OUR FileIO. If this fails nothing below means anything, because
      // the reads would be going straight to S3 and returning correct results.
      check(table.io() instanceof FlintFileIO,
          "ONE config line and Iceberg built our FileIO (" + table.io().getClass().getSimpleName() + ")");

        // ---------------------------------------------------------------- 2
      // Write a real table. Data files go through the SAME FileIO, so the write
      // path is exercised by construction rather than asserted.
      final int ROWS = 120_000;
      GenericAppenderFactory af = new GenericAppenderFactory(schema);
      OutputFile out = table.io().newOutputFile(table.location() + "/data/part-0." + fmt.name().toLowerCase() + "");
      // The appender must be CLOSED before the DataFile is built: withInputFile
      // does a HEAD, and until close() the object has not been uploaded. Building
      // it inside the try-with-resources produced a 404 that looked like a
      // fixture problem and was mine.
      FileAppender<Record> app = af.newAppender(out, fmt);
      try (app) {
        for (int i = 0; i < ROWS; i++) {
          GenericRecord r = GenericRecord.create(schema);
          r.setField("id", (long) i);
          // Self-describing, borrowing the chaos oracle's discipline: a row
          // delivered from the wrong offset is detectable as WRONG, not merely
          // different, so a reassembly bug cannot pass as "some rows returned".
          r.setField("payload", "row-" + i + "-" + Integer.toHexString(i * 2654435761L != 0
              ? (int) ((i * 2654435761L) & 0xFFFFFF) : 0));
          app.add(r);
        }
      }
      DataFile df = DataFiles.builder(PartitionSpec.unpartitioned())
          .withInputFile(out.toInputFile())
          .withMetrics(app.metrics())
          .withFormat(fmt)
          .build();
      table.newAppend().appendFile(df).commit();
      note("wrote " + ROWS + " rows to " + table.location());

        // ---------------------------------------------------------------- 3
      // Every read below uses a FRESH catalog, and that is the whole
      // measurement rather than hygiene.
      //
      // AAL keeps its own in-process cache, and one FileIO holds one AAL factory
      // for its lifetime. Reading twice through the SAME FileIO therefore
      // measures AAL's memory, not our tier -- the second read returned 0 origin
      // GETs even with the tier flushed completely, which is AAL working
      // exactly as designed and our cache contributing nothing observable.
      // Without the negative control that caught it, this suite would have
      // reported "warm Iceberg reads cost 0 origin GETs" as evidence for a
      // product that had not been exercised.
      //
      // A fresh catalog per phase gives a cold AAL cache over a warm shared
      // tier, which is also the deployment that sells the integration: the
      // SECOND query, in a different executor, on a different machine.
      final int ROWS_EXPECTED = ROWS;

      conn.sync().flushall();
      int g0 = gets();
      long cold = withFreshCatalog(props, conf, id, t -> readAndVerify(t, ROWS_EXPECTED));
      int coldGets = gets() - g0;
      check(cold == ROWS, "cold read returned all " + ROWS + " rows, every payload verified");
      check(coldGets > 0, "negative control: the cold read DID reach the origin ("
          + coldGets + " GETs)");
      long chunks = conn.sync().keys("c2/*".getBytes()).size();
      check(chunks > 0, "armed: the read populated the tier (" + chunks + " chunks)");

        // ---------------------------------------------------------------- 4
      // A DIFFERENT process's view: cold memory, warm tier. This is the product.
      int g1 = gets();
      long warm = withFreshCatalog(props, conf, id, t -> readAndVerify(t, ROWS_EXPECTED));
      int warmGets = gets() - g1;
      check(warm == ROWS, "warm read returned all " + ROWS + " rows, every payload verified");
      check(warmGets < coldGets,
          "A SECOND READER WITH COLD MEMORY PAYS LESS, BECAUSE THE TIER IS WARM ("
          + warmGets + " vs " + coldGets + " origin GETs)");
      note("cold " + coldGets + " GETs -> warm " + warmGets + " GETs, AAL's in-process "
          + "cache cold in both");

        // ---------------------------------------------------------------- 5
      // The control that makes the line above mean anything: empty the TIER and
      // the saving must disappear. If it does not, something else was serving.
      conn.sync().flushall();
      int g2 = gets();
      long again = withFreshCatalog(props, conf, id, t -> readAndVerify(t, ROWS_EXPECTED));
      int afterFlush = gets() - g2;
      check(again == ROWS, "correct again after flushing the tier");
      check(afterFlush > warmGets,
          "negative control: flushing the TIER brings the origin GETs back ("
          + afterFlush + " > " + warmGets + ") -- so the saving really was our cache");

    // Deliberately NOT closing table.io() here: the catalog is shared across
    // formats and its FileIO owns the S3 client, so closing it after the first
    // format left the second with a dead connection pool.
    conn.close(); rc2.shutdown();
  }

  /** Runs `body` against the table as a brand-new catalog would see it. */
  static <T> T withFreshCatalog(Map<String, String> props, Configuration conf,
                                TableIdentifier id, ThrowingFn<Table, T> body) throws Exception {
    HadoopCatalog c = new HadoopCatalog();
    c.setConf(conf);
    c.initialize("flint", props);
    try {
      return body.apply(c.loadTable(id));
    } finally {
      try { c.close(); } catch (Exception ignored) { }
    }
  }

  interface ThrowingFn<A, B> { B apply(A a) throws Exception; }

  /** Reads the whole table and verifies every row against its own identity. */
  static long readAndVerify(Table table, int expected) throws Exception {
    long n = 0;
    BitSet seen = new BitSet(expected);
    try (CloseableIterable<Record> it = IcebergGenerics.read(table).build()) {
      for (Record r : it) {
        long id = (Long) r.getField("id");
        String want = "row-" + id + "-" + Integer.toHexString(id * 2654435761L != 0
            ? (int) ((id * 2654435761L) & 0xFFFFFF) : 0);
        if (!want.equals(r.getField("payload"))) {
          throw new AssertionError("row " + id + " payload mismatch: " + r.getField("payload"));
        }
        if (seen.get((int) id)) throw new AssertionError("row " + id + " returned twice");
        seen.set((int) id);
        n++;
      }
    }
    return n;
  }
}
