"""Does the accelerator work in a SEPARATE EXECUTOR JVM?

Every Spark number this project has was taken with ``master=local[*]``, where
the DRIVER IS THE EXECUTOR. A real cluster ships work to separate executor
processes which build their own FileSystem from the shipped Hadoop config, and
that path had never been exercised. It is the kind of thing that works or fails
completely, and a customer's cluster is the wrong place to find out.

``local-cluster`` spawns genuine executor processes on one machine, so the
question is answerable locally and for nothing.

RESULT, 2026-08-28: **it works.** Two executor JVMs, PIDs distinct from the
driver, read through ``s3a://`` and populated the shared tier — 128 chunk keys
written by a process that was not the driver.

RUN IT BY HAND. There is deliberately no wrapper script: a wrapper was written
and deleted, because ``spark.executor.extraClassPath`` is a SUBMIT-time setting
and configuring it from the builder is unreliable — the same trap as
``spark.jars.packages`` below. A script that fails for environment reasons is
worse than a recipe that says what it needs.

    JH=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
    export JAVA_HOME=$JH PATH=$JH/bin:$PATH        # (1)
    python3 -m venv /tmp/sv && /tmp/sv/bin/pip install pyspark==4.0.4
    export SPARK_HOME=$(/tmp/sv/bin/python -c \
        "import pyspark,os;print(os.path.dirname(pyspark.__file__))")   # (2)
    cd jvm-spike && mvn dependency:build-classpath \
        -Dmdep.outputFile=/tmp/prov.txt -DincludeScope=provided && cd ..
    tr ':' '\n' < /tmp/prov.txt | grep -vE 'jackson|scala|slf4j|log4j|reload4j' \
        | paste -sd: - > /tmp/prov2.txt                                  # (6)
    printf '%s:%s' jvm-spike/target/*hadoop-shim.jar "$(cat /tmp/prov2.txt)" \
        > /tmp/cp.txt                                                    # (7)
    valkey-server --port 9399 --save '' &
    python3 tools/counting_s3.py --port 9301 --objects 4 --object-bytes 8388608 &
    PROV_CP=/tmp/cp.txt /tmp/sv/bin/python tools/executor_jvm_check.py \
        redis://127.0.0.1:9399 http://127.0.0.1:9301 \
        jvm-spike/target/flint-accel-seam-spike-*.jar

EVERY FAILURE ON THE WAY WAS ENVIRONMENT, NOT PRODUCT, and the next person hits
the same ones in the same order:

  1. Spark 4.0.4 does not run on Java 26. It needs 17 or 21.
  2. ``local-cluster`` needs SPARK_HOME to launch executors at all.
  3. pyspark bundles hadoop-client-api but NOT hadoop-aws, so ``s3a://`` has no
     implementation.
  4. ``spark.jars.packages`` set on the BUILDER is too late — ivy resolution
     happens at submit time, so it must go in PYSPARK_SUBMIT_ARGS.
  5. ``local-cluster`` does not push ``--jars`` onto the executor classpath the
     way a real cluster does.
  6. Hadoop's jackson-databind 2.12 shadows Spark's 2.18 and the Scala module
     refuses to load.
  7. And a seventh that is NOT environment and IS documented: hadoop-aws 3.4.3
     wants an AAL class no published AAL ships, which is what the shim jar is
     for (README, "Known limits"). Reproduced here by accident, because this
     borrows 3.4.3 from Maven rather than the 3.4.1 pyspark bundles — an
     independent confirmation that the shim story is real.
"""
import sys, json, urllib.request
from pyspark.sql import SparkSession

TIER, EP, JAR = sys.argv[1], sys.argv[2], sys.argv[3]

import pyspark, glob, os, re
_api = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars",
                              "hadoop-client-api-*.jar"))
HADOOP_VER = re.search(r"hadoop-client-api-([0-9.]+)\.jar", _api[0]).group(1)
print("pinning hadoop-aws to the bundled", HADOOP_VER, flush=True)
EXTRA_CP = JAR + ":" + open(os.environ["PROV_CP"]).read().strip()

def stats():
    with urllib.request.urlopen(EP + "/__stats") as r:
        return json.load(r)

def tier_keys():
    import subprocess
    out = subprocess.run(["valkey-cli", "-p", os.environ.get("TIER_CLI_PORT", "9399"), "--scan", "--pattern", "c2/*"],
                         capture_output=True, text=True).stdout
    return len([l for l in out.splitlines() if l.strip()])

spark = (SparkSession.builder
    .appName("flint-executor-test")
    # TWO real executor JVMs, one core each, 1 GiB -- separate processes from
    # the driver, which is the whole point.
    .master("local-cluster[2,1,2048]")
    .config("spark.jars", JAR)
    # pyspark bundles hadoop-client-api but NOT hadoop-aws, so s3a:// has no
    # implementation until this resolves. The EC2 harness does the same thing;
    # this is environment parity, not part of what is under test.
    .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{HADOOP_VER}")
    # local-cluster does not push --jars onto the executor classpath the way a
    # real cluster does, so name it explicitly. Same machine, so absolute paths
    # are valid on both sides. This is a quirk of the local harness, not of the
    # thing under test.
    .config("spark.executor.extraClassPath", EXTRA_CP)
    .config("spark.driver.extraClassPath", EXTRA_CP)
    .config("spark.hadoop.fs.s3a.endpoint", EP)
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.access.key", "x")
    .config("spark.hadoop.fs.s3a.secret.key", "x")
    .config("spark.hadoop.fs.s3a.bucket.probe", "0")
    .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
    .config("spark.hadoop.fs.s3a.impl", "ai.crestway.flintaccel.s3a.FlintS3AFileSystem")
    .config("spark.hadoop.fs.s3a.flint.tier.uri", TIER)
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

ok = True
def check(c, label):
    global ok
    ok &= bool(c)
    print(f"[{'ok' if c else 'FAIL'}] {label}", flush=True)

# Read from the EXECUTORS: binaryFiles forces the read to happen in the task,
# not on the driver. A driver-side read would prove nothing here.
before = tier_keys()
rdd = spark.sparkContext.binaryFiles("s3a://bucket/data/000001.bin")
n = rdd.map(lambda kv: len(kv[1])).sum()
after = tier_keys()

check(n > 0, f"executors read the object through s3a:// ({n} bytes)")
check(after > before,
      f"and the EXECUTOR populated the shared tier ({before} -> {after} chunk keys)")
# Armed: prove the executors are genuinely separate processes.
pids = spark.sparkContext.parallelize(range(8), 8).map(
    lambda _: __import__("os").getpid()).distinct().collect()
import os
check(len(pids) >= 1 and os.getpid() not in pids,
      f"armed: the work ran in OTHER processes (driver {os.getpid()}, executors {sorted(pids)})")
spark.stop()
sys.exit(0 if ok else 1)
