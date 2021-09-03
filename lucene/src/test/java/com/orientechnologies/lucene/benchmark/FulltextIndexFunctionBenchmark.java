package com.orientechnologies.lucene.benchmark;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 3, batchSize = 1)
@Warmup(iterations = 3, batchSize = 1)
@Fork(3)
public class FulltextIndexFunctionBenchmark {
  public static void main(String[] args) throws RunnerException {
    final Options opt =
        new OptionsBuilder()
            .include("FulltextIndexFunctionBenchmark.*")
            // .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
            .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
            // .result("target" + "/" + "results.csv")
            // .param("offHeapMessages", "true""
            // .resultFormat(ResultFormatType.CSV)
            .build();
    new Runner(opt).run();
  }

  private ODatabaseDocumentInternal db;
  private OrientDB context;
  private ODatabaseType type;

  private String name = "lucene-benchmark";

  @Setup(Level.Iteration)
  public void setup() {
    this.setupDatabase();

    final InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");
    db.execute("sql", getScriptFromStream(stream));
    db.command("create index Song.title on Song (title) FULLTEXT ENGINE LUCENE ");
    db.command("create index Song.author on Song (author) FULLTEXT ENGINE LUCENE ");
    db.command(
        "create index Song.lyrics_description on Song (lyrics,description) FULLTEXT ENGINE LUCENE ");
  }

  private void setupDatabase() {
    final String config =
        System.getProperty("orientdb.test.env", ODatabaseType.MEMORY.name().toLowerCase());
    String path;
    if ("ci".equals(config) || "release".equals(config)) {
      type = ODatabaseType.PLOCAL;
      path = "embedded:./target/databases";
    } else {
      type = ODatabaseType.MEMORY;
      path = "embedded:.";
    }
    context = new OrientDB(path, OrientDBConfig.defaultConfig());

    if (context.exists(name)) {
      context.drop(name);
    }

    context.execute(
        "create database " + name + " plocal users ( admin identified by 'admin' role admin)");

    db = (ODatabaseDocumentInternal) context.open(name, "admin", "admin");
    db.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 8);
  }

  private String getScriptFromStream(final InputStream scriptStream) {
    try {
      return OIOUtils.readStreamAsString(scriptStream);
    } catch (final IOException e) {
      throw new RuntimeException("Could not read script stream.", e);
    }
  }

  @TearDown(Level.Iteration)
  public void tearDown() {
    db.activateOnCurrentThread();
    context.drop(name);
  }

  @Benchmark
  public void searchOnSingleField() {
    final OResultSet resultSet =
        db.query("SELECT from Song where SEARCH_FIELDS(['title'], 'BELIEVE') = true");
    resultSet.close();
  }

  @Benchmark
  public void searhOnTwoFieldsInOR() {
    final OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_FIELDS(['title'], 'BELIEVE') = true OR SEARCH_FIELDS(['author'], 'Bob') = true ");
    resultSet.close();
  }

  @Benchmark
  public void searhOnTwoFieldsInAND() throws Exception {
    final OResultSet resultSet =
        db.query(
            "SELECT from Song where SEARCH_FIELDS(['title'], 'tambourine') = true AND SEARCH_FIELDS(['author'], 'Bob') = true ");
    resultSet.close();
  }
}
