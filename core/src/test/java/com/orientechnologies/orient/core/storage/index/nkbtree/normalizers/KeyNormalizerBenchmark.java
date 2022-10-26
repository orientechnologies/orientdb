package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.Collator;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(1)
public class KeyNormalizerBenchmark {
  private KeyNormalizer keyNormalizer;

  private OCompositeKey binaryCompositeKey;
  private OType[] binaryTypes;

  private OCompositeKey dateCompositeKey;
  private OType[] dateTypes;

  private OCompositeKey dateTimeCompositeKey;
  private OType[] dateTimeTypes;

  public static void main(String[] args) throws RunnerException {
    final Options opt =
        new OptionsBuilder()
            .include("KeyNormalizerBenchmark.*")
            .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
            .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
            // .result("target" + "/" + "results.csv")
            // .param("offHeapMessages", "true""
            // .resultFormat(ResultFormatType.CSV)
            .build();
    new Runner(opt).run();
  }

  @Setup(Level.Iteration)
  public void setup() {
    binaryFixture();
    dateFixture();
    dateTimeFixture();
  }

  private void binaryFixture() {
    keyNormalizer = new KeyNormalizer();
    final byte[] binaryKey = new byte[] {1, 2, 3, 4, 5, 6};
    binaryCompositeKey = new OCompositeKey();
    binaryCompositeKey.addKey(binaryKey);
    binaryTypes = new OType[1];
    binaryTypes[0] = OType.BINARY;
  }

  private void dateFixture() {
    final Date key = new GregorianCalendar(2013, Calendar.NOVEMBER, 5).getTime();
    dateCompositeKey = new OCompositeKey();
    dateCompositeKey.addKey(key);
    dateTypes = new OType[1];
    dateTypes[0] = OType.DATE;
  }

  private void dateTimeFixture() {
    final LocalDateTime ldt = LocalDateTime.of(2013, 11, 5, 3, 3, 3);
    final Date key = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    dateTimeCompositeKey = new OCompositeKey();
    dateTimeCompositeKey.addKey(key);
    dateTimeTypes = new OType[1];
    dateTimeTypes[0] = OType.DATETIME;
  }

  // final ByteArrayOutputStream bos = new ByteArrayOutputStream();

  @Benchmark
  public void normalizeCompositeNull() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);

    final OType[] types = new OType[1];
    types[0] = null;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeAddKeyNull() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);
  }

  @Benchmark
  public void normalizeCompositeNullInt() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(null);
    compositeKey.addKey(5);

    final OType[] types = new OType[2];
    types[0] = null;
    types[1] = OType.INTEGER;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeInt() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(5);

    final OType[] types = new OType[1];
    types[0] = OType.INTEGER;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeFloat() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(1.5f);

    final OType[] types = new OType[1];
    types[0] = OType.FLOAT;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDouble() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(1.5d);

    final OType[] types = new OType[1];
    types[0] = OType.DOUBLE;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeBoolean() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(true);

    final OType[] types = new OType[1];
    types[0] = OType.BOOLEAN;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeLong() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(5L);

    final OType[] types = new OType[1];
    types[0] = OType.LONG;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeByte() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey((byte) 3);

    final OType[] types = new OType[1];
    types[0] = OType.BYTE;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeShort() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey((short) 3);

    final OType[] types = new OType[1];
    types[0] = OType.SHORT;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDecimal() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(new BigDecimal(3.14159265359));

    final OType[] types = new OType[1];
    types[0] = OType.DECIMAL;

    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeString() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey("abcd");

    final OType[] types = new OType[1];
    types[0] = OType.STRING;
    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeTwoStrings() {
    final OCompositeKey compositeKey = new OCompositeKey();
    final String key = "abcd";
    compositeKey.addKey(key);
    final String secondKey = "test";
    compositeKey.addKey(secondKey);

    final OType[] types = new OType[2];
    types[0] = OType.STRING;
    types[1] = OType.STRING;
    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDate() {
    keyNormalizer.normalize(dateTimeCompositeKey, dateTimeTypes, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeDateTime() {
    keyNormalizer.normalize(dateCompositeKey, dateTypes, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void normalizeCompositeBinary() {
    keyNormalizer.normalize(binaryCompositeKey, binaryTypes, Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void decimalNormalizer() {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(new BigDecimal(new BigInteger("20"), 2));

    final OType[] types = new OType[1];
    types[0] = OType.DECIMAL;
    keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }
}
