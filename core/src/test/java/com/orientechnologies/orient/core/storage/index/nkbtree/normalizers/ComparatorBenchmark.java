package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.comparator.OByteArrayComparator;
import com.orientechnologies.common.comparator.OUnsafeByteArrayComparator;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Assert;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(1)
public class ComparatorBenchmark {
  KeyNormalizers keyNormalizer;

  public static void main(String[] args) throws RunnerException {
    final Options opt =
            new OptionsBuilder()
                    .include("ComparatorBenchmark.*")
                    .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
                    .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
                    // .result("target" + "/" + "results.csv")
                    // .param("offHeapMessages", "true""
                    // .resultFormat(ResultFormatType.CSV)
                    .build();
    new Runner(opt).run();
  }

  final OByteArrayComparator arrayComparator = new OByteArrayComparator();
  final OUnsafeByteArrayComparator byteArrayComparator = new OUnsafeByteArrayComparator();

  byte[] negative;
  byte[] zero;
  byte[] positive;

  @Setup(Level.Iteration)
  public void setup() {
    keyNormalizer = new KeyNormalizers(Locale.getDefault(), Collator.NO_DECOMPOSITION);

    try {
      negative = getNormalizedKeySingle(-62);
      zero = getNormalizedKeySingle(0);
      positive = getNormalizedKeySingle(5);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Benchmark
  public void comparatorByteArrayNegative() {
    byteArrayComparator.compare(negative, zero);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Benchmark
  public void comparatorByteArrayPositive() {
    byteArrayComparator.compare(positive, zero);
  }

  @SuppressWarnings({"EqualsWithItself", "ResultOfMethodCallIgnored"})
  @Benchmark
  public void comparatorByteArrayEqual() {
    byteArrayComparator.compare(zero, zero);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Benchmark
  public void comparatorUnsafeByteArrayNegative() {
    arrayComparator.compare(negative, zero);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Benchmark
  public void comparatorUnsafeByteArrayPositive() {
    arrayComparator.compare(positive, zero);
  }

  @SuppressWarnings({"EqualsWithItself", "ResultOfMethodCallIgnored"})
  @Benchmark
  public void comparatorUnsafeByteArrayEqual() {
    arrayComparator.compare(zero, zero);
  }

  private byte[] getNormalizedKeySingle(final int keyValue) throws IOException {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(keyValue);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = OType.INTEGER;

    return keyNormalizer.normalize(compositeKey, types);
  }
}