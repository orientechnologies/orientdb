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

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(1)
public class ComparatorBenchmark {
  KeyNormalizer keyNormalizer;

  public static void main(String[] args) throws RunnerException {
    final Options opt = new OptionsBuilder().include("ComparatorBenchmark.*")
        .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
        .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
        // .result("target" + "/" + "results.csv")
        // .param("offHeapMessages", "true""
        // .resultFormat(ResultFormatType.CSV)
        .build();
    new Runner(opt).run();
  }

  final OByteArrayComparator       arrayComparator     = new OByteArrayComparator();
  final OUnsafeByteArrayComparator byteArrayComparator = new OUnsafeByteArrayComparator();

  byte[]                           negative;
  byte[]                           zero;
  byte[]                           positive;

  @Setup(Level.Iteration)
  public void setup() {
    keyNormalizer = new KeyNormalizer();

    negative = getNormalizedKeySingle(-62, OType.INTEGER);
    zero = getNormalizedKeySingle(0, OType.INTEGER);
    positive = getNormalizedKeySingle(5, OType.INTEGER);
  }

  @Benchmark
  public void comparator_byteArrayNegative() throws Exception {
    byteArrayComparator.compare(negative, zero);
  }

  @Benchmark
  public void comparator_byteArrayPositive() throws Exception {
    byteArrayComparator.compare(positive, zero);
  }

  @Benchmark
  public void comparator_byteArrayEqual() throws Exception {
    byteArrayComparator.compare(zero, zero);
  }

  @Benchmark
  public void comparator_unsafeByteArrayNegative() throws Exception {
    arrayComparator.compare(negative, zero);
  }

  @Benchmark
  public void comparator_unsafeByteArrayPositive() throws Exception {
    arrayComparator.compare(positive, zero);
  }

  @Benchmark
  public void comparator_unsafeByteArrayEqual() throws Exception {
    arrayComparator.compare(zero, zero);
  }

  private byte[] getNormalizedKeySingle(final int keyValue, final OType type) {
    final OCompositeKey compositeKey = new OCompositeKey();
    compositeKey.addKey(keyValue);
    Assert.assertEquals(1, compositeKey.getKeys().size());

    final OType[] types = new OType[1];
    types[0] = type;

    return keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
  }
}
