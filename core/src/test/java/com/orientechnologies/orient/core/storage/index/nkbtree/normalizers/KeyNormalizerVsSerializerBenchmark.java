package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.ibm.icu.text.Collator;
import com.orientechnologies.common.serialization.types.*;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(1)
public class KeyNormalizerVsSerializerBenchmark {

  private KeyNormalizers keyNormalizers;
  private final byte[] binary = new byte[] {1, 2, 3, 4, 5, 6};
  private final Date date = new GregorianCalendar(2013, Calendar.NOVEMBER, 5).getTime();
  private Date dateTime;

  public static void main(String[] args) throws RunnerException {
    final Options opt =
        new OptionsBuilder()
            .include("KeyNormalizerVsSerializerBenchmark.*")
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
    keyNormalizers = new KeyNormalizers(Locale.ENGLISH, Collator.NO_DECOMPOSITION);

    final LocalDateTime ldt = LocalDateTime.of(2013, 11, 5, 3, 3, 3);
    dateTime = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
  }

  @Benchmark
  public byte[] booleanSerializer() {
    return OBooleanSerializer.INSTANCE.serializeNativeAsWhole(true);
  }

  @Benchmark
  public byte[] booleanNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey(true), new OType[] {OType.BOOLEAN});
  }

  @Benchmark
  public byte[] byteSerializer() {
    return OByteSerializer.INSTANCE.serializeNativeAsWhole((byte) 3);
  }

  @Benchmark
  public byte[] byteNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey((byte) 3), new OType[] {OType.BYTE});
  }

  @Benchmark
  public byte[] integerSerializer() {
    return OIntegerSerializer.INSTANCE.serializeNativeAsWhole(5);
  }

  @Benchmark
  public byte[] integerNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey(5), new OType[] {OType.INTEGER});
  }

  @Benchmark
  public byte[] floatSerializer() {
    return OFloatSerializer.INSTANCE.serializeNativeAsWhole(1.5f);
  }

  @Benchmark
  public byte[] floatNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey(1.5f), new OType[] {OType.FLOAT});
  }

  @Benchmark
  public byte[] doubleSerializer() {
    return ODoubleSerializer.INSTANCE.serializeNativeAsWhole(1.5d);
  }

  @Benchmark
  public byte[] doubleNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey(1.5d), new OType[] {OType.DOUBLE});
  }

  @Benchmark
  public byte[] shortSerializer() {
    return OShortSerializer.INSTANCE.serializeNativeAsWhole((short) 3);
  }

  @Benchmark
  public byte[] shortNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey((short) 3), new OType[] {OType.SHORT});
  }

  @Benchmark
  public byte[] longSerializer() {
    return OLongSerializer.INSTANCE.serializeNativeAsWhole(5L);
  }

  @Benchmark
  public byte[] longNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey(5L), new OType[] {OType.LONG});
  }

  @Benchmark
  public byte[] stringSerializer() {
    return OStringSerializer.INSTANCE.serializeNativeAsWhole("abcdefghokadnar");
  }

  @Benchmark
  public byte[] stringUtf8Serializer() {
    return OUTF8Serializer.INSTANCE.serializeNativeAsWhole("abcdefghokadnar");
  }

  @Benchmark
  public byte[] stringNormalizer() {
    return keyNormalizers.normalize(
        new OCompositeKey("abcdefghokadnar"), new OType[] {OType.STRING});
  }

  @Benchmark
  public byte[] binarySerializer() {
    return OBinaryTypeSerializer.INSTANCE.serializeNativeAsWhole(binary);
  }

  @SuppressWarnings("RedundantCast")
  @Benchmark
  public byte[] binaryNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey((Object) binary), new OType[] {OType.BINARY});
  }

  @Benchmark
  public byte[] dateSerializer() {
    return ODateSerializer.INSTANCE.serializeNativeAsWhole(date);
  }

  @Benchmark
  public byte[] dateNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey(date), new OType[] {OType.DATE});
  }

  @Benchmark
  public byte[] dateTimeSerializer() {
    return ODateTimeSerializer.INSTANCE.serializeNativeAsWhole(dateTime);
  }

  @Benchmark
  public byte[] dateTimeNormalizer() {
    return keyNormalizers.normalize(new OCompositeKey(date), new OType[] {OType.DATETIME});
  }
}
