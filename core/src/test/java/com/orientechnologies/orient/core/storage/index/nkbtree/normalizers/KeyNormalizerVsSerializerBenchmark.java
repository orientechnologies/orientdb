package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import static com.orientechnologies.common.serialization.types.OLongSerializer.LONG_SIZE;

import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.ODateSerializer;
import com.orientechnologies.common.serialization.types.ODateTimeSerializer;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.ODoubleSerializer;
import com.orientechnologies.common.serialization.types.OFloatSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.orient.core.storage.index.nkbtree.normalizers.benchmark.Plotter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.text.Collator;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.style.Styler;
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
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(0)
public class KeyNormalizerVsSerializerBenchmark {
  private KeyNormalizer keyNormalizer;
  private ByteOrder byteOrder;

  public static void main(String[] args) throws RunnerException, IOException {
    final Options opt =
        new OptionsBuilder()
            .include("KeyNormalizerVsSerializerBenchmark.*")
            .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
            .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
            // .result("target" + "/" + "results.csv")
            // .param("offHeapMessages", "true""
            // .resultFormat(ResultFormatType.CSV)
            .build();
    new KeyNormalizerVsSerializerBenchmark().postProcessRunResult(new Runner(opt).run());
  }

  private class Pair {
    private RunResult serializer;
    private RunResult normalizer;

    public Pair() {}

    public RunResult getSerializer() {
      return serializer;
    }

    public void setSerializer(RunResult serializer) {
      this.serializer = serializer;
    }

    public RunResult getNormalizer() {
      return normalizer;
    }

    public void setNormalizer(RunResult normalizer) {
      this.normalizer = normalizer;
    }
  }

  private void postProcessRunResult(final Collection<RunResult> results) throws IOException {
    final Map<String, Pair> resultMap = buildResultMap(results);

    final Plotter plotter = new Plotter();
    final XYChart chart =
        plotter.getXYChart(
            "Serializer vs. Normalizer",
            "Test",
            "Average time (us)",
            Styler.LegendPosition.InsideNE);
    final List<Integer> xData = new ArrayList<>();
    final List<Double> yData = new ArrayList<>();

    final List<Integer> xDataNormalizer = new ArrayList<>();
    final List<Double> yDataNormalizer = new ArrayList<>();

    int counter = 0;
    for (final Map.Entry<String, Pair> pair : resultMap.entrySet()) {
      xData.add(counter);
      if (pair.getValue().getSerializer() != null) {
        yData.add(pair.getValue().getSerializer().getPrimaryResult().getScore());
      } else {
        yData.add(0.0);
      }

      xDataNormalizer.add(counter);
      if (pair.getValue().getNormalizer() != null) {
        yDataNormalizer.add(pair.getValue().getNormalizer().getPrimaryResult().getScore());
      } else {
        yDataNormalizer.add(0.0);
      }
      counter++;
    }
    plotter.addSeriesToLineChart(chart, "Serializer", xData, yData);
    plotter.addSeriesToLineChart(chart, "Normalizer", xDataNormalizer, yDataNormalizer);

    plotter.exportChartAsPDF(chart, "core/target/normalizerVsSerializer");
  }

  private Map<String, Pair> buildResultMap(Collection<RunResult> results) {
    final Map<String, Pair> map = new HashMap<>();
    for (final RunResult rr : results) {
      final Result pr = rr.getPrimaryResult();
      final String key = pr.getLabel().replaceAll("Normalizer", "").replaceAll("Serializer", "");

      Pair pair = new Pair();
      if (map.containsKey(key)) {
        pair = map.get(key);
      }

      if (pr.getLabel().contains("Normalizer")) {
        pair.setNormalizer(rr);
      } else {
        pair.setSerializer(rr);
      }
      map.put(key, pair);
    }
    return map;
  }

  @Setup(Level.Iteration)
  public void setup() {
    keyNormalizer = new KeyNormalizer();
    byteOrder = ByteOrder.nativeOrder();
  }

  @Benchmark
  public void booleanSerializer() {
    final OBooleanSerializer serializer = new OBooleanSerializer();
    serializer.serialize(true, new byte[1], 0);
  }

  @Benchmark
  public void booleanNormalizer() throws Exception {
    final BooleanKeyNormalizer normalizer = new BooleanKeyNormalizer();
    normalizer.execute(true, 0);
  }

  @Benchmark
  public void byteSerializer() {
    final OByteSerializer serializer = new OByteSerializer();
    serializer.serialize((byte) 3, new byte[1], 0);
  }

  @Benchmark
  public void byteNormalizer() throws Exception {
    final ByteKeyNormalizer normalizer = new ByteKeyNormalizer();
    normalizer.execute((byte) 3, 0);
  }

  @Benchmark
  public void integerSerializer() {
    final OIntegerSerializer serializer = new OIntegerSerializer();
    serializer.serialize(5, new byte[4], 0);
  }

  @Benchmark
  public void integerNormalizer() throws Exception {
    final IntegerKeyNormalizer normalizer = new IntegerKeyNormalizer();
    normalizer.execute(5, 0);
  }

  @Benchmark
  public void floatSerializer() {
    final OFloatSerializer serializer = new OFloatSerializer();
    serializer.serialize(1.5f, new byte[4], 0);
  }

  @Benchmark
  public void floatNormalizer() throws Exception {
    final FloatKeyNormalizer normalizer = new FloatKeyNormalizer();
    normalizer.execute(1.5f, 0);
  }

  @Benchmark
  public void doubleSerializer() {
    final ODoubleSerializer serializer = new ODoubleSerializer();
    serializer.serialize(1.5d, new byte[8], 0);
  }

  @Benchmark
  public void doubleNormalizer() throws Exception {
    final DoubleKeyNormalizer normalizer = new DoubleKeyNormalizer();
    normalizer.execute(1.5d, 0);
  }

  @Benchmark
  public void shortSerializer() {
    final OShortSerializer serializer = new OShortSerializer();
    serializer.serialize((short) 3, new byte[2], 0);
  }

  @Benchmark
  public void shortNormalizer() throws Exception {
    final ShortKeyNormalizer normalizer = new ShortKeyNormalizer();
    normalizer.execute((short) 3, 0);
  }

  @Benchmark
  public void longSerializer() {
    final OLongSerializer serializer = new OLongSerializer();
    serializer.serialize(5L, new byte[LONG_SIZE], 0);
  }

  @Benchmark
  public void longNormalizer() throws Exception {
    final LongKeyNormalizer normalizer = new LongKeyNormalizer();
    normalizer.execute(5L, 0);
  }

  @Benchmark
  public void stringSerializer() {
    final OStringSerializer serializer = new OStringSerializer();
    serializer.serialize("abcd", new byte[16], 0);
  }

  @Benchmark
  public void stringUtf8Serializer() {
    final OUTF8Serializer serializer = new OUTF8Serializer();
    serializer.serialize("abcd", new byte[16], 0);
  }

  @Benchmark
  public void stringNormalizer() throws Exception {
    final StringKeyNormalizer normalizer = new StringKeyNormalizer();
    normalizer.execute("abcd", Collator.NO_DECOMPOSITION);
  }

  @Benchmark
  public void binarySerializer() {
    final OBinaryTypeSerializer serializer = new OBinaryTypeSerializer();
    final byte[] binary = new byte[] {1, 2, 3, 4, 5, 6};
    serializer.serialize(binary, new byte[binary.length + OIntegerSerializer.INT_SIZE], 0);
  }

  @Benchmark
  public void binaryNormalizer() throws Exception {
    final BinaryKeyNormalizer normalizer = new BinaryKeyNormalizer();
    final byte[] binary = new byte[] {1, 2, 3, 4, 5, 6};
    normalizer.execute(binary, 0);
  }

  @Benchmark
  public void dateSerializer() {
    final ODateSerializer serializer = new ODateSerializer();
    final Date date = new GregorianCalendar(2013, Calendar.NOVEMBER, 5).getTime();
    serializer.serialize(date, new byte[LONG_SIZE], 0);
  }

  @Benchmark
  public void dateNormalizer() throws Exception {
    final DateKeyNormalizer normalizer = new DateKeyNormalizer();
    final Date date = new GregorianCalendar(2013, Calendar.NOVEMBER, 5).getTime();
    normalizer.execute(date, 0);
  }

  @Benchmark
  public void dateTimeSerializer() {
    final ODateTimeSerializer serializer = new ODateTimeSerializer();
    final LocalDateTime ldt = LocalDateTime.of(2013, 11, 5, 3, 3, 3);
    final Date date = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    serializer.serialize(date, new byte[LONG_SIZE], 0);
  }

  @Benchmark
  public void dateTimeNormalizer() throws Exception {
    final DateKeyNormalizer normalizer = new DateKeyNormalizer();
    final LocalDateTime ldt = LocalDateTime.of(2013, 11, 5, 3, 3, 3);
    final Date date = Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    normalizer.execute(date, 0);
  }

  @Benchmark
  public void decimalSerializer() {
    final ODecimalSerializer serializer = new ODecimalSerializer();
    serializer.serialize(new BigDecimal(new BigInteger("20"), 2), new byte[9], 0);
  }

  @Benchmark
  public void decimalNormalizer() throws Exception {
    final DecimalKeyNormalizer normalizer = new DecimalKeyNormalizer();
    normalizer.execute(new BigDecimal(new BigInteger("20"), 2), 0);
  }
}
