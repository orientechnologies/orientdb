package com.orientechnologies.orient.core.storage.index.nkbtree.normalizers;

import com.ibm.icu.text.Collator;
import com.orientechnologies.orient.core.index.OCompositeKey;

import com.orientechnologies.orient.core.metadata.schema.OType;
import org.junit.Assert;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 1, batchSize = 1)
@Warmup(iterations = 1, batchSize = 1)
@Fork(1)
public class KeyNormalizerBenchmark {
    KeyNormalizer keyNormalizer;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include("KeyNormalizerBenchmark.*")
                .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
                .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
                //.result("target" + "/" + "results.csv")
                //.param("offHeapMessages", "true""
                //.resultFormat(ResultFormatType.CSV)
                .build();
        new Runner(opt).run();
    }

    @Setup(Level.Iteration)
    public void setup() {
        keyNormalizer = new KeyNormalizer();
    }

    final ByteArrayOutputStream bos = new ByteArrayOutputStream();

    @Benchmark
    public void normalizeComposite_null() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(null);

        final OType[] types = new OType[1];
        types[0] = null;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_addKey_null() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(null);
    }

    @Benchmark
    public void normalizeComposite_null_int() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(null);
        compositeKey.addKey(5);

        final OType[] types = new OType[2];
        types[0] = null;
        types[1] = OType.INTEGER;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_int() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(5);

        final OType[] types = new OType[1];
        types[0] = OType.INTEGER;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_float() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(1.5f);

        final OType[] types = new OType[1];
        types[0] = OType.FLOAT;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_double() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(1.5d);

        final OType[] types = new OType[1];
        types[0] = OType.DOUBLE;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_Boolean() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(true);

        final OType[] types = new OType[1];
        types[0] = OType.BOOLEAN;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_long() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(5L);

        final OType[] types = new OType[1];
        types[0] = OType.LONG;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_byte() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey((byte) 3);

        final OType[] types = new OType[1];
        types[0] = OType.BYTE;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_short() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey((short) 3);

        final OType[] types = new OType[1];
        types[0] = OType.SHORT;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_decimal() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey(new BigDecimal(3.14159265359));

        final OType[] types = new OType[1];
        types[0] = OType.DECIMAL;

        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    /*@Benchmark
    public void normalizeComposite_char() throws Exception {
        compositeKey.addKey('a');
        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }*/

    @Benchmark
    public void normalizeComposite_string() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        compositeKey.addKey("abcd");

        final OType[] types = new OType[1];
        types[0] = OType.STRING;
        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }

    @Benchmark
    public void normalizeComposite_two_strings() throws Exception {
        final OCompositeKey compositeKey = new OCompositeKey();
        final String key = "abcd";
        compositeKey.addKey(key);
        final String secondKey = "test";
        compositeKey.addKey(secondKey);
        Assert.assertEquals(2, compositeKey.getKeys().size());

        final OType[] types = new OType[2];
        types[0] = OType.STRING;
        types[1] = OType.STRING;
        final byte[] bytes = keyNormalizer.normalize(compositeKey, types, Collator.NO_DECOMPOSITION);
    }
}
