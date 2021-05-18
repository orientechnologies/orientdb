package com.orientechnologies.orient.core.storage.index.learned;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.StackProfiler;
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
@Fork(1)
public class MyIndexBenchmark {
  public static void main(String[] args) throws RunnerException {
    final Options opts =
        new OptionsBuilder()
            .include("MyIndexBenchmark.*")
            .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
            .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
            // .result("target" + "/" + "results.csv")
            // .param("offHeapMessages", "true""
            // .resultFormat(ResultFormatType.CSV)
            .build();
    final Collection<RunResult> records = new Runner(opts).run();
    /*try {
        final Collection<RunResult> records = new Runner(opts).run();
        for (final RunResult result : records) {
            // final Result result = results.getValue().getPrimaryResult();
            System.out.println("Benchmark score: "
                    + result.getPrimaryResult().getScore() + " "
                    + result.getPrimaryResult().getScoreUnit() + " over "
                    + result.getPrimaryResult().getStatistics().getN() + " iterations");
        }
    } catch (final RunnerException e) {
        e.printStackTrace();
    }*/
  }

  private MyIndex pgmIndex;
  private List<Integer> data;

  @Setup(Level.Iteration)
  public void setup() {
    data = MyIndexTest.generateData(1_000, 42, new Random());
    Collections.sort(data);
    pgmIndex = new MyIndex<Integer>(MyIndexTest.EPSILON, data);
  }

  @Benchmark
  public void build() throws Exception {
    pgmIndex = new MyIndex<Integer>(MyIndexTest.EPSILON, data);
  }
}
