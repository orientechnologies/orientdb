/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
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
public class JSONTestBenchmark extends DocumentDBBaseTest {
  // @Parameters(value = "url")
  /* public JSONTestBenchmark(@Optional final String iURL) {
    super(iURL);
  }*/

  public static void main(String[] args) throws RunnerException {
    final Options opt =
        new OptionsBuilder()
            .include("JSONTestBenchmark.*")
            .addProfiler(StackProfiler.class, "detailLine=true;excludePackages=true;period=1")
            .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
            // .result("target" + "/" + "results.csv")
            // .param("offHeapMessages", "true""
            // .resultFormat(ResultFormatType.CSV)
            .build();
    new Runner(opt).run();
  }

  @Benchmark
  public void testAlmostLink() {
    final ODocument doc = new ODocument();
    doc.fromJSON("{'title': '#330: Dollar Coins Are Done'}");
  }

  private final InputStream almostLinkStream =
      new ByteArrayInputStream(
          "{'title': '#330: Dollar Coins Are Done'}".getBytes(StandardCharsets.UTF_8));

  @Benchmark
  public void testAlmostLinkStream() throws IOException {
    final ODocument doc = new ODocument();
    doc.fromJSON(almostLinkStream);
  }

  @Benchmark
  public void testNullList() throws Exception {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [\"string\", null]}");

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());
  }

  @Benchmark
  public void testNullListStream() throws Exception {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream(
            "{\"list\" : [\"string\", null]}".getBytes(StandardCharsets.UTF_8)));

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());
  }
}
