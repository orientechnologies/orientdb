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

import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 5, batchSize = 1)
@Warmup(iterations = 5, batchSize = 1)
@Fork(3)
public class JSONExportBenchmark extends DocumentDBBaseTest {
  public static void main(String[] args) throws RunnerException {
    final Options opt =
        new OptionsBuilder()
            .include("JSONExportBenchmark.*")
            .jvmArgs("-server", "-XX:+UseConcMarkSweepGC", "-Xmx4G", "-Xms1G")
            .build();
    new Runner(opt).run();
    return;
  }

  ODocument doc = null;

  @Setup(Level.Trial)
  public void setup() {
    doc = new ODocument();
    doc.field("nan", Double.NaN);
    doc.field("p_infinity", Double.POSITIVE_INFINITY);
    doc.field("n_infinity", Double.NEGATIVE_INFINITY);
    doc.field("long", 100000000000l);
    doc.field("date", new Date());
    doc.field("byte", (byte) 12);
  }

  @Benchmark
  public void testExportFormatOld() {
    doc.toJSON(ORecordAbstract.OLD_FORMAT_WITH_LATE_TYPES);
  }

  @Benchmark
  public void testExportFormatNew() {
    doc.toJSON();
  }
}
