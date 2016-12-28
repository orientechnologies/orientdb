/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests ETL JSON Extractor.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OETLJsonRandomExtractorTest extends OETLBaseTest {

  private final static int TOTAL = 1000000;

  @Test
  public void shouldLoadSingleThread() {

    process("{extractor : { random: {items: " + TOTAL + ", fields: 10} }, "
            + "loader: { orientdb: {batchCommit: 10000 , dbURL: 'memory:" + name.getMethodName()
            + "', dbType:'graph', class: 'Person', useLightweightEdges:false, " + "classes: [{name: 'Person', extends: 'V'}] } } }",
        new OBasicCommandContext().setVariable("parallel", Boolean.FALSE).setVariable("dumpEveryMs", 1000));

    assertThat(graph.countVertices("Person")).isEqualTo(TOTAL);

    graph.getRawGraph().browseClass("Person").forEach(doc -> assertThat(doc.fields()).isEqualTo(10));
  }

  @Test
  public void shouldLoadMultipleThreadsInParallel() {

    process(
        "{extractor : { random: {items: " + TOTAL + ", fields: 10, delay: 0} }, " + "loader: { orientdb: { dbURL: 'memory:" + name
            .getMethodName() + "', dbType:'graph', class: 'Person', useLightweightEdges:false, "
            + "classes: [{name: 'Person', extends: 'V', clusters: 8  }] } } }",
        new OBasicCommandContext().setVariable("parallel", Boolean.TRUE).setVariable("dumpEveryMs", 1000));

    assertThat(graph.countVertices("Person")).isEqualTo(TOTAL);

    graph.getRawGraph().browseClass("Person").forEach(doc -> assertThat(doc.fields()).isEqualTo(10));
  }

  @Test
  public void shouldLoadMultipleThreadsInParallelWithBatchCommit() {

    process("{extractor : { random: {items: " + TOTAL + ", fields: 10, delay: 0} }, "
            + "loader: { orientdb: {batchCommit: 10000 ,dbURL: 'memory:" + name.getMethodName()
            + "', dbType:'graph', class: 'Person', useLightweightEdges:false, "
            + "classes: [{name: 'Person', extends: 'V', clusters: 8 }] } } }",
        new OBasicCommandContext().setVariable("parallel", Boolean.TRUE).setVariable("dumpEveryMs", 1000));

    assertThat(graph.countVertices("Person")).isEqualTo(TOTAL);

    graph.getRawGraph().browseClass("Person").forEach(doc -> assertThat(doc.fields()).isEqualTo(10));
  }

}
