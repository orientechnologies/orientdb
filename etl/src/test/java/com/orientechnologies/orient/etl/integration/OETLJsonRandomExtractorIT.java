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

package com.orientechnologies.orient.etl.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.etl.OETLBaseTest;
import com.orientechnologies.orient.etl.context.OETLContext;
import java.io.File;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests ETL JSON Extractor.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OETLJsonRandomExtractorIT extends OETLBaseTest {

  private static final int TOTAL = 1000000;

  @Before
  public void cleanFolder() throws Exception {

    OFileUtils.deleteRecursively(new File("./target/" + name.getMethodName()));
  }

  @Test
  public void shouldLoadSingleThread() {
    OETLContext context = new OETLContext();
    context.setVariable("parallel", Boolean.FALSE).setVariable("dumpEveryMs", 1000);
    configure(
        "{extractor : { random: {items: "
            + TOTAL
            + ", fields: 10} }, "
            + "loader: { orientdb: {batchCommit: 10000 , dbURL: 'plocal:./target/"
            + name.getMethodName()
            + "', dbType:'graph', class: 'Person', useLightweightEdges:false, "
            + "classes: [{name: 'Person', extends: 'V'}] } } }",
        context);

    proc.execute();

    ODatabasePool pool = proc.getLoader().getPool();

    ODatabaseDocument db = pool.acquire();
    assertThat(db.countClass("Person")).isEqualTo(TOTAL);

    db.browseClass("Person").forEach(doc -> assertThat(doc.fields()).isEqualTo(10));
    db.close();
  }

  @Test
  public void shouldLoadMultipleThreadsInParallel() {
    OETLContext context = new OETLContext();
    context.setVariable("parallel", Boolean.TRUE).setVariable("dumpEveryMs", 1000);
    configure(
        "{extractor : { random: {items: "
            + TOTAL
            + ", fields: 10, delay: 0} }, "
            + "loader: { orientdb: { dbURL: 'plocal:./target/"
            + name.getMethodName()
            + "', dbType:'graph', class: 'Person', useLightweightEdges:false, "
            + "classes: [{name: 'Person', extends: 'V', clusters: 8  }] } } }",
        context);

    proc.execute();
    ODatabasePool pool = proc.getLoader().getPool();
    ODatabaseDocument db = pool.acquire();

    assertThat(db.countClass("Person")).isEqualTo(TOTAL);

    db.browseClass("Person").forEach(doc -> assertThat(doc.fields()).isEqualTo(10));
    db.close();
  }

  @Test
  public void shouldLoadMultipleThreadsInParallelWithBatchCommit() {
    OETLContext context = new OETLContext();
    context.setVariable("parallel", Boolean.TRUE).setVariable("dumpEveryMs", 1000);
    configure(
        "{extractor : { random: {items: "
            + TOTAL
            + ", fields: 10, delay: 0} }, "
            + "loader: { orientdb: {batchCommit: 10000 ,dbURL: 'plocal:./target/"
            + name.getMethodName()
            + "', dbType:'graph', class: 'Person', useLightweightEdges:false, "
            + "classes: [{name: 'Person', extends: 'V', clusters: 8 }] } } }",
        context);

    proc.execute();
    ODatabasePool pool = proc.getLoader().getPool();
    ODatabaseDocument db = pool.acquire();

    assertThat(db.countClass("Person")).isEqualTo(TOTAL);

    db.browseClass("Person").forEach(doc -> assertThat(doc.fields()).isEqualTo(10));
    db.close();
  }
}
