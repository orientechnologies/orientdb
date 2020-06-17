/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 28/06/14. */
public class OLuceneInsertReadMultiThreadTest extends OLuceneBaseTest {

  private static final int THREADS = 10;
  private static final int RTHREADS = 10;
  private static final int CYCLE = 100;

  @Before
  public void init() {

    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");

    oClass.createProperty("name", OType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testConcurrentInsertWithIndex() throws Exception {

    List<CompletableFuture<Void>> futures =
        IntStream.range(0, THREADS)
            .boxed()
            .map(i -> CompletableFuture.runAsync(new LuceneInsert(pool, CYCLE)))
            .collect(Collectors.toList());

    futures.addAll(
        IntStream.range(0, 1)
            .boxed()
            .map(i -> CompletableFuture.runAsync(new LuceneReader(pool, CYCLE)))
            .collect(Collectors.toList()));

    futures.forEach(cf -> cf.join());

    ODatabaseDocument db1 = pool.acquire();
    db1.getMetadata().reload();
    OSchema schema = db1.getMetadata().getSchema();

    OIndex idx = schema.getClass("City").getClassIndex("City.name");

    Assert.assertEquals(idx.getInternal().size(), THREADS * CYCLE);
  }

  public class LuceneInsert implements Runnable {

    private final ODatabasePool pool;
    private final int cycle;
    private final int commitBuf;

    public LuceneInsert(ODatabasePool pool, int cycle) {
      this.pool = pool;
      this.cycle = cycle;

      this.commitBuf = cycle / 10;
    }

    @Override
    public void run() {

      final ODatabaseDocument db = pool.acquire();
      db.activateOnCurrentThread();
      db.declareIntent(new OIntentMassiveInsert());
      db.begin();
      int i = 0;
      for (; i < cycle; i++) {
        OElement doc = db.newElement("City");

        doc.setProperty("name", "Rome");

        db.save(doc);
        if (i % commitBuf == 0) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
      db.close();
    }
  }

  public class LuceneReader implements Runnable {
    private final int cycle;
    private final ODatabasePool pool;

    public LuceneReader(ODatabasePool pool, int cycle) {
      this.pool = pool;
      this.cycle = cycle;
    }

    @Override
    public void run() {

      final ODatabaseDocument db = pool.acquire();
      db.activateOnCurrentThread();
      OSchema schema = db.getMetadata().getSchema();
      OIndex idx = schema.getClass("City").getClassIndex("City.name");

      for (int i = 0; i < cycle; i++) {

        OResultSet resultSet =
            db.query("select from City where SEARCH_FIELDS(['name'], 'Rome') =true ");

        if (resultSet.hasNext()) {
          assertThat(resultSet.next().toElement().<String>getProperty("name"))
              .isEqualToIgnoringCase("rome");
        }
        resultSet.close();
      }
      db.close();
    }
  }
}
