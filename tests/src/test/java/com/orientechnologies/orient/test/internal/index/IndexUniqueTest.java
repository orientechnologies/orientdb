/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OConcurrentLegacyResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class IndexUniqueTest {
  private final AtomicInteger[] propValues = new AtomicInteger[10];
  private final Random random = new Random();
  private static final int ATTEMPTS = 100000;

  private final Phaser phaser =
      new Phaser() {
        @Override
        protected boolean onAdvance(int phase, int registeredParties) {
          for (AtomicInteger value : propValues) {
            value.set(random.nextInt());
          }

          return super.onAdvance(phase, registeredParties);
        }
      };

  public void indexUniqueTest() throws Exception {
    String[] indexNames = new String[10];
    Random random = new Random();

    char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    for (int i = 0; i < indexNames.length; i++) {
      String nm = "";
      for (int k = 0; k < 10; k++) nm += chars[random.nextInt(chars.length)];

      indexNames[i] = nm;
    }

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:./uniqueIndexTest");
    final int cores = Runtime.getRuntime().availableProcessors();

    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    for (int i = 0; i < propValues.length; i++) propValues[i] = new AtomicInteger();

    db.create();

    db.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, cores);

    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("indexTest");

    for (int i = 0; i < 10; i++) {
      oClass.createProperty("prop" + i, OType.INTEGER);
      oClass.createIndex(indexNames[i], OClass.INDEX_TYPE.UNIQUE, "prop" + i);
    }

    ExecutorService executor = Executors.newCachedThreadPool();
    List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

    for (int i = 0; i < cores; i++) {
      phaser.register();
      futures.add(executor.submit(new Populator("plocal:./uniqueIndexTest", random.nextBoolean())));
    }

    int sum = 0;
    for (Future<Integer> future : futures) sum += future.get();

    System.out.println("Total documents " + sum);

    Assert.assertEquals(db.countClass("indexTest"), sum);

    Set<Integer>[] props = new Set[10];
    for (int i = 0; i < props.length; i++) {
      props[i] = new HashSet<Integer>();
    }

    for (ODocument document : db.browseClass("indexTest")) {
      for (int i = 0; i < 10; i++) {
        Set<Integer> propValues = props[i];
        Assert.assertTrue(propValues.add(document.<Integer>field("prop" + i)));
      }
    }
  }

  public final class Populator implements Callable<Integer> {
    private final String url;
    private final boolean tx;

    public Populator(String url, boolean tx) {
      this.url = url;
      this.tx = tx;
    }

    @Override
    public Integer call() throws Exception {
      ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
      int i = 0;
      int success = 0;
      while (i < ATTEMPTS) {
        db.open("admin", "admin");
        try {
          i++;

          if (tx) db.begin();

          ODocument document = new ODocument("indexTest");

          for (int n = 0; n < 10; n++) document.field("prop" + n, propValues[n].get());

          document.save();

          if (tx) db.commit();

          success++;
        } catch (ORecordDuplicatedException e) {
          for (int n = 0; n < 10; n++) {
            OConcurrentLegacyResultSet result =
                db.command(
                        new OCommandSQL(
                            "select * from indexTest where prop"
                                + n
                                + " like "
                                + propValues[n].get()))
                    .execute();
            assert result.size() == 1;
          }
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        } finally {
          db.close();
        }

        phaser.arriveAndAwaitAdvance();
      }

      return success;
    }
  }
}
