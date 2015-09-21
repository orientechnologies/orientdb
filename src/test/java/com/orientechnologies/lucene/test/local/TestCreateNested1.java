/*
 *
 *  * Copyright 2014 Orient Technologies.
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

package com.orientechnologies.lucene.test.local;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

public class TestCreateNested1 {

  @Test
  public void testLocal() {

    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/VehicleHistoryGraph");
          db.open("admin", "admin");
          while (true) {
            String name = UUID.randomUUID().toString();
            db.command(new OSQLSynchQuery<String>("select from Person where name = '" + name + "'")).execute();
          }
        }
      });
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testLocal2() {

    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost:2425/VehicleHistoryGraph");
          db.open("admin", "admin");
          while (true) {
            String name = UUID.randomUUID().toString();
            db.command(new OSQLSynchQuery<String>("select from Person where name = '" + name + "'")).execute();
          }
        }
      });
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testIndex() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:tmp");

    db.create();

    OClass test = db.getMetadata().getSchema().createClass("Test");

    test.createProperty("name", OType.STRING);
    test.createProperty("age", OType.INTEGER);

    test.createIndex("Test.name_age", OClass.INDEX_TYPE.NOTUNIQUE, "name", "age");

    ODocument doc = new ODocument("Test");

    doc.field("name", "Enrico");
    doc.field("age", 32);
    db.save(doc);

    OIndex<?> index = db.getMetadata().getIndexManager().getIndex("Test.name_age");

    Collection<OIdentifiable> results = (Collection<OIdentifiable>) index.get(new OCompositeKey(Arrays.asList("Enrico", 32)));

    Assert.assertEquals(results.size(), 1);

    results = (Collection<OIdentifiable>) index.get(new OCompositeKey(Arrays.asList("Enrico", 31)));
    Assert.assertEquals(results.size(), 0);
  }
}