/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.test.ConcurrentTestHelper;
import com.orientechnologies.orient.test.TestFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ConcurrentSchemaTest extends DocumentDBBaseTest {
  private static final int THREADS = 10;
  private static final int CYCLES = 50;

  private final AtomicLong createClassThreadCounter = new AtomicLong();
  private final AtomicLong dropClassThreadCounter = new AtomicLong();
  private final AtomicLong counter = new AtomicLong();

  class CreateClassCommandExecutor implements Callable<Void> {
    long id;
    String url;

    public CreateClassCommandExecutor(String url) {
      this.url = url;
    }

    @Override
    public Void call() {
      this.id = createClassThreadCounter.getAndIncrement();
      for (int i = 0; i < CYCLES; i++) {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).open("admin", "admin");
        try {
          final String clsName = "ConcurrentClassTest-" + id + "-" + i;

          OClass cls = database.getMetadata().getSchema().createClass(clsName);

          Assert.assertEquals(cls.getName(), clsName);
          Assert.assertTrue(database.getMetadata().getSchema().existsClass(clsName));

          db.command(new OCommandSQL("select from " + clsName)).execute();

          counter.incrementAndGet();
        } finally {
          db.close();
        }
      }
      return null;
    }
  }

  class DropClassCommandExecutor implements Callable<Void> {
    long id;
    String url;

    public DropClassCommandExecutor(String url) {
      this.url = url;
    }

    @Override
    public Void call() {
      this.id = dropClassThreadCounter.getAndIncrement();
      for (int i = 0; i < CYCLES; i++) {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(url).open("admin", "admin");
        try {
          final String clsName = "ConcurrentClassTest-" + id + "-" + i;

          Assert.assertTrue(database.getMetadata().getSchema().existsClass(clsName));
          database.getMetadata().getSchema().dropClass(clsName);
          Assert.assertFalse(database.getMetadata().getSchema().existsClass(clsName));

          counter.decrementAndGet();
        } finally {
          db.close();
        }
      }
      return null;
    }
  }

  @Parameters(value = "url")
  public ConcurrentSchemaTest(@Optional String url) {
    super(url);
  }

  @Test
  public void concurrentCommands() throws Exception {
    //    System.out.println("Create classes, spanning " + THREADS + " threads...");

    ConcurrentTestHelper.test(
        THREADS,
        new TestFactory<Void>() {
          @Override
          public Callable<Void> createWorker() {
            return new CreateClassCommandExecutor(url);
          }
        });

    //    System.out.println("Create classes, checking...");

    for (int id = 0; id < THREADS; ++id) {
      for (int i = 0; i < CYCLES; ++i) {
        final String clsName = "ConcurrentClassTest-" + id + "-" + i;
        Assert.assertTrue(database.getMetadata().getSchema().existsClass(clsName));
      }
    }

    //    System.out.println("Dropping classes, spanning " + THREADS + " threads...");

    ConcurrentTestHelper.test(
        THREADS,
        new TestFactory<Void>() {
          @Override
          public Callable<Void> createWorker() {
            return new DropClassCommandExecutor(url);
          }
        });

    //    System.out.println("Done!");

    Assert.assertEquals(counter.get(), 0);
  }
}
