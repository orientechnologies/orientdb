/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.distributed;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Insert records concurrently against the cluster
 */
public class ServerClusterInsertTest extends AbstractServerClusterTest {
  protected static final int delayWriter = 0;
  protected static final int delayReader = 1000;
  protected int              count       = 2000;

  public String getDatabaseName() {
    return "distributed";
  }

  /**
   * Event called right after the database has been created and right before to be replicated to the X servers
   * 
   * @param db
   *          Current database
   */
  protected void onAfterDatabaseCreation(final ODatabaseDocumentTx db) {
  }

  public void executeTest() throws Exception {
    System.out.println("Creating Writers and Readers threads...");

    final ExecutorService writerExecutor = Executors.newCachedThreadPool();
    final ExecutorService readerExecutor = Executors.newCachedThreadPool();

    for (ServerRun server : serverInstance) {
      Writer writer = new Writer(getDatabaseURL(server));
      writerExecutor.submit(writer);

      Reader reader = new Reader(getDatabaseURL(server));
      readerExecutor.submit(reader);
    }

    System.out.println("Threads started, waiting for the end");

    writerExecutor.shutdown();
    Assert.assertTrue(writerExecutor.awaitTermination(3, TimeUnit.MINUTES));

    System.out.println("Writer threads finished, shutting down Reader threads...");

    readerExecutor.shutdownNow();
    Assert.assertTrue(readerExecutor.awaitTermination(10, TimeUnit.SECONDS));

    System.out.println("All threads have finished, shutting down server instances");

    for (ServerRun server : serverInstance) {
      printStats(getDatabaseURL(server));
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "local:" + server.getDatabasePath(getDatabaseName());
  }

  class Writer implements Runnable {
    private final String databaseUrl;

    public Writer(final String db) {
      databaseUrl = db;
    }

    @Override
    public void run() {
      String name = null;
      for (int i = 0; i < count; i++) {
        final ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        try {
          if (name == null)
            name = database.getURL();

          if ((i + 1) % 10000 == 0)
            System.out.println("\nWriter " + name + " created " + (i + 1) + "/" + count + " records so far");

          ODocument person = new ODocument("Person").fields("id", UUID.randomUUID().toString(), "firstName", "Billy", "lastName",
              "Mayes" + i, "birthday", new Date(), "children", i);
          database.save(person);

          Thread.sleep(delayWriter);

        } catch (InterruptedException e) {
          System.out.println("Writer received interrupt (db=" + database.getURL());
          Thread.currentThread().interrupt();
          break;
        } finally {
          database.close();
        }
      }

      System.out.println("\nWriter " + name + " END");
    }
  }

  class Reader implements Runnable {
    private final String databaseUrl;

    public Reader(final String db) {
      databaseUrl = db;
    }

    @Override
    public void run() {
      try {
        while (!Thread.interrupted()) {
          try {
            printStats(databaseUrl);
            Thread.sleep(delayReader);

          } catch (Exception e) {
            e.printStackTrace();
            break;

          }
        }

      } finally {
        printStats(databaseUrl);
      }
    }
  }

  private void printStats(final String databaseUrl) {
    final ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
    try {
      List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));

      final String name = database.getURL();

      System.out.println("\nReader " + name + " sql count: " + result.get(0) + " counting class: " + database.countClass("Person")
          + " counting cluster: " + database.countClusterElements("Person"));

      try {
        List<ODocument> conflicts = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from ODistributedConflict"));
        Assert.assertEquals(0, conflicts.get(0).field("count"));
        System.out.println("\nReader " + name + " conflicts: " + result.get(0));
      } catch (OQueryParsingException e) {
        // IGNORE IT
      }

    } finally {
      database.close();
    }

  }
}
