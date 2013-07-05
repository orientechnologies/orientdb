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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import com.hazelcast.core.Hazelcast;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Test class that creates and executes distributed operations against a cluster of servers created in the same JVM.
 */
public class ServerClusterTest {
  protected static final int DELAY_SERVER_STARTUP  = 0;
  protected static final int DELAY_SERVER_ALIGNING = 0;
  protected static final int DELAY_WRITER          = 0;
  protected static final int DELAY_READER_QUERY    = 1000;

  protected static String    DBNAME                = "distributed";

  protected List<ServerRun>  serverInstance        = new ArrayList<ServerRun>();
  protected int              count                 = 2000;

  public ServerClusterTest(final int servers) {
    Orient.setRegisterDatabaseByPath(true);
    for (int i = 0; i < servers; ++i)
      serverInstance.add(new ServerRun("target/servers/", "" + i));
  }

  public void execute() throws Exception {
    System.out.println("Starting test between " + serverInstance.size() + " servers executing " + count + " operations...");

    for (ServerRun server : serverInstance) {
      server.startServer("orientdb-dserver-config-" + server.getServerId() + ".xml");
      try {
        Thread.sleep(DELAY_SERVER_STARTUP * serverInstance.size());
      } catch (InterruptedException e) {
      }
    }

    try {
      System.out.println("Server started, waiting for synchronization (" + (DELAY_SERVER_ALIGNING * serverInstance.size() / 1000)
          + "secs)...");
      Thread.sleep(DELAY_SERVER_ALIGNING * serverInstance.size());
    } catch (InterruptedException e) {
    }

    for (ServerRun server : serverInstance) {
      final ODocument cfg = server.getServerInstance().getDistributedManager().getClusterConfiguration();
      Assert.assertNotNull(cfg);
      Assert.assertEquals(((Collection<?>) cfg.field("members")).size(), serverInstance.size());
    }

    System.out.println("Creating Writers and Readers threads...");

    final ExecutorService writerExecutor = Executors.newCachedThreadPool();
    final ExecutorService readerExecutor = Executors.newCachedThreadPool();

    for (ServerRun server : serverInstance) {
      Writer writer = new Writer("local:" + server.getDatabasePath(DBNAME));
      writerExecutor.submit(writer);

      Reader reader = new Reader("local:" + server.getDatabasePath(DBNAME));
      readerExecutor.submit(reader);
    }

    System.out.println("Threads started, waiting for the end");

    writerExecutor.shutdown();
    Assert.assertTrue(writerExecutor.awaitTermination(3, TimeUnit.MINUTES));

    System.out.println("Writer threads finished, shutting down Reader threads...");

    readerExecutor.shutdownNow();
    Assert.assertTrue(readerExecutor.awaitTermination(10, TimeUnit.SECONDS));

    System.out.println("All threads have finished, shutting down server instances");

    for (ServerRun server : serverInstance)
      server.shutdownServer();

    Hazelcast.shutdownAll();

    System.out.println("Execution termined");
  }

  protected void createDatabases() throws IOException {
    // CREATE THE DATABASE
    final Iterator<ServerRun> it = serverInstance.iterator();
    final ServerRun master = it.next();

    master.createDatabase(DBNAME);

    // COPY DATABASE TO OTHER SERVERS
    while (it.hasNext()) {
      final ServerRun replicaSrv = it.next();
      master.copyDatabase(DBNAME, replicaSrv.getDatabasePath(DBNAME));
    }
  }

  public static void main(final String[] args) throws Exception {
    String command;
    int servers;

    if (args.length >= 1)
      command = args[0];
    else
      throw new IllegalArgumentException("Usage: create [<servers>] or execute [<servers>]");

    if (args.length >= 2)
      servers = Integer.parseInt(args[1]);
    else
      servers = 2;

    final ServerClusterTest main = new ServerClusterTest(servers);

    if (command.equals("create"))
      main.createDatabases();
    else if (command.equals("execute"))
      main.execute();
    else if (command.equals("create+execute")) {
      main.createDatabases();
      main.execute();
    } else
      System.out.println("Usage: create, execute or create+execute");

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

          if ((i + 1) % 1 == 0)
            System.out.println("\nWriter " + name + " created " + (i + 1) + "/" + count + " records so far");

          ODocument person = new ODocument("Person").fields("id", UUID.randomUUID().toString(), "firstName", "Billy", "lastName",
              "Mayes" + i, "birthday", new Date(), "children", i);
          database.save(person);

          Thread.sleep(DELAY_WRITER);

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
      String name = null;
      try {
        while (!Thread.interrupted()) {
          ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
          try {
            List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));

            if (name == null)
              name = database.getURL();

            System.out.println("\nReader " + name + " sql count: " + result.get(0) + " counting class: "
                + database.countClass("Person") + " counting cluster: " + database.countClusterElements("Person"));

            Thread.sleep(DELAY_READER_QUERY);

          } catch (InterruptedException e) {
            System.out.println("\nReader received interrupt (db=" + database.getURL());
            Thread.currentThread().interrupt();
            break;

          } finally {
            database.close();
          }
        }
      } finally {
        ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
        System.out.println("\nReader END REPORT " + name + " sql count: " + result.get(0) + " counting class: "
            + database.countClass("Person") + " counting cluster: " + database.countClusterElements("Person"));
      }
    }
  }
}
