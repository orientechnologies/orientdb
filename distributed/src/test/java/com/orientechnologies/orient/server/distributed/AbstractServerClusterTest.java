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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test class that creates and executes distributed operations against a cluster of servers created in the same JVM.
 */
public abstract class AbstractServerClusterTest {
  protected int             delayServerStartup     = 0;
  protected int             delayServerAlign       = 0;
  protected boolean         startupNodesInSequence = true;
  protected String          rootDirectory          = "target/servers/";
  protected AtomicLong      totalVertices          = new AtomicLong(0);

  protected List<ServerRun> serverInstance         = new ArrayList<ServerRun>();

  protected AbstractServerClusterTest() {
    OGlobalConfiguration.STORAGE_TRACK_CHANGED_RECORDS_IN_WAL.setValue(true);
  }

  @SuppressWarnings("unchecked")
  public static void main(final String[] args) throws Exception {
    Class<? extends AbstractServerClusterTest> testClass = null;
    String command = null;
    int servers = 2;

    if (args.length > 0)
      testClass = (Class<? extends AbstractServerClusterTest>) Class.forName(args[0]);
    else
      syntaxError();

    if (args.length > 1)
      command = args[1];
    else
      syntaxError();

    if (args.length > 2)
      servers = Integer.parseInt(args[2]);

    final AbstractServerClusterTest main = testClass.newInstance();
    main.init(servers);

    if (command.equals("prepare"))
      main.prepare(true);
    else if (command.equals("execute"))
      main.execute();
    else if (command.equals("prepare+execute")) {
      main.prepare(true);
      main.execute();
    } else
      System.out.println("Usage: prepare, execute or prepare+execute ...");
  }

  private static void syntaxError() {
    System.err
        .println("Syntax error. Usage: <class> <operation> [<servers>]\nWhere <operation> can be: prepare|execute|prepare+execute");
    System.exit(1);
  }

  public void init(final int servers) {
    System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");

    Orient.setRegisterDatabaseByPath(true);
    for (int i = 0; i < servers; ++i)
      serverInstance.add(new ServerRun(rootDirectory, "" + i));
  }

  public void execute() throws Exception {
    System.out.println("Starting test against " + serverInstance.size() + " server nodes...");

    try {

      if (startupNodesInSequence) {
        for (final ServerRun server : serverInstance) {
          banner("STARTING SERVER -> " + server.getServerId() + "...");

          server.startServer(getDistributedServerConfiguration(server));

          if (delayServerStartup > 0)
            try {
              Thread.sleep(delayServerStartup * serverInstance.size());
            } catch (InterruptedException e) {
            }

          onServerStarted(server);
        }
      } else {
        for (final ServerRun server : serverInstance) {
          final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
              banner("STARTING SERVER -> " + server.getServerId() + "...");
              try {
                onServerStarting(server);
                server.startServer(getDistributedServerConfiguration(server));
                onServerStarted(server);
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          });
          thread.start();
        }
      }

      if (delayServerAlign > 0)
        try {
          System.out.println(
              "Server started, waiting for synchronization (" + (delayServerAlign * serverInstance.size() / 1000) + "secs)...");
          Thread.sleep(delayServerAlign * serverInstance.size());
        } catch (InterruptedException e) {
        }

      for (ServerRun server : serverInstance) {
        final ODistributedServerManager mgr = server.getServerInstance().getDistributedManager();
        Assert.assertNotNull(mgr);
        final ODocument cfg = mgr.getClusterConfiguration();
        Assert.assertNotNull(cfg);
      }

      banner("Executing test...");

      try {
        executeTest();
      } finally {
        onAfterExecution();
      }
    } catch (Exception e) {
      System.out.println("ERROR: ");
      e.printStackTrace();
      OLogManager.instance().flush();
      throw e;
    } finally {
      banner("Test finished");

      OLogManager.instance().flush();
      banner("Shutting down nodes...");
      for (ServerRun server : serverInstance) {
        System.out.println("Shutting down node " + server.getServerId() + "...");
        server.shutdownServer();
      }

      onTestEnded();

      banner("Terminate HZ...");
      for (HazelcastInstance in : Hazelcast.getAllHazelcastInstances()) {
        in.getLifecycleService().terminate();
      }

      banner("Clean server directories...");
      deleteServers();
    }

  }

  protected void banner(final String iMessage) {
    OLogManager.instance().flush();
    System.out
        .println("\n**********************************************************************************************************");
    System.out.println(iMessage);
    System.out
        .println("**********************************************************************************************************\n");
    System.out.flush();
  }

  protected void log(final String iMessage) {
    OLogManager.instance().flush();
    System.out.println("\n" + iMessage);
    System.out.flush();
  }

  protected void onServerStarting(ServerRun server) {
  }

  protected void onServerStarted(ServerRun server) {
  }

  protected void onTestEnded() {
  }

  protected void onAfterExecution() throws Exception {
  }

  protected abstract String getDatabaseName();

  /**
   * Event called right after the database has been created and right before to be replicated to the X servers
   * 
   * @param db
   *          Current database
   */
  protected void onAfterDatabaseCreation(final OrientBaseGraph db) {
  }

  protected abstract void executeTest() throws Exception;

  protected void prepare(final boolean iCopyDatabaseToNodes) throws IOException {
    prepare(iCopyDatabaseToNodes, true);
  }

  /**
   * Create the database on first node only
   *
   * @throws IOException
   */
  protected void prepare(final boolean iCopyDatabaseToNodes, final boolean iCreateDatabase) throws IOException {
    prepare(iCopyDatabaseToNodes, iCreateDatabase, null);
  }

  /**
   * Create the database on first node only
   *
   * @throws IOException
   */
  protected void prepare(final boolean iCopyDatabaseToNodes, final boolean iCreateDatabase,
      final OCallable<Object, OrientGraphFactory> iCfgCallback) throws IOException {
    // CREATE THE DATABASE
    final Iterator<ServerRun> it = serverInstance.iterator();
    final ServerRun master = it.next();

    if (iCreateDatabase) {
      final OrientBaseGraph graph = master.createDatabase(getDatabaseName(), iCfgCallback);
      try {
        onAfterDatabaseCreation(graph);
      } finally {
        graph.shutdown();
        Orient.instance().closeAllStorages();
      }
    }

    // COPY DATABASE TO OTHER SERVERS
    while (it.hasNext()) {
      final ServerRun replicaSrv = it.next();

      replicaSrv.deleteNode();

      if (iCopyDatabaseToNodes)
        master.copyDatabase(getDatabaseName(), replicaSrv.getDatabasePath(getDatabaseName()));
    }
  }

  protected void deleteServers() {
    for (ServerRun s : serverInstance)
      s.deleteNode();
  }

  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "orientdb-dserver-config-" + server.getServerId() + ".xml";
  }

  protected void executeWhen(final Callable<Boolean> condition, final Callable action) throws Exception {
    while (true) {
      if (condition.call()) {
        action.call();
        break;
      }

      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }
  }

  protected void executeWhen(int serverId, OCallable<Boolean, ODatabaseDocumentTx> condition,
      OCallable<Boolean, ODatabaseDocumentTx> action) throws Exception {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getDatabaseURL(serverInstance.get(serverId))).open("admin", "admin");
    try {
      executeWhen(db, condition, action);
    } finally {
      if (!db.isClosed()) {
        ODatabaseRecordThreadLocal.INSTANCE.set(db);
        db.close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      }
    }
  }

  protected void executeWhen(final ODatabaseDocumentTx db, OCallable<Boolean, ODatabaseDocumentTx> condition,
      OCallable<Boolean, ODatabaseDocumentTx> action) {
    while (true) {
      db.activateOnCurrentThread();
      if (condition.call(db)) {
        action.call(db);
        break;
      }

      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }
  }

  protected void waitFor(final int serverId, final OCallable<Boolean, ODatabaseDocumentTx> condition, final long timeout) {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(getDatabaseURL(serverInstance.get(serverId))).open("admin", "admin");
    try {

      final long startTime = System.currentTimeMillis();

      while (true) {
        if (condition.call(db)) {
          break;
        }

        if (timeout > 0 && System.currentTimeMillis() - startTime > timeout)
          break;

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // IGNORE IT
        }
      }

    } finally {
      if (!db.isClosed()) {
        ODatabaseRecordThreadLocal.INSTANCE.set(db);
        db.close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      }
    }
  }

  protected void waitFor(final long timeout, final OCallable<Boolean, Void> condition) {
    final long startTime = System.currentTimeMillis();

    while (true) {
      if (condition.call(null)) {
        break;
      }

      if (timeout > 0 && System.currentTimeMillis() - startTime > timeout)
        throw new OTimeoutException("Timeout waiting for test condition");

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // IGNORE IT
      }
    }
  }

  protected String getDatabaseURL(ServerRun server) {
    return null;
  }

}
