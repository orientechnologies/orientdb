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
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.GroupProperties;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.hazelcast.OHazelcastDistributedMessageService;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import org.junit.Assert;

import java.io.IOException;
import java.util.*;
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

  protected List<ServerRun> serverInstance         = new ArrayList<ServerRun>();
  protected AtomicLong      totalVertices          = new AtomicLong(0);

  protected AbstractServerClusterTest() {
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
        for (ServerRun server : serverInstance) {
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
                server.startServer(getDistributedServerConfiguration(server));
              } catch (Exception e) {
                e.printStackTrace();
              }
              onServerStarted(server);
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
        final ODocument cfg = server.getServerInstance().getDistributedManager().getClusterConfiguration();
        Assert.assertNotNull(cfg);
      }

      OLogManager.instance().flush();
      banner("Executing test...");

      try {
        executeTest();
      } finally {
        onAfterExecution();
      }

    } finally {
      banner("Test finished");

      OLogManager.instance().flush();
      banner("Shutting down nodes...");
      for (ServerRun server : serverInstance) {
        System.out.println("Shutting down node " + server.getServerId() + "...");
        server.shutdownServer();
      }

      onTestEnded();

      banner("Shutdown HZ...");
      Hazelcast.shutdownAll();

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
    // CREATE THE DATABASE
    final Iterator<ServerRun> it = serverInstance.iterator();
    final ServerRun master = it.next();

    if (iCreateDatabase) {
      final OrientBaseGraph graph = master.createDatabase(getDatabaseName());
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

  protected void executeWhen(Callable<Boolean> condition, Callable action) throws Exception {
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

  protected String getDatabaseURL(ServerRun server) {
    return null;
  }

  protected void startQueueMonitorTask() {
    new Timer(true).schedule(new TimerTask() {
      @Override
      public void run() {
        // DUMP QUEUE SIZES
        System.out.println("---------------------------------------------------------------------");
        for (int i = 0; i < serverInstance.size(); ++i) {
          try {
            final OHazelcastPlugin dInstance = (OHazelcastPlugin) serverInstance.get(i).getServerInstance().getDistributedManager();

            final String queueName = OHazelcastDistributedMessageService.getRequestQueueName(dInstance.getLocalNodeName(),
                getDatabaseName());

            final OHazelcastDistributedMessageService msgService = dInstance.getMessageService();
            if (msgService != null) {
              final IQueue<Object> queue = msgService.getQueue(queueName);
              System.out.println("Queue " + queueName + " size = " + queue.size());
            }
          } catch (Exception e) {
          }
        }
        System.out.println("---------------------------------------------------------------------");
      }
    }, 1000, 1000);
  }

  protected void startCountMonitorTask(final String iClassName) {
    new Timer(true).schedule(new TimerTask() {
      @Override
      public void run() {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(getDatabaseURL(serverInstance.get(0)));
        db.open("admin", "admin");
        try {
          totalVertices.set(db.countClass(iClassName));
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          db.close();
        }
      }
    }, 1000, 1000);
  }
}
