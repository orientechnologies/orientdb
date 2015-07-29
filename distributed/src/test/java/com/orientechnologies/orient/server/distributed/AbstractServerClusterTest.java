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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Test class that creates and executes distributed operations against a cluster of servers created in the same JVM.
 */
public abstract class AbstractServerClusterTest {
  protected int             delayServerStartup = 0;
  protected int             delayServerAlign   = 0;
  protected String          rootDirectory      = "target/servers/";

  protected List<ServerRun> serverInstance     = new ArrayList<ServerRun>();

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
    Orient.setRegisterDatabaseByPath(true);
    for (int i = 0; i < servers; ++i)
      serverInstance.add(new ServerRun(rootDirectory, "" + i));
  }

  public void execute() throws Exception {
    System.out.println("Starting test against " + serverInstance.size() + " server nodes...");

    for (ServerRun server : serverInstance) {
      log("STARTING SERVER -> " + server.getServerId() + "...");
      server.startServer(getDistributedServerConfiguration(server));
      try {
        Thread.sleep(delayServerStartup * serverInstance.size());
      } catch (InterruptedException e) {
      }

      onServerStarted(server);
    }

    try {
      System.out.println("Server started, waiting for synchronization (" + (delayServerAlign * serverInstance.size() / 1000)
          + "secs)...");
      Thread.sleep(delayServerAlign * serverInstance.size());
    } catch (InterruptedException e) {
    }

    for (ServerRun server : serverInstance) {
      final ODocument cfg = server.getServerInstance().getDistributedManager().getClusterConfiguration();
      Assert.assertNotNull(cfg);
    }

    log("Executing test...");

    try {
      executeTest();
    } finally {
      onAfterExecution();

      log("Shutting down nodes...");
      for (ServerRun server : serverInstance) {
        System.out.println("Shutting down node " + server.getServerId() + "...");
        server.shutdownServer();
      }

      log("Test finished");

      onTestEnded();

      Hazelcast.shutdownAll();

      deleteServers();
    }
  }

  protected void log(final String iMessage) {
    System.out
        .println("\n**********************************************************************************************************");
    System.out.println(iMessage);
    System.out
        .println("**********************************************************************************************************\n");
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

}
