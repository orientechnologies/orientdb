package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

/**
 * @author Enrico Risa
 */
public class HaSyncClusterTest extends AbstractServerClusterTest {
  private final static int SERVERS     = 2;
  public static final  int NUM_RECORDS = 1000;

  ExecutorService executorService = Executors.newSingleThreadExecutor();

  public String getDatabaseName() {
    return "HaSyncClusterTest";
  }

  @Test
  @Ignore
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() throws Exception {

    ServerRun firstServer = serverInstance.get(0);
    String databasePath = firstServer.getDatabasePath(getDatabaseName());

    String localNodeName = firstServer.getServerInstance().getDistributedManager().getLocalNodeName();
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + databasePath);
    db.open("admin", "admin");

    final OClass person = db.getMetadata().getSchema().getClass("Person");
    person.createProperty("name", OType.STRING);
    person.createIndex("testAutoSharding", OClass.INDEX_TYPE.UNIQUE.toString(), (OProgressListener) null, (ODocument) null,
        "AUTOSHARDING", new String[] { "name" });

    try {
      Future<Long> future = invokeSyncCluster(localNodeName, serverInstance.get(1));

      for (int i = 0; i < NUM_RECORDS; i++) {

        ODocument doc = new ODocument("Person");
        doc.field("name", "person" + i);
        db.save(doc);
      }
      List<ODocument> query = db.query(new OSQLSynchQuery("select count(*) from Person"));

      Long result0 = query.iterator().next().field("count");
      Long result1 = future.get();

      Assert.assertEquals(result1, result0);

    } finally {
      db.close();
    }
  }

  protected Future<Long> invokeSyncCluster(final String localNodeName, final ServerRun server) {

    Future<Long> future = executorService.submit(new Callable<Long>() {
      @Override
      public Long call() throws Exception {

        Long countRecors = new Long(0);

        String databasePath = server.getDatabasePath(getDatabaseName());
        ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + databasePath);
        db.open("admin", "admin");
        try {

          ODistributedServerManager manager = server.getServerInstance().getDistributedManager();
          ODistributedConfiguration databaseConfiguration = manager.getDatabaseConfiguration(getDatabaseName());
          int[] persons = db.getMetadata().getSchema().getClass("Person").getClusterIds();
          String clusterName = null;
          for (int person : persons) {
            String clusterNameById = db.getStorage().getPhysicalClusterNameById(person);
            String clusterOwner = databaseConfiguration.getClusterOwner(clusterNameById);
            if (clusterOwner.equals(localNodeName)) {
              clusterName = clusterNameById;
              break;
            }
          }

          Assert.assertNotNull(clusterName);

          db.command(new OCommandSQL(String.format("HA sync cluster %s", clusterName))).execute();

          final ODistributedMessageService messageService = manager.getMessageService();

          waitFor(5000, new OCallable<Boolean, Void>() {
            @Override
            public Boolean call(Void iArgument) {
              ODocument messageStats = messageService.getMessageStats();

              long heartbeat = 0;
              long deploy_cluster = 0;
              if (messageStats != null && messageStats.containsField("heartbeat")) {
                heartbeat = messageStats.field("heartbeat");
              }

              if (messageStats != null && messageStats.containsField("deploy_cluster")) {
                deploy_cluster = messageStats.field("deploy_cluster");
              }
              long processed = messageService.getProcessedRequests() - heartbeat - deploy_cluster;

              OLogManager.instance()
                  .info(this, "Waiting for processed requests to be [%d], actual [%d] with stats [%s] ", NUM_RECORDS, processed,
                      messageStats.toJSON());

              return processed >= NUM_RECORDS;
            }
          }, String.format("Number for processed request should be [%s]", NUM_RECORDS));

          List<ODocument> query = db.query(new OSQLSynchQuery("select count(*) from Person"));

          countRecors = query.iterator().next().field("count");

        } finally {
          db.close();
        }

        return countRecors;
      }
    });
    return future;
  }

  @Override
  protected void onAfterDatabaseCreation(OrientBaseGraph db) {
    db.command(new OCommandSQL("CREATE CLASS Person extends V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Person.name STRING")).execute();
  }

  @Override
  protected void waitFor(int serverId, OCallable<Boolean, ODatabaseDocumentTx> condition, long timeout) {
    super.waitFor(serverId, condition, timeout);
  }
}
