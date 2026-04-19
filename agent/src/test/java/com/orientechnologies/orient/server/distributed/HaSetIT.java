package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ODatabasesTopology;
import com.orientechnologies.orient.distributed.context.coordination.dbs.ONodeRole;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.junit.Assert;
import org.junit.Test;

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

/** @author Luca Garulli */
public class HaSetIT extends AbstractServerClusterTest {
  private static final OLogger logger = OLogManager.instance().logger(HaSetIT.class);
  private static final int SERVERS = 2;

  public String getDatabaseName() {
    return "HaSetTest";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() {
    ServerRun firstServer = serverInstance.get(0);

    ODistributedServerManager distributedManager =
        firstServer.getServerInstance().getDistributedManager();

    ODatabaseSession db = firstServer.getServerInstance().openDatabase(getDatabaseName());

    OrientDBDistributed ctx = (OrientDBDistributed) firstServer.getServerInstance().getDatabases();
    ODatabasesTopology dbTopology = ctx.getNodeState().getDatabaseTopology();
    var dbId = dbTopology.getDatabaseId(getDatabaseName()).get();

    db.command("HA set role `europe-0`=REPLICA");
    Assert.assertEquals(ONodeRole.Replica, dbTopology.getRole(dbId, new ONodeId("europe-0")));

    db.command("HA set role `europe-1`=REPLICA");
    Assert.assertEquals(ONodeRole.Replica, dbTopology.getRole(dbId, new ONodeId("europe-1")));

    db.command("HA set role `europe-0`=MASTER");
    Assert.assertEquals(ONodeRole.Main, dbTopology.getRole(dbId, new ONodeId("europe-0")));

    db.command("HA set role `europe-1`=MASTER");
    Assert.assertEquals(ONodeRole.Main, dbTopology.getRole(dbId, new ONodeId("europe-1")));
  }

  @Override
  protected void onAfterDatabaseCreation(OrientGraph db) {
    db.executeSql("CREATE CLASS Person extends V").close();
    db.executeSql("CREATE PROPERTY Person.name STRING").close();
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "dc-server-config-" + server.getServerId() + ".xml";
  }
}
