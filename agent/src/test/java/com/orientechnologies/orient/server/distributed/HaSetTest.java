package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
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

/**
 * @author Luca Garulli
 */
public class HaSetTest extends AbstractServerClusterTest {
  private final static int SERVERS = 2;

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

    ODistributedServerManager distributedManager = firstServer.getServerInstance().getDistributedManager();

    ODatabaseSession db = firstServer.getServerInstance().openDatabase(getDatabaseName());

    try {

      db.command("HA set role `europe-0`=REPLICA");
      Thread.sleep(1000);
      Assert.assertEquals(ODistributedConfiguration.ROLES.REPLICA,
          distributedManager.getDatabaseConfiguration(getDatabaseName()).getServerRole("europe-0"));

      db.command("HA set role `europe-1`=REPLICA");
      Thread.sleep(1000);
      Assert.assertEquals(ODistributedConfiguration.ROLES.REPLICA,
          distributedManager.getDatabaseConfiguration(getDatabaseName()).getServerRole("europe-1"));

      db.command("HA set role `europe-0`=MASTER");
      Thread.sleep(1000);
      Assert.assertEquals(ODistributedConfiguration.ROLES.MASTER,
          distributedManager.getDatabaseConfiguration(getDatabaseName()).getServerRole("europe-0"));

      db.command("HA set role `europe-1`=MASTER");
      Thread.sleep(1000);
      Assert.assertEquals(ODistributedConfiguration.ROLES.MASTER,
          distributedManager.getDatabaseConfiguration(getDatabaseName()).getServerRole("europe-1"));

      db.command("HA set owner `*`=`europe-1`");
      Thread.sleep(1000);
      Assert.assertEquals("europe-1", distributedManager.getDatabaseConfiguration(getDatabaseName()).getClusterOwner("*"));

      db.command("HA set owner `*`=`europe-0`");
      Thread.sleep(1000);
      Assert.assertEquals("europe-0", distributedManager.getDatabaseConfiguration(getDatabaseName()).getClusterOwner("*"));

    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "", e);
    }
  }

  @Override
  protected void onAfterDatabaseCreation(OrientBaseGraph db) {
    db.command(new OCommandSQL("CREATE CLASS Person extends V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Person.name STRING")).execute();
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "dc-server-config-" + server.getServerId() + ".xml";
  }

}
