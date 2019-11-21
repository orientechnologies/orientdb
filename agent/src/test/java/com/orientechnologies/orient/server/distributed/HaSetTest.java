package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import org.junit.Assert;

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

//  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void executeTest() {
    ServerRun firstServer = serverInstance.get(0);

    ODistributedServerManager distributedManager = firstServer.getServerInstance().getDistributedManager();

    String databasePath = firstServer.getDatabasePath(getDatabaseName());
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:" + databasePath);
    db.open("admin", "admin");

    try {

      db.command(new OCommandSQL(String.format("HA set role europe0=REPLICA"))).execute();
      Thread.sleep(1000);
      Assert.assertEquals(ODistributedConfiguration.ROLES.REPLICA,
          distributedManager.getDatabaseConfiguration(getDatabaseName()).getServerRole("europe0"));

      db.command(new OCommandSQL(String.format("HA set role europe1=REPLICA"))).execute();
      Thread.sleep(1000);
      Assert.assertEquals(ODistributedConfiguration.ROLES.REPLICA,
          distributedManager.getDatabaseConfiguration(getDatabaseName()).getServerRole("europe1"));

      db.command(new OCommandSQL(String.format("HA set role europe0=MASTER"))).execute();
      Thread.sleep(1000);
      Assert.assertEquals(ODistributedConfiguration.ROLES.MASTER,
          distributedManager.getDatabaseConfiguration(getDatabaseName()).getServerRole("europe0"));

      db.command(new OCommandSQL(String.format("HA set role europe1=MASTER"))).execute();
      Thread.sleep(1000);
      Assert.assertEquals(ODistributedConfiguration.ROLES.MASTER,
          distributedManager.getDatabaseConfiguration(getDatabaseName()).getServerRole("europe1"));

      db.command(new OCommandSQL(String.format("HA set owner *=europe1"))).execute();
      Thread.sleep(1000);
      Assert.assertEquals("europe1", distributedManager.getDatabaseConfiguration(getDatabaseName()).getClusterOwner("*"));

      db.command(new OCommandSQL(String.format("HA set owner *=europe0"))).execute();
      Thread.sleep(1000);
      Assert.assertEquals("europe0", distributedManager.getDatabaseConfiguration(getDatabaseName()).getClusterOwner("*"));

    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "", e);
    }
  }

  @Override
  protected void onAfterDatabaseCreation(OrientBaseGraph db) {
    db.command(new OCommandSQL("CREATE CLASS Person extends V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Person.name STRING")).execute();
  }

}
