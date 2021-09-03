/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import junit.framework.Assert;
import org.junit.Test;

/** Distributed test on drop database and further resync. */
public class DistributedDbDropAndResyncIT extends AbstractServerClusterTxTest {
  static final int SERVERS = 2;
  final long TIMEOUT = 15000;

  @Test
  public void test() throws Exception {
    count = 10;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    for (ServerRun s : serverInstance) {

      final ODatabaseDocument db =
          s.getServerInstance().getContext().open(getDatabaseName(), "admin", "admin");

      banner("RE-SYNC DATABASE ON SERVER " + s.getServerId());
      db.command(new OCommandSQL("ha sync database")).execute();

      ODistributedServerManager.DB_STATUS currentStatus = null;

      // WAIT FOR ONLINE
      for (long chrono = System.currentTimeMillis();
          System.currentTimeMillis() - chrono < TIMEOUT; ) {

        final ODistributedServerManager.DB_STATUS status =
            s.getServerInstance()
                .getDistributedManager()
                .getDatabaseStatus(
                    s.getServerInstance().getDistributedManager().getLocalNodeName(), db.getName());

        if (status == ODistributedServerManager.DB_STATUS.ONLINE) {
          currentStatus = status;
          break;
        }
      }

      Assert.assertEquals(currentStatus, ODistributedServerManager.DB_STATUS.ONLINE);
    }
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-resyncdb";
  }
}
