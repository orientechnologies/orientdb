/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.orient.server.distributed.scenariotest;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ServerRun;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server down (quorum=2) with DBs distributed as below:
 *    - server1: db A, db B
 *    - server2: db B, db C
 * - servers startup
 * - each server deploys its dbs in the cluster of nodes
 * - check consistency on all servers:
 *      - all the servers have  db A, db B, db C.
 *      - db A, db B and db C are consistent on each server
 */
public class MultipleDBAlignmentOnNodesJoining extends AbstractScenarioTest {


  @Test
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // execute writes only on server3
    executeWritesOnServers = new ArrayList<ServerRun>();
    executeWritesOnServers.add(serverInstance.get(2));

    execute();
  }



  @Override
  public void executeTest() throws Exception {    //  TO-CHANGE

    List<ODocument> result = null;
    ODatabaseDocumentTx dbServer3 = new ODatabaseDocumentTx(getRemoteDatabaseURL(serverInstance.get(2))).open("admin", "admin");
    String dbServerUrl1 = getRemoteDatabaseURL(serverInstance.get(0));

    try {

      /*
       * Test with quorum = 2
       */

      banner("Test with quorum = 2");

      // TODO
      // Override prepare() method in AbstractServerClusterTest

      // check consistency on all the server:
      // all the records destined to server3 were redirected to an other server, so we must inspect consistency for all 500 records
      checkWritesAboveCluster(serverInstance, executeWritesOnServers);

    } catch(Exception e) {
      e.printStackTrace();
      fail();
    } finally {

      if(!dbServer3.isClosed()) {
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
        dbServer3.close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      }
    }
  }



}
