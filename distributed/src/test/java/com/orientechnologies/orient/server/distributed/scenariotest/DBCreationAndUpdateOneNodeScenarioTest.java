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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server
 * - db creation on server1
 * - check the new db is present both on server2 and server3
 * - 5 threads write 100 records on server1
 * - check consistency: db with all the records are consistent
 */

public class DBCreationAndUpdateOneNodeScenarioTest extends AbstractScenarioTest {

  @Ignore
  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    super.executeWritesOnServers.addAll(super.serverInstance);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    String url1 = "plocal:target/server0/databases/new-distributed-db";
    String url2 = "plocal:target/server1/databases/new-distributed-db";
    String url3 = "plocal:target/server2/databases/new-distributed-db";

    // creating new database on server1
    ODatabaseDocumentTx dbServer1 = new ODatabaseDocumentTx(url1);

    Thread.sleep(5000);

    // checking the db was created both on server2 and server3
    ODatabaseRecordThreadLocal.INSTANCE.set(null);
    try {
      ODatabaseDocumentTx dbServer2 = poolFactory.get(url2,"admin","admin").acquire();
      assertNotNull(dbServer2);
      List<ODocument> result = dbServer2.query(new OSQLSynchQuery<OIdentifiable>("select from Person"));
      assertEquals(0, result.size());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }

    ODatabaseRecordThreadLocal.INSTANCE.set(null);
    try {
      ODatabaseDocumentTx dbServer3 = poolFactory.get(url3,"admin","admin").acquire();
      assertNotNull(dbServer3);
      List<ODocument> result = dbServer3.query(new OSQLSynchQuery<OIdentifiable>("select from Person"));
      assertEquals(0, result.size());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }

    // executing writes on server1
    executeWritesOnServers.remove(2);
    executeWritesOnServers.remove(1);
    executeMultipleWrites(super.executeWritesOnServers, "plocal");

    // check consistency
    checkWritesAboveCluster(serverInstance, executeWritesOnServers);
  }

}
