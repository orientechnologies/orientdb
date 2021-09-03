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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Test;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 server - db creation on
 * server1 - check the new db is present both on server2 and server3 - 5 threads write 100 records
 * on server1 - check consistency: db with all the records are consistent
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class DBCreationAndUpdateOneNodeScenarioIT extends AbstractScenarioTest {

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    String url1 = "plocal:target/server0/databases/distributed-dbcreation-update";
    String url2 = "plocal:target/server1/databases/distributed-dbcreation-update";
    String url3 = "plocal:target/server2/databases/distributed-dbcreation-update";

    // creating new database on server0
    Thread.sleep(20000);

    // checking the db was created both on server1 and server2
    ODatabaseDocument dbServer2 = getDatabase(1);
    try {
      assertNotNull(dbServer2);
      List<ODocument> result =
          dbServer2.query(new OSQLSynchQuery<OIdentifiable>("select from Person"));
      assertEquals(0, result.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      dbServer2.close();
    }

    ODatabaseDocument dbServer3 = getDatabase(2);
    try {
      assertNotNull(dbServer3);
      List<ODocument> result =
          dbServer3.query(new OSQLSynchQuery<OIdentifiable>("select from Person"));
      assertEquals(0, result.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      dbServer3.close();
    }

    // executing writes on server1
    executeTestsOnServers.remove(2);
    executeTestsOnServers.remove(1);
    executeMultipleWrites(super.executeTestsOnServers, "plocal");

    // check consistency
    checkWritesAboveCluster(serverInstance, executeTestsOnServers);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-dbcreation-update";
  }
}
