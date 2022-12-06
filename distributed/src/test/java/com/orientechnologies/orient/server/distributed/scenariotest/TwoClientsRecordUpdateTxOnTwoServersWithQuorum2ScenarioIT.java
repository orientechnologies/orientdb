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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Checks for consistency on the cluster with these steps: - 2 server (quorum=2) - a record is
 * inserted on server1 - the record (version 1) is present in full replica on all the servers - a
 * delay is inserted into tx of server0 - client c0 begins a tx on server1 and client c1 begins a tx
 * on server2 at the same time (both txs for updating the same record) - client c0 waits the delay -
 * client c1 tries to update the record, but server0 return a lockedrecord exception - after c1
 * gives up, c0 tries the tx and succeeds
 */
public class TwoClientsRecordUpdateTxOnTwoServersWithQuorum2ScenarioIT
    extends AbstractScenarioTest {

  private final String RECORD_ID = "R001";
  private HashMap<String, Object> lukeFields =
      new HashMap<String, Object>() {
        {
          put("firstName", "Luke");
          put("lastName", "Skywalker");
        }
      };
  private HashMap<String, Object> darthFields =
      new HashMap<String, Object>() {
        {
          put("firstName", "Darth");
          put("lastName", "Vader");
        }
      };

  @Test
  @Ignore
  public void test() throws Exception {
    maxRetries = 10;
    init(2);
    prepare(false);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    setWriteQuorum(2);

    ODatabaseDocument dbServer0 = getDatabase(0);

    try {

      // inserts record
      ODocument recordServer0 =
          new ODocument("Person").fields("id", RECORD_ID, "firstName", "Han", "lastName", "Solo");
      recordServer0.save();

      // waits for propagation of the record on all the servers
      waitForInsertedRecordPropagation(RECORD_ID);

      // retrieves record from server1 and checks they're equal
      ODocument recordServer1 = retrieveRecord(serverInstance.get(1), RECORD_ID);
      assertEquals(recordServer1.getVersion(), recordServer0.getVersion());
      assertEquals(recordServer1.field("id"), recordServer0.field("id"));
      assertEquals(recordServer1.field("firstName"), recordServer0.field("firstName"));
      assertEquals(recordServer1.field("lastName"), recordServer0.field("lastName"));

      // gets the actual version of record from server0
      int actualVersion = recordServer0.getVersion();

      // updates the same record from two different clients, each calling a different node
      List<Callable<Void>> clients = new LinkedList<Callable<Void>>();
      clients.add(new RecordUpdater(serverInstance.get(0), recordServer0, lukeFields, true));
      clients.add(new RecordUpdater(serverInstance.get(1), recordServer1, darthFields, true));
      List<Future<Void>> futures = Executors.newCachedThreadPool().invokeAll(clients);
      executeFutures(futures);

      // checks that record on server1 is discarded in favour of record present on server0
      waitForUpdatedRecordPropagation(
          RECORD_ID, "firstName", lukeFields.get("firstName").toString());

      recordServer0 = retrieveRecord(serverInstance.get(0), RECORD_ID);
      recordServer1 = retrieveRecord(serverInstance.get(1), RECORD_ID);

      int finalVersionServer0 = recordServer0.getVersion();
      int finalVersionServer1 = recordServer1.getVersion();
      assertEquals(finalVersionServer0, actualVersion + 1);
      assertEquals(finalVersionServer1, actualVersion + 1);
    } finally {
      dbServer0.close();
    }
  }

  @Override
  public String getDatabaseName() {
    return "distributed-simultaneous-update";
  }

  private void setWriteQuorum(int quorum) throws InterruptedException {
    ODistributedPlugin manager =
        (ODistributedPlugin) serverInstance.get(0).getServerInstance().getDistributedManager();
    OModifiableDistributedConfiguration databaseConfiguration =
        manager.getDatabaseConfiguration(getDatabaseName()).modify();
    ODocument cfg = databaseConfiguration.getDocument();
    cfg.field("writeQuorum", quorum);
    manager.updateCachedDatabaseConfiguration(getDatabaseName(), databaseConfiguration);
    Thread.sleep(100);
  }
}
