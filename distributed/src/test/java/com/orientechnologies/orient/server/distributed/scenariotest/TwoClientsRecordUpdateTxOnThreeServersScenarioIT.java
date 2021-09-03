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
import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Checks for consistency on the cluster for these steps: - 3 server (server0, server1 and server2)
 * with writeQuorum=2 - a record is inserted on server0 - the record (version 1) is propagated to
 * the other two servers (server1 and server2) - introduce a delay after record locking for server0
 * and server1 - two clients (connected to server0 and server1) tries to update that record -
 * server0 notifies the update to server1 and server2: server1 return a RecordLockedException, since
 * it's trying to update that record itself as well; server2 returns "true" - server0's request has
 * reached the write quorum and propagates the new value to all the servers in the cluster
 */
public class TwoClientsRecordUpdateTxOnThreeServersScenarioIT extends AbstractScenarioTest {

  private final String RECORD_ID = "R001";
  private Map<String, Object> hanFields =
      new HashMap<String, Object>() {
        {
          put("id", RECORD_ID);
          put("firstName", "Han");
          put("lastName", "Solo");
        }
      };
  private Map<String, Object> darthFields =
      new HashMap<String, Object>() {
        {
          put("id", RECORD_ID);
          put("firstName", "Darth");
          put("lastName", "Vader");
        }
      };
  private Map<String, Object> leiaFields =
      new HashMap<String, Object>() {
        {
          put("id", RECORD_ID);
          put("firstName", "Leia");
          put("lastName", "Organa");
        }
      };

  @Test
  @Ignore
  public void test() throws Exception {
    init(3);
    prepare(false);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    ODatabaseDocument dbServer0 = getDatabase(0);

    // inserts record
    ODocument recordServer0 = new ODocument("Person").fromMap(hanFields);
    recordServer0.save();

    // waits for propagation of the record on all the servers
    waitForInsertedRecordPropagation(RECORD_ID);

    ODatabaseDocument dbServer1 = getDatabase(1);

    try {
      // retrieves record from server1 and server2 and checks they're equal
      ODocument recordServer1 = retrieveRecord(serverInstance.get(1), RECORD_ID);
      ODocument recordServer2 = retrieveRecord(serverInstance.get(2), RECORD_ID);
      assertTrue(compareRecords(recordServer0, recordServer1));
      assertTrue(compareRecords(recordServer0, recordServer2));

      // gets the actual version of the record
      int actualVersion = recordServer0.getVersion();
      OLogManager.instance().error(this, "Actual version: " + actualVersion, null);

      // sets a delay for operations on distributed storage of server0 and server1

      // updates the same record from two different clients, each calling a different server
      // (server2 is idle)
      List<Callable<Void>> clients = new LinkedList<Callable<Void>>();
      clients.add(new RecordUpdater(serverInstance.get(0), recordServer0, darthFields, true));
      clients.add(new RecordUpdater(serverInstance.get(1), recordServer1, leiaFields, true));
      List<Future<Void>> futures = Executors.newCachedThreadPool().invokeAll(clients);
      executeFutures(futures);

      //    Thread.sleep(2000);
      //    recordServer0 = retrieveRecord(getDatabaseURL(serverInstance.get(0)), RECORD_ID);
      //    recordServer1 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), RECORD_ID);
      //    recordServer2 = retrieveRecord(getDatabaseURL(serverInstance.get(2)), RECORD_ID);
      //
      //    OLogManager.instance().error(this, "server0: " + recordServer0.toString() + " v" +
      // recordServer0.getVersion());
      //    OLogManager.instance().error(this, "server1: " + recordServer1.toString() + " v" +
      // recordServer1.getVersion());
      //    OLogManager.instance().error(this, "server2: " + recordServer2.toString() + " v" +
      // recordServer2.getVersion());

      // checks that record on server0 is the one which wins over the others
      System.out.println("serverInstance: " + serverInstance);
      waitForUpdatedRecordPropagation(
          RECORD_ID, "firstName", darthFields.get("firstName").toString());

      recordServer0 = retrieveRecord(serverInstance.get(0), RECORD_ID);
      recordServer1 = retrieveRecord(serverInstance.get(1), RECORD_ID);
      recordServer2 = retrieveRecord(serverInstance.get(2), RECORD_ID);

      int finalVersionServer0 = recordServer0.getVersion();
      int finalVersionServer1 = recordServer1.getVersion();
      int finalVersionServer2 = recordServer2.getVersion();
      assertEquals(actualVersion + 1, finalVersionServer0);
      assertEquals(actualVersion + 1, finalVersionServer1);
      assertEquals(actualVersion + 1, finalVersionServer2);
    } finally {
      dbServer0.close();
      dbServer1.close();
    }
  }

  @Override
  public String getDatabaseName() {
    return "distributed-two-simultaneous-update-on-three-servers";
  }

  private boolean compareRecords(ODocument record1, ODocument record2) {
    return record1.getVersion() == record2.getVersion()
        && record1.field("id").equals(record2.field("id"))
        && record1.field("firstName").equals(record2.field("firstName"))
        && record1.field("lastName").equals(record2.field("lastName"));
  }
}
