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
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorage;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

/**
 * Checks for consistency on the cluster with these steps:
 * - 2 server (quorum=1)
 * - record1 is inserted on server1
 * - record1 (version 1) is present in full replica on all the servers
 * - client c1 begins a tx on server1, wait some time and updates record1
 * - client c2 in the meanwhile updates record1 on server2 with value value2
 * <p/>
 * Checks that c2 did not update record1 on server2 because record1 is locked by client1
 */

public class TwoClientsRecordUpdateWithTransactionsOnMultipleServersScenarioTest extends AbstractScenarioTest {

  private final String                  RECORD_ID   = "R001";
  private       HashMap<String, Object> lukeFields  = new HashMap<String, Object>() {
    {
      put("firstName", "Luke");
      put("lastName", "Skywalker");
    }
  };
  private       HashMap<String, Object> darthFields = new HashMap<String, Object>() {
    {
      put("firstName", "Darth");
      put("lastName", "Vader");
    }
  };

  @Test
  public void test() throws Exception {
    maxRetries = 10;
    init(2);
    prepare(false);
    executeWritesOnServers.addAll(serverInstance);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    ODatabaseDocumentTx dbServer1 = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();

    // inserts record1
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    ODocument record1Server1 = new ODocument("Person").fields("id", RECORD_ID, "firstName", "Han", "lastName", "Solo");
    record1Server1.save();

    // waits for propagation of the record on all the servers
    waitForInsertedRecordPropagation(RECORD_ID);

    // retrieves record1 from server2 and checks they're equal
    ODocument record1Server2 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), RECORD_ID);
    assertEquals(record1Server2.getVersion(), record1Server1.getVersion());
    assertEquals(record1Server2.field("id"), record1Server1.field("id"));
    assertEquals(record1Server2.field("firstName"), record1Server1.field("firstName"));
    assertEquals(record1Server2.field("lastName"), record1Server1.field("lastName"));

    // gets the actual version of record1
    int actualVersion = record1Server1.getVersion();

    // sets a delay for operations on distributed storage of server1
    ((ODistributedStorage) dbServer1.getStorage()).setEventListener(new AfterRecordLockDelayer("server1"));

    // updates the same record from two different clients, each calling a different node
    List<Callable<Void>> clients = new LinkedList<Callable<Void>>();
    clients.add(new RecordUpdater(getDatabaseURL(serverInstance.get(0)), record1Server1, lukeFields, true));
    clients.add(new RecordUpdater(getDatabaseURL(serverInstance.get(1)), record1Server2, darthFields, true));
    List<Future<Void>> futures = Executors.newCachedThreadPool().invokeAll(clients);
    executeFutures(futures);

    // checks that record on server2 is discarded in favour of record present on server1
    waitForUpdatedRecordPropagation(RECORD_ID, "firstName", "Luke");

    record1Server1 = retrieveRecord(getDatabaseURL(serverInstance.get(0)), RECORD_ID);
    record1Server2 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), RECORD_ID);

    int finalVersionServer1 = record1Server1.field("@version");
    int finalVersionServer2 = record1Server2.field("@version");
    assertEquals(finalVersionServer1, actualVersion + 1);
    assertEquals(finalVersionServer2, actualVersion + 1);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-simultaneous-update";
  }
}
