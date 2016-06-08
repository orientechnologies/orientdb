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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
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
  @author Andrea Iacono (a.iacono--at--orientdb.com)
 * Checks for consistency on the cluster with these steps:
 * - 2 server (quorum=2)
 * - record1 is inserted on server1
 * - record1 (version 1) is propagated to the other server
 * - introduce a delay after record locking for the two servers (different for each one)
 * - the two clients at the same time update the same record on different servers
 * - the server1 immediately commits the transaction and tries to update the record to server2, which has the record locked
 * - meanwhile (while server1 is retrying) server2 commits and starts to try to update server1 as well
 * - since server1 has started first, it's the one which finishes first and rollback
 * - server2 can now successfully update the record on server1
 */

public class TwoClientsRecordUpdateDuringRetryWithTransactionsOnMultipleServersScenarioTest extends AbstractScenarioTest {

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
    OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.setValue(new Integer(2000));
    OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_MAX_AUTORETRY.setValue(new Integer(1));
    maxRetries = 10;
    init(2);
    prepare(false);
    executeWritesOnServers.addAll(serverInstance);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    ODatabaseDocumentTx dbServer1 = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
    ODatabaseDocumentTx dbServer2 = poolFactory.get(getDatabaseURL(serverInstance.get(1)), "admin", "admin").acquire();

    // inserts record1
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer2);
    ODocument record1Server1 = new ODocument("Person").fields("id", RECORD_ID, "firstName", "Han", "lastName", "Solo");
    record1Server1.save();

    // waits for propagation of the record on all the servers
    waitForInsertedRecordPropagation(RECORD_ID);

    // gets the actual version of record1
    int actualVersion = record1Server1.getVersion();

    // sets a delay for operations on distributed storage of server1 and server2
    // so that server1 will start to commit after server2 has started the transaction
    ((ODistributedStorage) dbServer2.getStorage()).setEventListener(new AfterRecordLockDelayer("server2", 1000));
    ((ODistributedStorage) dbServer1.getStorage()).setEventListener(new AfterRecordLockDelayer("server1", 250));

    // updates the same record from two different clients, each calling a different node
    ODocument record1Server2 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), RECORD_ID);
    List<Callable<Void>> clients = new LinkedList<Callable<Void>>();
    clients.add(new RecordUpdater(getDatabaseURL(serverInstance.get(0)), record1Server1, lukeFields, true));
    clients.add(new RecordUpdater(getDatabaseURL(serverInstance.get(1)), record1Server2, darthFields, true));
    List<Future<Void>> futures = Executors.newCachedThreadPool().invokeAll(clients);
    executeFutures(futures);

    // checks that record on server1 is discarded in favour of record present on server2
    waitForUpdatedRecordPropagation(RECORD_ID, "firstName", "Darth");

    record1Server1 = retrieveRecord(getDatabaseURL(serverInstance.get(0)), RECORD_ID);
    record1Server2 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), RECORD_ID);

    int finalVersionServer1 = record1Server1.field("@version");
    int finalVersionServer2 = record1Server2.field("@version");
    assertEquals(finalVersionServer1, actualVersion + 1);
    assertEquals(finalVersionServer2, actualVersion + 1);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-simultaneous-update-within-retry";
  }
}
