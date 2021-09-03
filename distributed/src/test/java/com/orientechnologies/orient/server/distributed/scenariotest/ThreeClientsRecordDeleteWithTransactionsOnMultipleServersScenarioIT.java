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

import static org.junit.Assert.assertTrue;

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
 * Checks for consistency on the cluster with these steps: - 3 server (quorum=2) - record1 is
 * inserted on server1 - record1 (version 1) is propagated to the other two servers - introduce a
 * delay after record locking for all servers (different for each one) - the three clients at the
 * same time delete the same record
 */
public class ThreeClientsRecordDeleteWithTransactionsOnMultipleServersScenarioIT
    extends AbstractScenarioTest {

  private final String RECORD_ID = "R001";
  private Map<String, Object> hanFields =
      new HashMap<String, Object>() {
        {
          put("id", RECORD_ID);
          put("firstName", "Han");
          put("lastName", "Solo");
        }
      };

  @Test
  @Ignore
  public void test() throws Exception {
    maxRetries = 10;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    ODatabaseDocument dbServer1 = getDatabase(0);

    // inserts record1
    ODocument record1Server1 = new ODocument("Person").fromMap(hanFields);
    record1Server1.save();

    // waits for propagation of the record on all the servers
    waitForInsertedRecordPropagation(RECORD_ID);

    ODatabaseDocument dbServer2 = getDatabase(1);
    ODatabaseDocument dbServer3 = getDatabase(2);

    try {
      // sets a delay for operations on distributed storage of all servers
      // updates the same record from three different clients, each calling a different server
      List<Callable<Void>> clients = new LinkedList<Callable<Void>>();
      clients.add(new RecordDeleter(serverInstance.get(0), RECORD_ID, true));
      clients.add(new RecordDeleter(serverInstance.get(1), RECORD_ID, true));
      clients.add(new RecordDeleter(serverInstance.get(2), RECORD_ID, true));
      List<Future<Void>> futures = Executors.newCachedThreadPool().invokeAll(clients);
      executeFutures(futures);

      waitForDeletedRecordPropagation(RECORD_ID);

      assertTrue(
          retrieveRecordOrReturnMissing(serverInstance.get(0), RECORD_ID) == MISSING_DOCUMENT);
      assertTrue(
          retrieveRecordOrReturnMissing(serverInstance.get(1), RECORD_ID) == MISSING_DOCUMENT);
      assertTrue(
          retrieveRecordOrReturnMissing(serverInstance.get(2), RECORD_ID) == MISSING_DOCUMENT);
    } finally {
      dbServer1.activateOnCurrentThread();
      dbServer1.close();
      dbServer2.activateOnCurrentThread();
      dbServer2.close();
      dbServer3.activateOnCurrentThread();
      dbServer3.close();
    }
  }

  @Override
  public String getDatabaseName() {
    return "distributed-three-simultaneous-delete";
  }
}
