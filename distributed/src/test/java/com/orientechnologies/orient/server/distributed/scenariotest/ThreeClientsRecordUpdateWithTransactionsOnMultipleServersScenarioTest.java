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
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

/**
 * Checks for consistency on the cluster with these steps:
 * - 3 server (quorum=2)
 * - record1 is inserted on server1
 * - record1 (version 1) is propagated to the other two servers
 * - introduce a delay after record locking for all servers (different for each one)
 * - the three clients at the same time update the same record on different servers
 *
 */
// TODO Temporary Ignored
public class ThreeClientsRecordUpdateWithTransactionsOnMultipleServersScenarioTest extends AbstractScenarioTest {

  private final String        RECORD_ID   = "R001";
  private Map<String, Object> hanFields   = new HashMap<String, Object>() {
                                            {
                                              put("id", RECORD_ID);
                                              put("firstName", "Han");
                                              put("lastName", "Solo");
                                            }
                                          };
  private Map<String, Object> lukeFields  = new HashMap<String, Object>() {
                                            {
                                              put("id", RECORD_ID);
                                              put("firstName", "Luke");
                                              put("lastName", "Skywalker");
                                            }
                                          };
  private Map<String, Object> darthFields = new HashMap<String, Object>() {
                                            {
                                              put("id", RECORD_ID);
                                              put("firstName", "Darth");
                                              put("lastName", "Vader");
                                            }
                                          };
  private Map<String, Object> leiaFields  = new HashMap<String, Object>() {
                                            {
                                              put("id", RECORD_ID);
                                              put("firstName", "Leia");
                                              put("lastName", "Organa");
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

    ODatabaseDocumentTx dbServer1 = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
    ODatabaseDocumentTx dbServer2 = poolFactory.get(getDatabaseURL(serverInstance.get(1)), "admin", "admin").acquire();
    ODatabaseDocumentTx dbServer3 = poolFactory.get(getDatabaseURL(serverInstance.get(2)), "admin", "admin").acquire();

    // inserts record1
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    ODocument record1Server1 = new ODocument("Person").fromMap(hanFields);
    record1Server1.save();

    // waits for propagation of the record on all the servers
    waitForInsertedRecordPropagation(RECORD_ID);

    // retrieves record1 from server2 and server 3 and checks they're equal
    ODocument record1Server2 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), RECORD_ID);
    assertEquals(record1Server2.getVersion(), record1Server1.getVersion());
    assertEquals(record1Server2.field("id"), record1Server1.field("id"));
    assertEquals(record1Server2.field("firstName"), record1Server1.field("firstName"));
    assertEquals(record1Server2.field("lastName"), record1Server1.field("lastName"));

    ODocument record1Server3 = retrieveRecord(getDatabaseURL(serverInstance.get(2)), RECORD_ID);
    assertEquals(record1Server3.getVersion(), record1Server1.getVersion());
    assertEquals(record1Server3.field("id"), record1Server1.field("id"));
    assertEquals(record1Server3.field("firstName"), record1Server1.field("firstName"));
    assertEquals(record1Server3.field("lastName"), record1Server1.field("lastName"));

    // gets the actual version of the record1
    int actualVersion = record1Server1.getVersion();
    System.out.println("Actual version: " + actualVersion);

    // sets a delay for operations on distributed storage of all servers
    ((ODistributedStorage) dbServer1.getStorage()).setEventListener(new AfterRecordLockDelayer("server1", DOCUMENT_WRITE_TIMEOUT));
    ((ODistributedStorage) dbServer2.getStorage()).setEventListener(new AfterRecordLockDelayer("server2", DOCUMENT_WRITE_TIMEOUT / 4));
    ((ODistributedStorage) dbServer3.getStorage()).setEventListener(new AfterRecordLockDelayer("server3", DOCUMENT_WRITE_TIMEOUT / 2));

    // updates the same record from three different clients, each calling a different server
    List<Callable<Void>> clients = new LinkedList<Callable<Void>>();
    clients.add(new RecordUpdater(getDatabaseURL(serverInstance.get(0)), record1Server1, lukeFields, true));
    clients.add(new RecordUpdater(getDatabaseURL(serverInstance.get(1)), record1Server2, darthFields, true));
    clients.add(new RecordUpdater(getDatabaseURL(serverInstance.get(2)), record1Server3, leiaFields, true));
    List<Future<Void>> futures = Executors.newCachedThreadPool().invokeAll(clients);
    executeFutures(futures);

    // checks that record on server3 is the one which wins over the others
    waitForUpdatedRecordPropagation(RECORD_ID, "firstName", "Leia");

    record1Server1 = retrieveRecord(getDatabaseURL(serverInstance.get(0)), RECORD_ID);
    record1Server2 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), RECORD_ID);
    record1Server3 = retrieveRecord(getDatabaseURL(serverInstance.get(2)), RECORD_ID);

    int finalVersionServer1 = record1Server1.getVersion();
    int finalVersionServer2 = record1Server2.getVersion();
    int finalVersionServer3 = record1Server3.getVersion();
    assertEquals(actualVersion + 1, finalVersionServer1);
    assertEquals(actualVersion + 1, finalVersionServer2);
    assertEquals(actualVersion + 1, finalVersionServer3);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-three-simultaneous-update";
  }

}
