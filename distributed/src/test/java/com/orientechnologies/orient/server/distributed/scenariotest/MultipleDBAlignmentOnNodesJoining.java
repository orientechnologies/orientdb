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

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
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

  @Ignore
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

      banner("Populating databases on server1 and server2");






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


  /**
   * Creates the databases as follows:
   * - server1: db A, db B
   * - server2: db B, db C
   *
   * @throws IOException
   */
  @Override
  protected void prepare(final boolean iCopyDatabaseToNodes, final boolean iCreateDatabase, final OCallable<Object, OrientGraphFactory> iCfgCallback) throws IOException {

    // creating databases on server1
    ServerRun master = serverInstance.get(0);

    if (iCreateDatabase) {
      final OrientBaseGraph graph1 = master.createDatabase("db-A", iCfgCallback);
      final OrientBaseGraph graph2 = master.createDatabase("db-B", iCfgCallback);
      try {
        onAfterDatabaseCreation(graph1);
        onAfterDatabaseCreation(graph2);
      } finally {
        if(!graph1.isClosed()) {
          graph1.shutdown();
        }
        if(!graph1.isClosed()) {
          graph2.shutdown();
        }
        Orient.instance().closeAllStorages();
      }
    }

    // creating databases on server1
    master = serverInstance.get(1);

    if (iCreateDatabase) {
      final OrientBaseGraph graph1 = master.createDatabase("db-B", iCfgCallback);
      final OrientBaseGraph graph2 = master.createDatabase("db-C", iCfgCallback);
      try {
        onAfterDatabaseCreation(graph1);
        onAfterDatabaseCreation(graph2);
      } finally {
        if(!graph1.isClosed()) {
          graph1.shutdown();
        }
        if(!graph1.isClosed()) {
          graph2.shutdown();
        }
        Orient.instance().closeAllStorages();
      }
    }
  }

  /**
   * Event called right after the database has been created. It builds the schema and populates the db.
   *
   * @param db
   *          Current database
   */
  @Override
  protected void onAfterDatabaseCreation(final OrientBaseGraph db) {
    System.out.println("Creating database schema...");

    // building basic schema
    OClass personClass = db.getRawGraph().getMetadata().getSchema().createClass("Person");
    personClass.createProperty("id", OType.STRING);
    personClass.createProperty("name", OType.STRING);
    personClass.createProperty("birthday", OType.DATE);
    personClass.createProperty("children", OType.STRING);

    final OSchema schema = db.getRawGraph().getMetadata().getSchema();
    OClass person = schema.getClass("Person");
    idx = person.createIndex("Person.name", OClass.INDEX_TYPE.UNIQUE, "name");

    // populating db
//    executeMultipleWrites();
  }


}
