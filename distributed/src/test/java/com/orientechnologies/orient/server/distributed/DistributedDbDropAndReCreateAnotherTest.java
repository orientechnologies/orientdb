/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.distributed;

import org.junit.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * Distributed test on drop + recreate database with a different name.
 */
public class DistributedDbDropAndReCreateAnotherTest extends AbstractServerClusterTxTest {
  final static int SERVERS       = 3;
  private int      lastServerNum = 0;

  @Test
  public void test() throws Exception {
    count = 10;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    do {
      for (ServerRun server : serverInstance) {
        final String dbName = getDatabaseURL(server);

        final ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbName);
        db.open("admin", "admin");

        banner("DROPPING DATABASE " + dbName + " ON SERVER " + server.getServerId());
        db.drop();
      }

      Thread.sleep(2000);

      ServerRun server = serverInstance.get(lastServerNum);

      ++lastServerNum;

      final String dbName = getDatabaseURL(server);

      banner("RE-CREATING DATABASE " + dbName + " ON SERVER " + server.getServerId());

      final OrientGraphNoTx db = new OrientGraphNoTx(dbName);
      onAfterDatabaseCreation(db);
      db.shutdown();

      Thread.sleep(2000);

    } while (lastServerNum < serverInstance.size());

    Thread.sleep(2000);

    executeMultipleTest();
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-dropcreatedbnewname" + lastServerNum;
  }
}
