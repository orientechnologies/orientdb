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

import java.util.List;

import junit.framework.Assert;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Insert indexed records concurrently against the cluster
 */
public class ServerClusterIndexedTest extends ServerClusterInsertTest {

  /**
   * Event called right after the database has been created and right before to be replicated to the X servers
   * 
   * @param db
   *          Current database
   */
  protected void onAfterDatabaseCreation(final ODatabaseDocumentTx db) {
    super.onAfterDatabaseCreation(db);

    createSchema(db);
    new ODocument("Customer").fields("name", "Jay", "surname", "Miner").save();
    new ODocument("Customer").fields("name", "Luke", "surname", "Skywalker").save();
    new ODocument("Provider").fields("name", "Yoda", "surname", "Nothing").save();
  }

  private void createSchema(final ODatabaseDocumentTx db) {
    final OSchema schema = db.getMetadata().getSchema();
    OClass person = schema.getClass("Person");
    person.createIndex("Person.name", INDEX_TYPE.UNIQUE, "name");

    OClass customer = schema.createClass("Customer", person);
    customer.createProperty("totalSold", OType.DECIMAL);

    OClass provider = schema.createClass("Provider", person);
    provider.createProperty("totalPurchased", OType.DECIMAL);
  }

  @Override
  public void executeTest() throws Exception {
    super.executeTest();

    dropIndexNode1();
    recreateIndexNode2();

    for (ServerRun server : serverInstance) {
      final ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
      try {
        final long indexSize = database.getMetadata().getIndexManager().getIndex("Person.name").getSize();
        Assert.assertEquals((long) (count * serverInstance.size()) + beginInstances, indexSize);

        System.out.println("From metadata: indexes " + indexSize + " items");

        List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from index:Person.name"));
        Assert.assertEquals((long) (count * serverInstance.size()) + beginInstances,
            ((Long) result.get(0).field("count")).longValue());

        System.out.println("From sql: indexes " + indexSize + " items");
      } finally {
        database.close();
      }
    }
  }

  private void dropIndexNode1() {
    ServerRun server = serverInstance.get(0);
    ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
    try {
      Object result = database.command(new OCommandSQL("drop index Person.name")).execute();
      System.out.println("dropIndexNode1: Node1 drop index: " + result);
    } finally {
      database.close();
    }

    // CHECK ON NODE 1
    server = serverInstance.get(1);
    database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
    try {
      Assert.assertNull(database.getMetadata().getIndexManager().getIndex("Person.name"));
      System.out.println("dropIndexNode1: Node2 hasn't the index too, ok");
    } finally {
      database.close();
    }
  }

  private void recreateIndexNode2() {
    // RE-CREATE INDEX ON NODE 1
    ServerRun server = serverInstance.get(1);
    ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
    try {
      Object result = database.command(new OCommandSQL("create index Person.name on Person (name) unique")).execute();
      System.out.println("recreateIndexNode2: Node2 created index: " + result);
      Assert.assertEquals((long) (count * serverInstance.size()) + beginInstances, result);
    } finally {
      database.close();
    }

    // CHECK ON NODE 1
    server = serverInstance.get(0);
    database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
    try {
      final long indexSize = database.getMetadata().getIndexManager().getIndex("Person.name").getSize();
      Assert.assertEquals((long) (count * serverInstance.size()) + beginInstances, indexSize);
      System.out.println("recreateIndexNode2: Node1 has the index too, ok");
    } finally {
      database.close();
    }
  }
}
