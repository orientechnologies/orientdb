/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.test.object;

import static org.testng.Assert.assertTrue;

import java.io.IOException;

import javax.persistence.Id;
import javax.persistence.Version;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

/**
 * Created by luigidellaquila on 01/07/15.
 */
public class MultipleObjectDbInstancesTest {
  /**
   * Scenario: create database, register Pojos, create another database, register Pojos again. Check in both if Pojos exist in
   * Schema.
   *
   * @throws java.io.IOException
   */
  @Test
  public void testTwiceCreateDBSchemaRegistered() throws IOException {
    createDatabase("MultipleDbInstancesTest_first");
    Connection conFirst = new Connection("MultipleDbInstancesTest_first");
    assertTrue(conFirst.objectDb.getMetadata().getSchema().existsClass("V"));
    assertTrue(conFirst.objectDb.getMetadata().getSchema().existsClass("X"));

    createDatabase("MultipleDbInstancesTest_second");
    Connection conSecond = new Connection("MultipleDbInstancesTest_second");
    assertTrue(conSecond.objectDb.getMetadata().getSchema().existsClass("V"));
    assertTrue(conSecond.objectDb.getMetadata().getSchema().existsClass("X"));
  }

  private void createDatabase(String databaseName) throws IOException {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:" + databaseName);
    db.create();
    db.close();
  }

  public class V {
    @Id
    private Object graphId;
    @Version
    private Object graphVersion;

    public Object getGraphId() {
      return graphId;
    }

    public Object getGraphVersion() {
      return graphVersion;
    }
  }

  public class X extends V {
  }

  private class Connection {
    OrientBaseGraph   graph;
    OObjectDatabaseTx objectDb;

    public Connection(String databaseName) {
      OrientGraphFactory graphFactory = new OrientGraphFactory("memory:" + databaseName, "admin", "admin");

      // Create graph API access
      graph = graphFactory.getNoTx();
      graph.setUseLightweightEdges(false);

      if (graph.getVertexType("V") == null) {
        graph.createVertexType("V");
      }

      // Create object API access
      objectDb = new OObjectDatabaseTx(graph.getRawGraph());
      objectDb.setAutomaticSchemaGeneration(true);
      objectDb.getEntityManager().registerEntityClass(V.class);
      objectDb.getEntityManager().registerEntityClass(X.class);
    }

    public void close() {
      objectDb.close();
    }
  }
}
