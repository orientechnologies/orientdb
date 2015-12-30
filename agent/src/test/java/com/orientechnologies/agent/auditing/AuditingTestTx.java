/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.auditing;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import junit.framework.Assert;

/**
 * Tests against Auditing.
 * 
 * @author Luca Garulli
 */

public class AuditingTestTx extends AuditingTest {

  @Override
  protected void setUp() {
    graph = new OrientGraph("memory:AuditingTest");
    graph.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
      @Override
      public Object call(OrientBaseGraph graph) {
        OrientVertexType cls = graph.createVertexType("AuditingLog");
        cls.createProperty("date", OType.DATETIME);
        cls.createProperty("user", OType.LINK);
        cls.createProperty("operation", OType.BYTE);
        cls.createProperty("record", OType.LINK);
        cls.createProperty("changes", OType.EMBEDDED);
        cls.createProperty("note", OType.STRING);
        graph.createVertexType("User");
        graph.createVertexType("Test");
        return null;
      }
    });
  }

  public void testTxManualRollback() {

    graph.addVertex("class:User", new Object[] { "name", "Enrico" });
    graph.addVertex("class:User", new Object[] { "name", "Luca" });

    graph.rollback();

    assertEquals(0, graph.getRawGraph().countClass("AuditingLog"));

  }

  public void testTxForcedRollback() {

    try {

      graph.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
        @Override
        public Object call(OrientBaseGraph iArgument) {
          OrientVertexType user = iArgument.getVertexType("User");
          OrientVertexType.OrientVertexProperty name = user.createProperty("name", OType.STRING);
          name.createIndex(OClass.INDEX_TYPE.UNIQUE);
          return null;
        }
      });
      graph.addVertex("class:User", new Object[] { "name", "Enrico" });
      graph.addVertex("class:User", new Object[] { "name", "Enrico" });

      graph.commit();

    } catch (ORecordDuplicatedException e) {
      assertEquals(0, graph.getRawGraph().countClass("AuditingLog"));

      return;
    }
    Assert.fail();
  }
}
