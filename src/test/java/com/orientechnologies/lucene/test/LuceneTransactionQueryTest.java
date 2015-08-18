/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by Enrico Risa on 10/08/15.
 */
public class LuceneTransactionQueryTest extends BaseConfiguredLuceneTest {

  public LuceneTransactionQueryTest() {
  }

  public LuceneTransactionQueryTest(boolean remote) {
    super(remote);
  }

  @Test
  public void bugTest() {

    final OrientVertexType c1 = new OrientGraphNoTx(databaseDocumentTx).createVertexType("C1");
    c1.createProperty("p1", OType.STRING);
    c1.createIndex("p1", "FULLTEXT", null, null, "LUCENE", new String[] { "p1" });

    final OrientGraph graph = new OrientGraph(databaseDocumentTx);
    graph.begin();
    final OrientVertex result = graph.addVertex("class:C1");
    result.setProperty("p1", "abc");

    final String query = "select from C1 where p1 lucene \"abc\" limit 1";
    final List<ODocument> vertices = ODatabaseRecordThreadLocal.INSTANCE.get().command(new OSQLSynchQuery<ODocument>(query))
        .execute();
    Assert.assertEquals(vertices.size(), 1);

  }

  @Override
  protected String getDatabaseName() {
    return "transactionQueryTest";
  }
}
