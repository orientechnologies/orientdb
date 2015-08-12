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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by Enrico Risa on 10/08/15.
 */
public class LuceneBugTest {

  @Test
  public void bugTest() {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:default").create();
    final OrientVertexType c1 = new OrientGraphNoTx(db).createVertexType("C1");
    c1.createProperty("p1", OType.STRING);
    c1.createIndex("p1", "FULLTEXT", null, null, "LUCENE", new String[] { "p1" });

    final OrientGraph graph = new OrientGraph(db);
    graph.begin();
    final OrientVertex result = graph.addVertex("class:C1");
    result.setProperty("p1", "abc");


    final String query = "select from C1 where p1 lucene \"abc\" limit 1";
    final List<ODocument> vertices = ODatabaseRecordThreadLocal.INSTANCE.get().command(new OSQLSynchQuery<ODocument>(query))
        .execute();
    assert vertices != null && vertices.size() == 1;

    db.drop();
  }
}
