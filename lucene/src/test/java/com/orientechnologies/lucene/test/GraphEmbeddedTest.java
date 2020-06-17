/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 03/09/14. */
public class GraphEmbeddedTest extends BaseLuceneTest {

  public GraphEmbeddedTest() {}

  @Before
  public void init() {

    OClass type = db.createVertexClass("City");
    type.createProperty("latitude", OType.DOUBLE);
    type.createProperty("longitude", OType.DOUBLE);
    type.createProperty("name", OType.STRING);

    db.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE"))
        .execute();
    db.commit();
  }

  @Test
  public void embeddedTx() {

    // THIS WON'T USE LUCENE INDEXES!!!! see #6997

    db.begin();
    OVertex city = db.newVertex("City");
    city.setProperty("name", "London / a");
    db.save(city);

    city = db.newVertex("City");
    city.setProperty("name", "Rome");
    db.save(city);
    db.commit();

    db.begin();

    List<ODocument> resultSet =
        db.query(new OSQLSynchQuery<ODocument>("SELECT from City where name = 'London / a' "));

    Assertions.assertThat(resultSet).hasSize(1);

    resultSet = db.query(new OSQLSynchQuery<ODocument>("SELECT from City where name = 'Rome' "));

    Assertions.assertThat(resultSet).hasSize(1);
  }

  @Test
  public void testGetVericesFilterClass() {

    OClass v = db.getClass("V");
    v.createProperty("name", OType.STRING);
    db.command("CREATE INDEX V.name ON V(name) NOTUNIQUE");
    db.commit();

    OClass oneClass = db.createVertexClass("One");
    OClass twoClass = db.createVertexClass("Two");

    OVertex one = db.newVertex(oneClass);
    one.setProperty("name", "Same");
    db.save(one);

    OVertex two = db.newVertex(twoClass);
    two.setProperty("name", "Same");
    db.save(two);

    db.commit();

    List<ODocument> resultSet =
        db.query(new OSQLSynchQuery<ODocument>("SELECT from One where name = 'Same' "));

    Assertions.assertThat(resultSet).hasSize(1);

    //    graph.addVertex("class:Two", new Object[] { "name", "Same" });
    //
    //    graph.commit();
    //
    //    Iterable<Vertex> vertexes = graph.getVertices("One", new String[] { "name" }, new Object[]
    // { "Same" });
    //
    //    int size = 0;
    //    for (Vertex v : vertexes) {
    //      size++;
    //      Assert.assertNotNull(v);
    //    }
    //    Assert.assertEquals(1, size);
  }
}
