/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

package com.orientechnologies.orient.etl.transformer;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.etl.OETLBaseTest;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 23/11/2015. */
public class OETLMergeTransformerTest extends OETLBaseTest {

  @Before
  public void loadData() {}

  @Test
  public void shouldUpdateExistingVertices() throws Exception {
    // update graph with CSV: avoid num to be casted to integer forcing string
    configure(
        " {source: { content: { value: 'num,name\n10000,FirstNameUpdated' } }, "
            + "extractor : { csv: {} },"
            + " transformers: ["
            + "{merge: {  joinFieldName:'num', lookup:'Person.num'}}, "
            + "{vertex: { class:'Person', skipDuplicates: false}}"
            + "],"
            + "loader: { orientdb: { dbURL: 'memory:"
            + name.getMethodName()
            + "', dbType:'graph', tx: true} } }");

    ODatabasePool pool = proc.getLoader().getPool();
    ODatabaseDocument db = pool.acquire();

    OClass personClass = db.createVertexClass("Person");
    personClass.createProperty("num", OType.INTEGER);
    personClass.createIndex("Person.num", OClass.INDEX_TYPE.UNIQUE, "num");

    db.commit();

    // prepare graph
    OVertex person = db.newVertex("Person");
    person.setProperty("num", 10000);
    person.setProperty("name", "FirstName");
    person.save();
    db.commit();
    db.close();

    // verify
    db = pool.acquire();
    assertThat(db.countClass("Person")).isEqualTo(1);

    OResultSet resultSet = db.query("SELECT from Person where num = 10000");

    final OResult inserted = resultSet.next();

    assertThat(inserted.<String>getProperty("name")).isEqualTo("FirstName");
    assertThat(inserted.<Integer>getProperty("num")).isEqualTo(10000);
    assertThat(resultSet.hasNext()).isFalse();

    resultSet.close();
    db.close();
    // run processor
    proc.execute();

    // verify
    db = pool.acquire();

    assertThat(db.countClass("Person")).isEqualTo(1);

    resultSet = db.query("SELECT from Person where num = 10000");

    final OResult updated = resultSet.next();

    //    ORecord load = graph.load(updated.toElement().getIdentity());
    assertThat(updated.<String>getProperty("name")).isEqualTo("FirstNameUpdated");
    assertThat(updated.<Integer>getProperty("num")).isEqualTo(10000);
    assertThat(resultSet.hasNext()).isFalse();

    resultSet.close();
  }

  @Test
  public void shouldMergeVertexOnDuplitcatedInputSet() throws Exception {

    // CSV contains duplicated data
    configure(
        "{source: { content: { value: 'num,name\n10000,FirstName\n10001,SecondName\n10000,FirstNameUpdated' } }, extractor : { csv: {} },"
            + " transformers: [{merge: { joinFieldName:'num', lookup:'Person.num'}}, {vertex: {class:'Person', skipDuplicates: true}}],"
            + " "
            + "loader: { orientdb: { dbURL: 'memory:"
            + name.getMethodName()
            + "', dbType:'graph', useLightweightEdges:false } } }");

    ODatabasePool pool = proc.getLoader().getPool();
    ODatabaseDocument db = pool.acquire();

    OClass personClass = db.createVertexClass("Person");
    personClass.createProperty("num", OType.INTEGER);

    personClass.createIndex("Person.num", OClass.INDEX_TYPE.UNIQUE, "num");

    db.commit();
    db.close();

    // run processor
    proc.execute();

    db = pool.acquire();
    assertThat(db.countClass("Person")).isEqualTo(2);

    OResultSet resultSet = db.query("SELECT from Person where num = 10000");

    final OResult updated = resultSet.next();

    assertThat(updated.<String>getProperty("name")).isEqualTo("FirstNameUpdated");
    assertThat(resultSet.hasNext()).isFalse();
    resultSet.close();
  }
}
