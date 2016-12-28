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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.etl.OETLBaseTest;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by frank on 23/11/2015.
 */
public class OETLMergeTransformerTest extends OETLBaseTest {

  @Before
  public void loadData() {
    graph.createVertexType("Person");
    graph.createKeyIndex("num", Vertex.class, new Parameter<String, String>("type", "UNIQUE"),
        new Parameter<String, String>("class", "Person"));
    graph.commit();
  }

  @Test
  public void shouldUpdateExistingVertices() throws Exception {

    //prepare graph
    graph.addVertex("class:Person", "num", 10000, "name", "FirstName");
    graph.commit();

    assertThat(graph.countVertices("Person")).isEqualTo(1);

    Iterable<Vertex> vertices = graph.getVertices("Person.num", 10000);

    assertThat(vertices).hasSize(1);
    final Vertex inserted = vertices.iterator().next();

    assertThat(inserted.<String>getProperty("name")).isEqualTo("FirstName");
    assertThat(inserted.<Integer>getProperty("num")).isEqualTo(10000);

    //update graph with CSV: avoid num to be casted to integer forcing string
    process(
        " {source: { content: { value: 'num,name\n10000,FirstNameUpdated' } }, " + "extractor : { csv: {} }," + " transformers: ["
            + "{merge: {  joinFieldName:'num', lookup:'Person.num'}}, " + "{vertex: { class:'Person', skipDuplicates: false}}"
            + "]," + "loader: { orientdb: { dbURL: 'memory:" + name.getMethodName() + "', dbType:'graph', tx: true} } }");

    //verify
    graph = new OrientGraph("memory:" + name.getMethodName());

    assertThat(graph.countVertices("Person")).isEqualTo(1);

    vertices = graph.getVertices("Person.num", 10000);

    assertThat(vertices).hasSize(1);
    final Vertex updated = vertices.iterator().next();

    ORecord load = graph.getRawGraph().load((ORID) updated.getId());
    assertThat(updated.<String>getProperty("name")).isEqualTo("FirstNameUpdated");
    assertThat(updated.<Integer>getProperty("num")).isEqualTo(10000);
  }

  @Test
  public void shouldMergeVertexOnDuplitcatedInputSet() throws Exception {

    //CSV contains duplicated data
    process(
        "{source: { content: { value: 'num,name\n10000,FirstName\n10001,SecondName\n10000,FirstNameUpdated' } }, extractor : { csv: {} },"
            + " transformers: [{merge: { joinFieldName:'num', lookup:'Person.num'}}, {vertex: {class:'Person', skipDuplicates: true}}],"
            + " " + "loader: { orientdb: { dbURL: 'memory:" + name.getMethodName()
            + "', dbType:'graph', useLightweightEdges:false } } }");

    assertThat(graph.countVertices("Person")).isEqualTo(2);

    final Iterable<Vertex> vertices = graph.getVertices("Person.num", "10000");

    assertThat(vertices).hasSize(1);
    final Vertex updated = vertices.iterator().next();

    assertThat(updated.<String>getProperty("name")).isEqualTo("FirstNameUpdated");
  }

}