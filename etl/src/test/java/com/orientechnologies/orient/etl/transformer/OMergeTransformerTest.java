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
public class OMergeTransformerTest extends OETLBaseTest {

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

    assertThat(inserted.getProperty("name")).isEqualTo("FirstName");
    assertThat(inserted.getProperty("num")).isEqualTo(10000);

    //update graph with CSV: avoid num to be casted to integer forcing string
    process(" {source: { content: { value: 'num,name\n10000,FirstNameUpdated' } }, "
        + "extractor : { csv: {} }," + " transformers: ["
        + "{merge: {  joinFieldName:'num', lookup:'Person.num'}}, "
        + "{vertex: { class:'Person', skipDuplicates: false}}"
        + "],"
        + "loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', tx: true} } }");

    //verify
    graph = new OrientGraph("memory:OETLBaseTest");

    assertThat(graph.countVertices("Person")).isEqualTo(1);

    vertices = graph.getVertices("Person.num", 10000);

    assertThat(vertices).hasSize(1);
    final Vertex updated = vertices.iterator().next();

    ORecord load = graph.getRawGraph().load((ORID) updated.getId());
    assertThat(updated.getProperty("name")).isEqualTo("FirstNameUpdated");
    assertThat(updated.getProperty("num")).isEqualTo(10000);
  }

  @Test
  public void shouldMergeVertexOnDuplitcatedInputSet() throws Exception {

    //CSV contains duplicated data
    process(
        "{source: { content: { value: 'num,name\n10000,FirstName\n10001,SecondName\n10000,FirstNameUpdated' } }, extractor : { csv: {} },"
            + " transformers: [{merge: { joinFieldName:'num', lookup:'Person.num'}}, {vertex: {class:'Person', skipDuplicates: true}}],"
            + " " + "loader: { orientdb: { dbURL: 'memory:OETLBaseTest', dbType:'graph', useLightweightEdges:false } } }");

    assertThat(graph.countVertices("Person")).isEqualTo(2);

    final Iterable<Vertex> vertices = graph.getVertices("Person.num", "10000");

    assertThat(vertices).hasSize(1);
    final Vertex updated = vertices.iterator().next();

    assertThat(updated.getProperty("name")).isEqualTo("FirstNameUpdated");
  }

}