package org.apache.tinkerpop.gremlin.orientdb;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideeffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

public class OrientGraphMatchStrategyTest {

  @Test
  public void shouldUseMatchOptimization() {
    OrientGraph graph = OrientGraph.open();

    OClass v = graph.getRawDatabase().getClass("V");
    OProperty property = v.createProperty("name", OType.STRING);
    property.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    try {
      GraphTraversalSource traversal = graph.traversal();

      GraphTraversal.Admin<Vertex, Map<String, Object>> admin =
          traversal.V().match(__.as("a").has("name", "Enrico").out("Friends").as("b")).asAdmin();

      admin.applyStrategies();

      Step<Vertex, ?> startStep = admin.getStartStep();
      assertEquals(OrientGraphStep.class, startStep.getClass());

      OrientGraphStep countStep = (OrientGraphStep) startStep;
      assertEquals(1, countStep.getHasContainers().size());

      Assert.assertEquals(1, countStep.findIndex().size());

    } finally {
      graph.close();
    }
  }

  @Test
  public void shouldUseMatchOptimizationWithLabel() {
    OrientGraph graph = OrientGraph.open();

    OClass v = graph.getRawDatabase().createVertexClass("Person");
    OProperty property = v.createProperty("name", OType.STRING);
    property.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    try {
      GraphTraversalSource traversal = graph.traversal();

      GraphTraversal.Admin<Vertex, Map<String, Object>> admin =
          traversal.V().match(__.as("a").has("name", "Foo").out("Friends").as("b")).asAdmin();

      admin.applyStrategies();

      Step<Vertex, ?> startStep = admin.getStartStep();
      assertEquals(OrientGraphStep.class, startStep.getClass());

      OrientGraphStep countStep = (OrientGraphStep) startStep;
      assertEquals(1, countStep.getHasContainers().size());

      Assert.assertEquals(0, countStep.findIndex().size());

      admin =
          traversal
              .V()
              .match(__.as("a").hasLabel("Person").has("name", "Foo").out("Friends").as("b"))
              .asAdmin();

      admin.applyStrategies();

      startStep = admin.getStartStep();
      assertEquals(OrientGraphStep.class, startStep.getClass());

      countStep = (OrientGraphStep) startStep;
      assertEquals(2, countStep.getHasContainers().size());

      Assert.assertEquals(1, countStep.findIndex().size());

    } finally {
      graph.close();
    }
  }

  @Test
  public void shouldFetchDataUsingMatchOptimization() {
    OrientGraph graph = OrientGraph.open();

    OClass v = graph.getRawDatabase().createVertexClass("Person");
    OProperty property = v.createProperty("name", OType.STRING);
    property.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    Vertex bar = graph.addVertex(T.label, "Person", "name", "Bar");

    for (int i = 0; i < 100; i++) {
      Vertex foo = graph.addVertex(T.label, "Person", "name", "Foo" + i);
      bar.addEdge("Friends", foo);
    }

    try {
      GraphTraversalSource traversal = graph.traversal();

      List<Map<String, Object>> admin =
          traversal.V().match(__.as("a").has("name", "Foo0").in("Friends").as("b")).toList();

      Assert.assertEquals(1, admin.size());

      admin =
          traversal
              .V()
              .match(__.as("a").hasLabel("Person").has("name", "Foo0").in("Friends").as("b"))
              .toList();

      Assert.assertEquals(1, admin.size());

    } finally {
      graph.close();
    }
  }
}
