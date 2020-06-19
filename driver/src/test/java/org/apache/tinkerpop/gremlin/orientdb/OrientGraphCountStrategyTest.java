package org.apache.tinkerpop.gremlin.orientdb;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import org.apache.tinkerpop.gremlin.orientdb.traversal.step.map.OrientClassCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

public class OrientGraphCountStrategyTest {

  @Test
  public void shouldUseGlobalCountStepWithV() {
    OrientGraph graph = OrientGraph.open();

    try {
      GraphTraversalSource traversal = graph.traversal();

      GraphTraversal.Admin<Vertex, Long> admin = traversal.V().count().asAdmin();

      admin.applyStrategies();

      Step<Vertex, ?> startStep = admin.getStartStep();
      assertEquals(OrientClassCountStep.class, startStep.getClass());
      assertEquals(OrientClassCountStep.class, admin.getEndStep().getClass());

      OrientClassCountStep countStep = (OrientClassCountStep) startStep;
      assertEquals(Collections.singletonList("V"), countStep.getKlasses());

    } finally {
      graph.close();
    }
  }

  @Test
  public void shouldCountWithV() {
    OrientGraph graph = OrientGraph.open();

    for (int i = 0; i < 10; i++) {
      graph.addVertex();
    }
    try {
      GraphTraversalSource g = graph.traversal();

      Assert.assertEquals(10, g.V().count().toStream().findFirst().get().longValue());

    } finally {
      graph.close();
    }
  }

  @Test
  public void shouldCountWithVWithAlias() {
    OrientGraph graph = OrientGraph.open();

    for (int i = 0; i < 10; i++) {
      graph.addVertex();
    }
    try {
      GraphTraversalSource g = graph.traversal();

      Assert.assertEquals(10, g.V().as("a").count().toStream().findFirst().get().longValue());

    } finally {
      graph.close();
    }
  }

  @Test
  public void shouldUseGlobalCountStepWithE() {
    OrientGraph graph = OrientGraph.open();

    try {
      GraphTraversalSource traversal = graph.traversal();

      GraphTraversal.Admin<Edge, Long> admin = traversal.E().count().asAdmin();

      admin.applyStrategies();

      Step<Edge, ?> startStep = admin.getStartStep();
      assertEquals(OrientClassCountStep.class, startStep.getClass());
      assertEquals(OrientClassCountStep.class, admin.getEndStep().getClass());

      OrientClassCountStep countStep = (OrientClassCountStep) startStep;
      assertEquals(Collections.singletonList("E"), countStep.getKlasses());

    } finally {
      graph.close();
    }
  }

  @Test
  public void shouldCountWithE() {
    OrientGraph graph = OrientGraph.open();

    Vertex v1 = graph.addVertex();
    Vertex v2 = graph.addVertex();
    for (int i = 0; i < 10; i++) {
      v1.addEdge("Rel", v2);
    }
    try {
      GraphTraversalSource g = graph.traversal();

      Assert.assertEquals(10, g.E().count().toStream().findFirst().get().longValue());
      Assert.assertEquals(
          10, g.E().hasLabel("Rel").count().toStream().findFirst().get().longValue());

    } finally {
      graph.close();
    }
  }

  @Test
  public void shouldUseGlobalCountStepWithCustomClass() {
    OrientGraph graph = OrientGraph.open();
    graph.createVertexClass("Person");
    try {
      GraphTraversalSource traversal = graph.traversal();

      GraphTraversal.Admin<Vertex, Long> admin = traversal.V().hasLabel("Person").count().asAdmin();

      admin.applyStrategies();

      Step<Vertex, ?> startStep = admin.getStartStep();
      assertEquals(OrientClassCountStep.class, startStep.getClass());
      assertEquals(OrientClassCountStep.class, admin.getEndStep().getClass());

      OrientClassCountStep countStep = (OrientClassCountStep) startStep;
      assertEquals(Collections.singletonList("Person"), countStep.getKlasses());

    } finally {
      graph.close();
    }
  }

  @Test
  public void shouldCountWithPerson() {
    OrientGraph graph = OrientGraph.open();

    for (int i = 0; i < 10; i++) {
      graph.addVertex(T.label, "Person");
    }
    try {
      GraphTraversalSource g = graph.traversal();
      Assert.assertEquals(
          10, g.V().hasLabel("Person").count().toStream().findFirst().get().longValue());

    } finally {
      graph.close();
    }
  }

  @Test
  public void shouldUseLocalCountStep() {
    OrientGraph graph = OrientGraph.open();
    Vertex v1 = graph.addVertex(T.label, "Person");
    Vertex v2 = graph.addVertex(T.label, "Person");

    for (int i = 0; i < 10; i++) {
      v1.addEdge("HasFriend", v2);
    }
    try {
      GraphTraversalSource traversal = graph.traversal();

      Long count =
          traversal.V().hasLabel("Person").out("HasFriend").count().toStream().findFirst().get();

      Assert.assertEquals(10L, count.longValue());

    } finally {
      graph.close();
    }
  }
}
