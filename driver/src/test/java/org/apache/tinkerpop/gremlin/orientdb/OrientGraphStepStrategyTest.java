package org.apache.tinkerpop.gremlin.orientdb;

import static org.junit.Assert.assertEquals;

import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideeffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.junit.Test;

// copy of TinkerGraphStepStrategyTest
public class OrientGraphStepStrategyTest {
  public static final String URL = "memory:" + OrientGraphStepStrategyTest.class.getSimpleName();

  @Test
  public void shouldFoldInHasContainers() {
    OrientGraph graph = new OrientGraphFactory(URL, "admin", "admin").getNoTx();
    GraphTraversalSource g = graph.traversal();
    ////
    GraphTraversal.Admin traversal = g.V().has("name", "marko").asAdmin();
    System.out.println("STEPS:");
    traversal.getSteps().forEach(System.out::println);

    assertEquals(2, traversal.getSteps().size());
    assertEquals(HasStep.class, traversal.getEndStep().getClass());
    traversal.applyStrategies();
    System.out.println("STEPS:");
    traversal.getSteps().forEach(System.out::println);

    assertEquals(1, traversal.getSteps().size());
    assertEquals(OrientGraphStep.class, traversal.getStartStep().getClass());
    assertEquals(OrientGraphStep.class, traversal.getEndStep().getClass());
    assertEquals(1, ((OrientGraphStep) traversal.getStartStep()).getHasContainers().size());
    assertEquals(
        "name",
        ((OrientGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
    assertEquals(
        "marko",
        ((OrientGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
    ////
    traversal = g.V().has("name", "marko").has("age", P.gt(20)).asAdmin();
    System.out.println("STEPS:");
    traversal.getSteps().forEach(System.out::println);

    traversal.applyStrategies();

    System.out.println("STEPS:");
    traversal.getSteps().forEach(System.out::println);

    assertEquals(1, traversal.getSteps().size());
    assertEquals(OrientGraphStep.class, traversal.getStartStep().getClass());
    assertEquals(2, ((OrientGraphStep) traversal.getStartStep()).getHasContainers().size());
    ////
    traversal = g.V().has("name", "marko").out().has("name", "daniel").asAdmin();

    System.out.println("STEPS:");
    traversal.getSteps().forEach(System.out::println);

    traversal.applyStrategies();

    System.out.println("STEPS:");
    traversal.getSteps().forEach(System.out::println);

    assertEquals(3, traversal.getSteps().size());
    assertEquals(OrientGraphStep.class, traversal.getStartStep().getClass());
    assertEquals(1, ((OrientGraphStep) traversal.getStartStep()).getHasContainers().size());
    assertEquals(
        "name",
        ((OrientGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getKey());
    assertEquals(
        "marko",
        ((OrientGraphStep<?, ?>) traversal.getStartStep()).getHasContainers().get(0).getValue());
    assertEquals(HasStep.class, traversal.getEndStep().getClass());
  }
}
