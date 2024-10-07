package org.apache.tinkerpop.gremlin.orientdb;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

public class MatcherStepTest {

  private OrientGraph graph;

  @Before
  public void setup() {}

  //  @Test
  public void searchMatching() {
    graph = new OrientGraphFactory("memory:matching").getNoTx();

    Vertex marko = graph.addVertex("name", "marko", "age", 29);
    Vertex vadas = graph.addVertex("name", "vadas", "age", 27);
    Vertex lop = graph.addVertex("name", "lop", "lang", "java");
    Vertex josh = graph.addVertex("name", "josh", "age", 32);
    Vertex ripple = graph.addVertex("name", "ripple", "lang", "java");
    Vertex peter = graph.addVertex("name", "peter", "age", 35);
    marko.addEdge("knows", vadas, "weight", 0.5f);
    marko.addEdge("knows", josh, "weight", 1.0f);
    marko.addEdge("created", lop, "weight", 0.4f);
    josh.addEdge("created", ripple, "weight", 1.0f);
    josh.addEdge("created", lop, "weight", 0.4f);
    peter.addEdge("created", lop, "weight", 0.2f);

    GraphTraversalSource g = graph.traversal();

    List<Map<String, Object>> result =
        g.V()
            .match(
                __.as("a").out("created").as("b"),
                __.as("b").has("name", "lop"),
                __.as("b").in("created").as("c"),
                __.as("c").has("age", 29))
            .select("a", "c")
            .by("name")
            .toList();

    assertThat(result, hasSize(3));
    assertThat(result.get(0), allOf(hasEntry("a", "marko"), hasEntry("c", "marko")));
    assertThat(result.get(1), allOf(hasEntry("a", "josh"), hasEntry("c", "marko")));
    assertThat(result.get(2), allOf(hasEntry("a", "peter"), hasEntry("c", "marko")));
  }

  @Test
  public void singleMatching() {
    graph = new OrientGraphFactory("memory:singleMatching").getNoTx();

    Vertex marko = graph.addVertex("name", "marko", "age", 29);
    marko.addEdge("pays", marko);

    GraphTraversalSource g = graph.traversal();

    List<Map<String, Object>> result =
        g.V()
            .match(__.as("a").out("pays").as("b"), __.as("b").has("name", "marko"))
            .select("a", "b")
            .by("name")
            .toList();

    assertThat(result.toString(), result, hasSize(1));
    assertThat(result.get(0), allOf(hasEntry("a", "marko"), hasEntry("b", "marko")));
  }
}
