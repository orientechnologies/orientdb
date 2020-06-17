package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import java.io.FileInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author Marko A. Rodriguez (http://markorodriguez.com) */
@RunWith(JUnit4.class)
public class OrientBenchmarkTestSuite extends TestSuite {

  private static final int TOTAL_RUNS = 1;

  public OrientBenchmarkTestSuite() {}

  public OrientBenchmarkTestSuite(final GraphTest graphTest) {
    super(graphTest);
  }

  @Test
  public void testOrientGraph() throws Exception {
    double totalTime = 0.0d;
    if (graphTest == null) return;

    Graph graph = graphTest.generateGraph();

    // graph = new OrientBatchGraph((ODatabaseDocumentTx) ((OrientGraph) graph).getRawGraph());
    // GraphMLReader.inputGraph(graph,
    // GraphMLReader.class.getResourceAsStream("graph-example-2.xml"));
    GraphMLReader.inputGraph(
        graph, new FileInputStream("/Users/luca/Downloads/graph-example-2.xml"));
    System.out.println(
        "V: "
            + ((OrientBaseGraph) graph).getRawGraph().countClass("V")
            + " E: "
            + ((OrientBaseGraph) graph).getRawGraph().countClass("E"));
    graph.shutdown();

    for (int i = 0; i < TOTAL_RUNS; i++) {
      graph = graphTest.generateGraph();
      int counter = execute(graph);
      double currentTime = this.stopWatch();
      totalTime = totalTime + currentTime;
      BaseTest.printPerformance(
          graph.toString(), counter, "OrientGraph elements touched", currentTime);
      graph.shutdown();
    }
    BaseTest.printPerformance(
        "OrientGraph", 1, "OrientGraph experiment average", totalTime / (double) TOTAL_RUNS);
  }

  private int execute(Graph graph) {
    this.stopWatch();
    int counter = 0;

    for (final Vertex vertex : graph.getVertices()) {
      counter++;
      for (final Edge edge : vertex.getEdges(Direction.OUT)) {
        counter++;
        final Vertex vertex2 = edge.getVertex(Direction.IN);
        counter++;
        for (final Edge edge2 : vertex2.getEdges(Direction.OUT)) {
          counter++;
          final Vertex vertex3 = edge2.getVertex(Direction.IN);
          counter++;
          for (final Edge edge3 : vertex3.getEdges(Direction.OUT)) {
            counter++;
            edge3.getVertex(Direction.OUT);
            counter++;
          }
        }
      }
    }
    return counter;
  }
}
