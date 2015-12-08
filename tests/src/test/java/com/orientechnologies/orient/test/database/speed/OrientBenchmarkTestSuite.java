package com.orientechnologies.orient.test.database.speed;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

import java.io.FileInputStream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrientBenchmarkTestSuite {

  private static final int TOTAL_RUNS = 1;

  public void main(String args[]) throws Exception {
    new OrientBenchmarkTestSuite().testOrientGraph();
  }

  public OrientBenchmarkTestSuite() {
  }

  public void testOrientGraph() throws Exception {
    long totalTime = 0;

    OrientBaseGraph graph = new OrientGraphNoTx("GraphExample2");

    GraphMLReader.inputGraph(graph, new FileInputStream("/Users/luca/Downloads/graph-example-2.xml"));
    System.out.println("V: " + graph.getRawGraph().countClass("V") + " E: " + graph.getRawGraph().countClass("E"));
    graph.shutdown();

    long lastTime = System.currentTimeMillis();
    for (int i = 0; i < TOTAL_RUNS; i++) {
      graph = new OrientGraphNoTx("GraphExample2");
      int counter = execute(graph);

      long currentTime = System.currentTimeMillis() - lastTime;
      lastTime = System.currentTimeMillis();

      totalTime += currentTime;
      System.out.println("OrientGraph elements touched " + counter + " in " + currentTime);
      graph.shutdown();
    }
    System.out.println("OrientGraph experiment average = " + (totalTime / (double) TOTAL_RUNS));
  }

  private int execute(Graph graph) {
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
