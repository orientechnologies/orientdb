package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 14.08.13
 */
public class SuperNodeGraphSpeedTest extends SpeedTestMonoThread {
  private static final long TOT = 100000l;
  private OrientBaseGraph graph;
  private OrientVertex superNode;

  public SuperNodeGraphSpeedTest() {
    super(TOT);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null) {
      buildDirectory = ".";
    }

    final OrientGraphFactory factory =
        new OrientGraphFactory(
            "plocal:" + buildDirectory + "/SuperNodeGraphSpeedTest", "admin", "admin");
    if (factory.exists()) factory.drop();

    graph = factory.getNoTx();

    superNode = graph.addVertex(null);

    factory.close();
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    superNode.addEdge("test", graph.addVertex(null));
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    int i = 0;
    for (Edge e : superNode.getEdges(Direction.OUT)) i++;
    Assert.assertEquals(i, TOT);

    graph.shutdown();
  }
}
